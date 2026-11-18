"""
Continuous collection of UTF-8 encoded CSV shards that are written progressively.

The producer writes a sequence of shard files identified by consecutive indices. Only the current (last)
shard is appended to; the appearance of the next shard means the previous one is sealed. The producer may
declare completion by creating a marker file once the last shard is finished.

Unlike :class:`bonobo.CsvReader`, this reader is built for *tail*-like consumption:

* reaching the temporary end of the current shard does not end the pipeline, it just waits for more data;
* partially written multibyte characters are buffered until a complete UTF-8 sequence is available;
* physical lines that are not terminated (including lines inside an open quoted field) are not emitted;
* every shard is read from its beginning and its header is checked against the first shard's header;
* each record is delivered exactly once, in shard index then logical record order, as a plain tuple
  (duplicate rows are preserved);
* a sealed shard containing bad encoding, a truncated CSV record, a missing shard in the sequence or a
  divergent header fails the pipeline instead of silently joining fragments across shards;
* the normal end-of-input is only emitted once the completion marker is present and the last shard has
  been fully read and validated, at which point all open resources are released.

The existing :class:`bonobo.CsvReader` is intentionally left untouched.
"""
import codecs
import csv
import io
import re
import time

from bonobo.config import Configurable, Method, Option, Service, use_context
from bonobo.errors import UnrecoverableError
from bonobo.nodes.io.base import filesystem_path
from bonobo.util.collections import tuple_or_const


class CsvShardError(UnrecoverableError):
    """Unrecoverable error raised while collecting progressively written CSV shards."""


class _CsvLineSplitter:
    """
    Stateful splitter cutting decoded text into complete CSV physical records.

    A record is only considered complete once a line terminator is reached *outside* a quoted field.
    Quote characters are tracked across chunks so line breaks embedded in quoted fields do not end a
    record. The splitter honours the same quote/escape knobs as the standard :mod:`csv` module
    (``quotechar``, ``escapechar``, ``doublequote``) and normalizes both ``\\r\\n`` and bare ``\\r``
    terminators to ``\\n``, like universal-newline mode.

    The currently open (possibly multi-line, quoted) record accumulates in :attr:`pending`; it is only
    released when a terminator closes it, which guarantees partially written lines are never handed to
    the CSV parser.
    """

    def __init__(self, quotechar, escapechar, doublequote):
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.in_quotes = False
        self.pending = ""
        # Position in pending up to which the state machine has already been advanced. Bytes before it
        # are retained (they belong to the still-open record) but must not be scanned again.
        self._scanned = 0
        # Position in pending up to which complete records have already been released. The prefix before
        # it is dropped after each push; the [released, scanned) suffix is the still-open record.
        self._released = 0

    def push(self, text):
        """Append ``text`` to the pending buffer and return all records completed by this push."""
        self.pending += text
        records = []
        quotechar, escapechar = self.quotechar, self.escapechar
        i, length = self._scanned, len(self.pending)
        released = self._released

        while i < length:
            char = self.pending[i]
            if self.in_quotes:
                if escapechar and char == escapechar:
                    # The next character is a literal, whatever it is.
                    i += 2
                    continue
                if char == quotechar:
                    if self.doublequote and i + 1 < length and self.pending[i + 1] == quotechar:
                        # Escaped quote inside a quoted field.
                        i += 2
                        continue
                    self.in_quotes = False
                i += 1
                continue

            if char == quotechar:
                self.in_quotes = True
                i += 1
            elif char == "\n":
                records.append(self.pending[released:i] + "\n")
                i += 1
                released = i
            elif char == "\r":
                records.append(self.pending[released:i] + "\n")
                i += 1
                if i < length and self.pending[i] == "\n":
                    # Swallow the second half of a CRLF terminator.
                    i += 1
                released = i
            else:
                i += 1

        # Drop only the fully released prefix; the open record (possibly quoted, possibly multi-line)
        # stays in pending and both cursors shift with the truncation.
        self.pending = self.pending[released:]
        self._scanned = i - released
        self._released = 0
        return "".join(records)

    @property
    def residual(self):
        return self.pending

    def flush_residual(self):
        residual, self.pending = self.pending, ""
        self._scanned, self._released = 0, 0
        return residual


@use_context
class CsvShardReader(Configurable):
    """
    Continuously collect rows from a sequence of progressively written UTF-8 CSV shards.

    Shard names are built from :attr:`filename_pattern`, which must contain one ``{index}`` placeholder.
    Indices are consecutive integers starting at :attr:`start_index`, zero-padded to :attr:`index_digits`
    digits when this option is positive. Completion is declared through :attr:`completion_marker`: once that
    file exists, the current shard is the last one and the pipeline ends as soon as it is read in full.

    Every shard must start with the same header line as the first shard read. Rows are yielded as plain
    tuples, in shard index then logical record order, once per physical record (duplicate rows kept).
    """

    # CSV dialect options (same knobs as CsvHandler/CsvReader).
    delimiter = Option(str, default=csv.excel.delimiter, required=False)
    quotechar = Option(str, default=csv.excel.quotechar, required=False)
    escapechar = Option(str, default=csv.excel.escapechar, required=False)
    doublequote = Option(bool, default=csv.excel.doublequote, required=False)
    skipinitialspace = Option(bool, default=csv.excel.skipinitialspace, required=False)
    lineterminator = Option(str, default=csv.excel.lineterminator, required=False)
    quoting = Option(int, default=csv.excel.quoting, required=False)

    path = Option(
        filesystem_path,
        required=True,
        positional=True,
        __doc__="Directory (within the provided filesystem) containing the shard files.",
    )
    encoding = Option(
        str,
        default="utf-8",
        required=False,
        __doc__="Text encoding of the shard files. Only UTF-8 compatible decoding is guaranteed.",
    )
    filename_pattern = Option(
        str,
        default="shard-{index}.csv",
        required=False,
        __doc__="Shard filename pattern. Must contain exactly one '{index}' placeholder.",
    )
    index_digits = Option(
        int,
        default=0,
        required=False,
        __doc__="Zero-padding width for shard indices. Zero means no padding.",
    )
    start_index = Option(
        int,
        default=0,
        required=False,
        __doc__="Consecutive index of the first shard to collect.",
    )
    completion_marker = Option(
        str,
        default="_SUCCESS",
        required=False,
        __doc__="""
        Name (relative to the shard directory) of the marker file the producer creates once the last
        shard is fully written. The reader only sends a normal EOF once this marker exists and every
        shard has been read in full.
    """,
    )
    poll_interval = Option(
        float,
        default=0.2,
        required=False,
        __doc__="Delay, in seconds, between polls while waiting for more data or a new shard.",
    )
    stable_polls = Option(
        int,
        default=2,
        required=False,
        __doc__="""
        Number of consecutive polls on which a sealed shard must keep the same size before being parsed
        fully. Gives the producer time to flush its last buffer. Only applied to sealed shards, never to
        the live shard.
    """,
    )
    fields = Option(
        tuple_or_const,
        required=False,
        __doc__="Expected header fields. When set, every shard header is also checked against this value.",
    )

    fs = Service("fs", __doc__="The filesystem instance to use.")

    def get_dialect_kwargs(self):
        return {
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "escapechar": self.escapechar,
            "doublequote": self.doublequote,
            "skipinitialspace": self.skipinitialspace,
            "lineterminator": self.lineterminator,
            "quoting": self.quoting,
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate_options()
        prefix, suffix = self.filename_pattern.split("{index}")
        self._shard_name_re = re.compile(
            "^" + re.escape(prefix) + r"(\d+)" + re.escape(suffix) + "$"
        )

    def _validate_options(self):
        pattern = self.filename_pattern
        if pattern.count("{index}") != 1:
            raise ValueError(
                "filename_pattern must contain exactly one '{{index}}' placeholder, got {!r}.".format(pattern)
            )
        try:
            rendered = pattern.format(index="0")
        except (IndexError, KeyError) as exc:
            raise ValueError("Invalid filename_pattern {!r}: {}.".format(pattern, exc)) from exc
        if rendered == pattern:
            raise ValueError("filename_pattern {!r} does not render its {index} placeholder.".format(pattern))
        if self.index_digits < 0:
            raise ValueError("index_digits must be zero or positive, got {}.".format(self.index_digits))
        if self.stable_polls < 1:
            raise ValueError("stable_polls must be at least 1, got {}.".format(self.stable_polls))
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be strictly positive, got {}.".format(self.poll_interval))

    def shard_name(self, index):
        return self.filename_pattern.format(index=str(index).zfill(self.index_digits))

    def shard_path(self, index):
        directory = self.path.strip("/")
        name = self.shard_name(index)
        return "/".join((directory, name)) if directory else name

    @property
    def marker_path(self):
        marker = self.completion_marker.lstrip("/")
        directory = self.path.strip("/")
        if directory and not marker.startswith(directory + "/"):
            return "/".join((directory, marker))
        return marker

    @Method(
        positional=False,
        __doc__="Builds the CSV reader used to parse complete records. Defaults to builtin csv.reader(...).",
    )
    def reader_factory(self, file):
        return csv.reader(file, **self.get_dialect_kwargs())

    def read(self, context, *, fs):
        context.setdefault("header", None)

        index = self.start_index

        while True:
            path = self.shard_path(index)

            # A shard only becomes available once the producer created it. Its existence also seals the
            # previous shard, so wait here without ending the pipeline.
            while not fs.exists(path):
                final_index = self._final_index(fs)
                if final_index is None:
                    # Producer still working: temporary end-of-data, keep polling.
                    time.sleep(self.poll_interval)
                    continue
                # The producer declared completion and this shard is not there: the sequence is broken.
                if final_index < self.start_index:
                    raise CsvShardError(
                        "Completion marker {!r} exists but the first shard {!r} is missing.".format(
                            self.marker_path, self.shard_path(self.start_index)
                        )
                    )
                # Raises an explicit CsvShardError listing every missing index in the declared range.
                self._check_continuity(fs, index)
                raise CsvShardError(
                    "Missing shard {!r}: shard indices must be consecutive, but the completion marker "
                    "{!r} was already created.".format(path, self.marker_path)
                )

            final_index = self._final_index(fs)
            sealed = self._is_sealed(fs, index, final_index)
            yield from self._read_shard(context, fs, path, index, sealed)

            final_index = self._final_index(fs)
            if final_index is not None:
                self._check_continuity(fs, index)
                if index == final_index:
                    # Last shard fully read and validated: emit a normal EOF by stopping the generator.
                    return
            index += 1

    __call__ = read

    def _final_index(self, fs):
        """The last shard index, once the completion marker exists; resolved lazily on every poll."""
        if not fs.exists(self.marker_path):
            return None
        indices = self._available_indices(fs)
        if not indices:
            return self.start_index - 1
        return max(indices)

    def _available_indices(self, fs):
        """Indices of shard files currently present in the shard directory."""
        directory = self.path.strip("/")
        names = fs.listdir(directory) if directory and fs.exists(directory) else fs.listdir(".")
        indices = set()
        for name in names:
            match = self._shard_name_re.match(name)
            if match:
                indices.add(int(match.group(1)))
        return indices

    def _check_continuity(self, fs, index):
        """Once the sequence is complete, no shard index may be missing in the declared range."""
        final_index = self._final_index(fs)
        if final_index is None:
            return
        available = self._available_indices(fs)
        missing = [i for i in range(self.start_index, final_index + 1) if i not in available]
        if missing:
            raise CsvShardError(
                "Missing shard(s) {}: shard indices must be consecutive from {} to {}, but the completion "
                "marker {!r} was already created.".format(
                    ", ".join(self.shard_path(i) for i in missing),
                    self.start_index,
                    final_index,
                    self.marker_path,
                )
            )

    def _is_sealed(self, fs, index, final_index):
        if final_index is None:
            final_index = self._final_index(fs)
        if final_index is not None and index == final_index:
            return True
        # Any higher shard existing (including a gap) means the producer moved on: this one is sealed.
        return bool(self._higher_indices(fs, index))

    def _higher_indices(self, fs, index):
        return [i for i in self._available_indices(fs) if i > index]

    def _wait_stable_size(self, fs, path):
        """Wait until a sealed shard keeps the same size across ``stable_polls`` consecutive polls."""
        last_size, stable = -1, 0
        while stable < self.stable_polls:
            size = fs.getsize(path)
            if size == last_size:
                stable += 1
            else:
                last_size, stable = size, 1
            if stable < self.stable_polls:
                time.sleep(self.poll_interval)

    def _read_shard(self, context, fs, path, index, initially_sealed):
        decoder = codecs.getincrementaldecoder(self.encoding)(errors="strict")
        splitter = _CsvLineSplitter(self.quotechar, self.escapechar, self.doublequote)
        offset = 0
        sealed = initially_sealed
        saw_header = False

        while True:
            chunk = self._read_chunk(fs, path, offset)
            offset += len(chunk)

            if not chunk:
                if sealed:
                    # Finalize the shard: flush any pending multibyte character, then validate it.
                    tail = self._decode_final(decoder, path)
                    if tail:
                        saw_header = (
                            yield from self._parse_and_emit(context, splitter, tail, path, saw_header)
                        ) or saw_header
                    residual = splitter.flush_residual()
                    if splitter.in_quotes:
                        raise CsvShardError(
                            "Sealed shard {!r} ends inside an unterminated quoted field; refusing to join "
                            "the fragment across shard boundaries.".format(path)
                        )
                    if residual:
                        raise CsvShardError(
                            "Sealed shard {!r} ends with an unterminated CSV record {!r}; refusing to join "
                            "the fragment across shard boundaries.".format(path, residual)
                        )
                    if not saw_header:
                        raise CsvShardError(
                            "Sealed shard {!r} does not contain the expected header line.".format(path)
                        )
                    return

                time.sleep(self.poll_interval)
                if self._is_sealed(fs, index, None):
                    sealed = True
                    self._wait_stable_size(fs, path)
                continue

            try:
                text = decoder.decode(chunk, final=False)
            except UnicodeDecodeError as exc:
                raise CsvShardError(
                    "Shard {!r} contains bytes that are not valid {encoding}: {exc}".format(
                        path, encoding=self.encoding, exc=exc
                    )
                ) from exc

            if text:
                saw_header = (
                    yield from self._parse_and_emit(context, splitter, text, path, saw_header)
                ) or saw_header

    def _read_chunk(self, fs, path, offset):
        with fs.openbin(path, mode="rb") as binary:
            binary.seek(offset)
            return binary.read()

    def _decode_final(self, decoder, path):
        try:
            return decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            raise CsvShardError(
                "Shard {!r} ends on an incomplete {encoding} sequence (bad or truncated encoding): {exc}".format(
                    path, encoding=self.encoding, exc=exc
                )
            ) from exc

    def _parse_and_emit(self, context, splitter, text, path, saw_header):
        """Parse the complete records contained in ``text`` and yield data rows.

        Returns True when the shard header was part of the parsed records of this or a previous chunk.
        """
        complete_text = splitter.push(text)
        reader = self.reader_factory(io.StringIO(complete_text))

        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                raise CsvShardError("Malformed CSV in shard {!r}: {exc}".format(path, exc=exc)) from exc

            row = tuple(row)

            if not row:
                # Blank physical lines carry no record and must not be mistaken for a shard header.
                continue

            if not saw_header:
                # First non-blank record of this shard: it is the shard header.
                if context.header is None:
                    # First shard of the sequence establishes the shared header.
                    if self.fields and row != self.fields:
                        raise CsvShardError(
                            "Shard {!r} header {!r} differs from expected fields {!r}.".format(
                                path, row, self.fields
                            )
                        )
                    context.header = row
                    context.set_output_fields(row)
                elif row != context.header:
                    # Subsequent shards must repeat the exact same header.
                    raise CsvShardError(
                        "Shard {!r} header {!r} differs from the first shard header {!r}.".format(
                            path, row, context.header
                        )
                    )
                saw_header = True
                continue

            yield row

        return saw_header
