import threading

import pytest

from bonobo import CsvReader, CsvShardReader, open_fs
from bonobo.config.processors import ContextCurrifier
from bonobo.constants import BEGIN, EMPTY, END
from bonobo.errors import InactiveReadableError, UnrecoverableError
from bonobo.nodes.io.csv_shard import CsvShardError, _CsvLineSplitter
from bonobo.util.testing import BufferingNodeExecutionContext

POLL = {"poll_interval": 0.01, "stable_polls": 1}


class _DummyContext:
    """Minimal stand-in for a NodeExecutionContext for driving the node directly."""

    header = None

    def setdefault(self, attr, value):
        if not hasattr(self, attr) or getattr(self, attr) is None:
            setattr(self, attr, value)

    def set_output_fields(self, fields):
        self.output_fields = fields


class ShardFixture:
    def __init__(self, tmpdir):
        self.fs = open_fs(str(tmpdir))
        self.services = {"fs": self.fs}

    def write(self, name, data):
        self.fs.appendbytes(name, data if isinstance(data, bytes) else data.encode("utf-8"))

    def complete(self, name="_SUCCESS"):
        self.fs.touch(name)

    def reader(self, **kwargs):
        options = dict(POLL)
        options.update(kwargs)
        return CsvShardReader(".", **options)

    def collect(self, reader=None, max_steps=500):
        """Drive the node through the real execution context until its normal EOF."""
        from queue import Empty

        context = BufferingNodeExecutionContext(reader or self.reader(), services=self.services)
        context.start()
        # Mirror GraphExecutionContext.run_until_complete: BEGIN opens the input, EMPTY drives the
        # extractor (one step runs its generator to completion), END closes the input afterwards.
        context.write(BEGIN, EMPTY, END)
        steps = 0
        while True:
            try:
                context.step()
            except InactiveReadableError:
                break
            except Empty:
                continue
            steps += 1
            if steps >= max_steps:
                raise AssertionError("reader did not reach EOF after {} steps".format(max_steps))
        return [tuple(row) for row in context.get_buffer()], context

    def collect_directly(self, reader=None):
        """Drive the node without an execution context so fatal errors propagate as exceptions."""
        node = reader or self.reader()
        stack = ContextCurrifier(node, fs=self.fs)
        stack.setup(_DummyContext())
        try:
            return list(stack(()))
        finally:
            stack.teardown()


###
# Line splitter unit tests
###


def test_splitter_emits_only_terminated_records():
    splitter = _CsvLineSplitter('"', None, True)
    assert splitter.push("a,b\n1,x") == "a,b\n"
    assert splitter.flush_residual() == "1,x"
    assert not splitter.in_quotes


def test_splitter_keeps_quoted_newline_in_residual():
    splitter = _CsvLineSplitter('"', None, True)
    assert splitter.push('a,b\n1,"line1\n') == "a,b\n"
    assert splitter.in_quotes
    # A terminator inside the still-open quote does not end the record: nothing is emitted and the
    # whole physical fragment stays in the pending buffer.
    assert splitter.push("still inside\n") == ""
    assert splitter.in_quotes
    assert splitter.pending == '1,"line1\nstill inside\n'
    # Once the quote closes and a terminator is reached, the whole multi-line record is released.
    complete = splitter.push('line2",x\n2,y\n')
    assert complete == '1,"line1\nstill inside\nline2",x\n2,y\n'
    assert not splitter.in_quotes
    assert splitter.flush_residual() == ""


def test_splitter_handles_doublequote_and_crlf():
    splitter = _CsvLineSplitter('"', None, True)
    complete = splitter.push('a,b\r\n1,"a""b"\r\n2,x\r')
    assert complete == 'a,b\n1,"a""b"\n2,x\n'
    assert splitter.flush_residual() == ""


def test_splitter_honours_escapechar():
    splitter = _CsvLineSplitter('"', "\\", True)
    splitter.push('1,"x\\"')
    assert splitter.in_quotes  # \" is an escaped quote, field still open
    splitter.push('y"\n')
    assert not splitter.in_quotes


###
# Happy path
###


def test_collects_sealed_shards_in_order_and_keeps_duplicates(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "id\n1\n2\n2\n")
    fixture.write("shard-1.csv", "id\n3\n")
    fixture.write("shard-2.csv", "id\n4\n4\n")
    fixture.complete()

    rows, context = fixture.collect()
    assert rows == [("1",), ("2",), ("2",), ("3",), ("4",), ("4",)]
    assert context.get_output_fields() == ("id",)


def test_eof_only_after_marker_and_last_shard_drained(tmpdir):
    import time

    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "a,b\n1,x\n")

    context = BufferingNodeExecutionContext(fixture.reader(), services=fixture.services)
    context.start()
    context.write(BEGIN, EMPTY, END)

    # Drive the blocked reader in a thread; it must not reach EOF while the producer is silent.
    def drain():
        while True:
            try:
                context.step()
            except InactiveReadableError:
                return
            except Exception:
                return

    worker = threading.Thread(target=drain, daemon=True)
    worker.start()
    time.sleep(0.25)
    assert worker.is_alive()  # still waiting, no EOF
    assert [tuple(r) for r in context.get_buffer()] == [("1", "x")]

    fixture.write("shard-1.csv", "a,b\n2,y\n")
    fixture.complete()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert [tuple(r) for r in context.get_buffer()] == [("1", "x"), ("2", "y")]


def test_partial_multibyte_character_is_not_emitted(tmpdir):
    fixture = ShardFixture(tmpdir)
    prefix = "a,b\n1,".encode("utf-8")
    payload = "你好\n2,z\n".encode("utf-8")
    fixture.write("shard-0.csv", prefix + payload[:1])  # first byte of 你 only
    fixture.complete()

    # Sealed shard ending on an incomplete UTF-8 sequence must fail.
    with pytest.raises(CsvShardError):
        fixture.collect_directly()


def test_partial_multibyte_completes_on_next_poll(tmpdir):
    import time

    fixture = ShardFixture(tmpdir)
    full = "a,b\n1,你好\n2,z\n".encode("utf-8")
    cut = len("a,b\n1,".encode("utf-8")) + 1
    fixture.write("shard-0.csv", full[:cut])

    def producer():
        time.sleep(0.15)
        fixture.fs.appendbytes("shard-0.csv", full[cut:])
        # Only declare completion once the shard is fully written.
        fixture.complete()

    threading.Thread(target=producer, daemon=True).start()
    rows, _ = fixture.collect()
    assert rows == [("1", "你好"), ("2", "z")]


def test_unterminated_quoted_field_waits_then_emits_when_closed(tmpdir):
    import time

    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", 'a,b\n1,"quo')

    def producer():
        time.sleep(0.15)
        fixture.fs.appendbytes("shard-0.csv", b'ted"\n2,done\n')
        # Only declare completion once the quoted field is closed.
        fixture.complete()

    threading.Thread(target=producer, daemon=True).start()
    rows, _ = fixture.collect()
    assert rows == [("1", "quoted"), ("2", "done")]


def test_embedded_newline_in_quoted_field(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", 'a,b\n1,"line1\nline2",x\n2,y\n')
    fixture.complete()
    rows, _ = fixture.collect()
    assert rows == [("1", "line1\nline2", "x"), ("2", "y")]


def test_custom_pattern_padding_and_start_index(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("part-01.csv", "a\nx\n")
    fixture.write("part-02.csv", "a\ny\n")
    fixture.complete()
    rows, _ = fixture.collect(
        fixture.reader(filename_pattern="part-{index}.csv", index_digits=2, start_index=1)
    )
    assert rows == [("x",), ("y",)]


def test_fields_option_sets_output_fields(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "a,b\n1,x\n")
    fixture.complete()
    rows, context = fixture.collect(fixture.reader(fields=("a", "b")))
    assert rows == [("1", "x")]
    assert context.get_output_fields() == ("a", "b")


###
# Failure modes: all must be unrecoverable and explicit
###


@pytest.mark.parametrize(
    "files,message_fragment",
    [
        ({"shard-0.csv": b"a,b\n1,\xff\n"}, "not valid utf-8"),
        ({"shard-0.csv": b"a,b\n1,ok\n2,partial"}, "unterminated CSV record"),
        ({"shard-0.csv": b'a,b\n1,"open quote\n2,x\n'}, "unterminated quoted field"),
        ({"shard-0.csv": b"a,b\n1,\xe4\xbd"}, "incomplete"),
        ({"shard-0.csv": b""}, "header"),
    ],
)
def test_sealed_shard_defects_fail_explicitly(tmpdir, files, message_fragment):
    fixture = ShardFixture(tmpdir)
    for name, data in files.items():
        fixture.write(name, data)
    fixture.complete()
    with pytest.raises(CsvShardError) as excinfo:
        fixture.collect_directly()
    assert message_fragment in str(excinfo.value)
    assert isinstance(excinfo.value, UnrecoverableError)


def test_divergent_header_between_shards_fails(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "a,b\n1,x\n")
    fixture.write("shard-1.csv", "a,c\n2,y\n")
    fixture.complete()
    with pytest.raises(CsvShardError) as excinfo:
        fixture.collect_directly()
    assert "differs from the first shard header" in str(excinfo.value)


def test_header_repeated_on_every_shard_is_consumed_not_delivered(tmpdir):
    # Regression: each shard repeats the shared header, which must be checked and skipped, never mixed
    # up with data rows even when shards are discovered/appended progressively.
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "id,name\n0,user0\n1,user1\n")
    fixture.write("shard-1.csv", "id,name\n2,user2\n")
    fixture.write("shard-2.csv", "id,name\n3,user3\n4,user4\n")
    fixture.complete()
    rows, _ = fixture.collect()
    assert rows == [
        ("0", "user0"), ("1", "user1"), ("2", "user2"), ("3", "user3"), ("4", "user4")
    ]


def test_fields_option_mismatch_fails(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "a,b\n1,x\n")
    fixture.complete()
    with pytest.raises(CsvShardError):
        fixture.collect_directly(fixture.reader(fields=("x", "y")))


def test_missing_shard_in_completed_sequence_fails(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "a,b\n1,x\n")
    fixture.write("shard-2.csv", "a,b\n2,y\n")
    fixture.complete()
    with pytest.raises(CsvShardError) as excinfo:
        fixture.collect_directly()
    assert "Missing shard" in str(excinfo.value)


def test_marker_without_any_shard_fails(tmpdir):
    fixture = ShardFixture(tmpdir)
    fixture.complete()
    with pytest.raises(CsvShardError):
        fixture.collect_directly()


def test_residual_record_is_not_joined_across_shards(tmpdir):
    fixture = ShardFixture(tmpdir)
    # Shard 0 ends without a terminator; shard 1 starts a brand new (header-checked) file.
    fixture.write("shard-0.csv", "a,b\n1,ok\n2,partial")
    fixture.write("shard-1.csv", "a,b\n3,next\n")
    fixture.complete()
    with pytest.raises(CsvShardError) as excinfo:
        fixture.collect_directly()
    assert "unterminated CSV record" in str(excinfo.value)


def test_fatal_error_marks_execution_context_defunct(tmpdir):
    # Through the real execution context, an unrecoverable error must mark the node defunct (and shut
    # its input down) instead of being swallowed silently.
    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", b"a,b\n1,\xff\n")
    fixture.complete()

    context = BufferingNodeExecutionContext(fixture.reader(), services=fixture.services)
    context.start()
    context.write(BEGIN, EMPTY, END)
    while not context.defunct:
        try:
            context.step()
        except InactiveReadableError:
            break
    context.stop()
    assert context.defunct
    assert context.xstatus == 70


def test_file_handles_are_released_after_eof(tmpdir):
    import gc

    fixture = ShardFixture(tmpdir)
    fixture.write("shard-0.csv", "a,b\n1,x\n")
    fixture.complete()

    opened = []
    original_openbin = fixture.fs.openbin

    def tracking_openbin(path, mode="rb", **kwargs):
        handle = original_openbin(path, mode, **kwargs)
        opened.append([handle, False])
        original_close = handle.close

        def close():
            opened[-1][1] = True
            return original_close()

        handle.close = close
        return handle

    fixture.fs.openbin = tracking_openbin
    fixture.collect()
    gc.collect()
    assert opened, "expected at least one shard open"
    assert all(closed for _, closed in opened), "a shard file handle was not released"


###
# Configuration validation
###


def test_invalid_filename_pattern_rejected():
    with pytest.raises(ValueError):
        CsvShardReader(".", filename_pattern="shard.csv", **POLL)
    with pytest.raises(ValueError):
        CsvShardReader(".", filename_pattern="shard-{n}.csv", **POLL)


def test_invalid_timing_options_rejected():
    with pytest.raises(ValueError):
        CsvShardReader(".", poll_interval=0, **{k: v for k, v in POLL.items() if k != "poll_interval"})
    with pytest.raises(ValueError):
        CsvShardReader(".", stable_polls=0, **{k: v for k, v in POLL.items() if k != "stable_polls"})


###
# Legacy reader untouched
###


def test_legacy_csv_reader_is_separate_class():
    assert CsvReader is not CsvShardReader
    assert not issubclass(CsvShardReader, CsvReader)
