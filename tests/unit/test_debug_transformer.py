"""Unit tests for DebugTransformer — no real Spark required."""
from __future__ import annotations

from unittest.mock import MagicMock

from spark_quick_start.orchestration.transformers.debug import DebugTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def _mock_df(rows=None, columns=None):
    """Return a mock DataFrame that satisfies DebugTransformer's new API.

    The new implementation calls df.count(), df.schema.fields, and
    df.limit(n).collect() — not df.show() or df.printSchema().
    """
    df = MagicMock()
    # count() returns a row count
    df.count.return_value = len(rows) if rows is not None else 0
    # schema.fields is a list of mock field objects
    field = MagicMock()
    field.name = "col"
    field.dataType.simpleString.return_value = "string"
    field.nullable = True
    df.schema.fields = columns if columns is not None else [field]
    # limit(n).collect() returns the sample rows
    sample = rows if rows is not None else []
    df.limit.return_value.collect.return_value = sample
    # columns attribute used to build the data table header
    df.columns = [f.name for f in df.schema.fields]
    return df


def _mock_state(df=None, frames=None):
    state = MagicMock(spec=PipelineState)
    state.df = df or _mock_df()
    state.frames = frames or {}
    state.halted = False
    return state


def _mock_ctx():
    ctx = MagicMock()
    ctx.runtime_ctx.capture_row_counts = False
    return ctx


# ── enabled=False is a pure pass-through ──────────────────────────────────────

def test_disabled_returns_same_state():
    state = _mock_state()
    t = DebugTransformer(enabled=False)
    result = t(state, _mock_ctx())
    assert result is state


def test_disabled_never_calls_count():
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer(enabled=False)(state, _mock_ctx())
    df.count.assert_not_called()


def test_disabled_never_calls_limit():
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer(enabled=False)(state, _mock_ctx())
    df.limit.assert_not_called()


# ── enabled=True produces output ──────────────────────────────────────────────

def test_enabled_calls_limit_with_n():
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test", n=5)(state, _mock_ctx())
    df.limit.assert_called_once_with(5)


def test_enabled_calls_count():
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test")(state, _mock_ctx())
    df.count.assert_called()


def test_show_schema_true_accesses_schema_fields(capsys):
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test", show_schema=True)(state, _mock_ctx())
    captured = capsys.readouterr()
    # Schema table header appears in output
    assert "Schema" in captured.out


def test_show_schema_false_omits_schema_section(capsys):
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test", show_schema=False)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "Schema" not in captured.out


def test_n_zero_skips_limit():
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test", n=0)(state, _mock_ctx())
    df.limit.assert_not_called()


def test_n_zero_still_shows_schema(capsys):
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test", n=0, show_schema=True)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "Schema" in captured.out


# ── Markdown table output ─────────────────────────────────────────────────────

def test_output_contains_markdown_table_header(capsys):
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test")(state, _mock_ctx())
    captured = capsys.readouterr()
    # Markdown table separator row
    assert "| ---" in captured.out


def test_output_contains_column_name(capsys):
    df = _mock_df()
    state = _mock_state(df=df)
    DebugTransformer("test")(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "col" in captured.out


def test_truncate_clips_long_cell_values(capsys):
    field = MagicMock()
    field.name = "val"
    field.dataType.simpleString.return_value = "string"
    field.nullable = True
    row = MagicMock()
    row.__getitem__ = lambda self, k: "A" * 200
    df = _mock_df(rows=[row], columns=[field])
    df.columns = ["val"]
    state = _mock_state(df=df)
    DebugTransformer("test", truncate=10)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "…" in captured.out
    assert "A" * 200 not in captured.out


def test_truncate_zero_does_not_clip(capsys):
    field = MagicMock()
    field.name = "val"
    field.dataType.simpleString.return_value = "string"
    field.nullable = True
    long_val = "A" * 200
    row = MagicMock()
    row.__getitem__ = lambda self, k: long_val
    df = _mock_df(rows=[row], columns=[field])
    df.columns = ["val"]
    state = _mock_state(df=df)
    DebugTransformer("test", truncate=0)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert long_val in captured.out


def test_pipe_characters_in_cell_are_escaped(capsys):
    field = MagicMock()
    field.name = "val"
    field.dataType.simpleString.return_value = "string"
    field.nullable = True
    row = MagicMock()
    row.__getitem__ = lambda self, k: "a|b|c"
    df = _mock_df(rows=[row], columns=[field])
    df.columns = ["val"]
    state = _mock_state(df=df)
    DebugTransformer("test", truncate=0)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "a\\|b\\|c" in captured.out


# ── Error swallowing ──────────────────────────────────────────────────────────

def test_exception_in_count_does_not_propagate():
    df = _mock_df()
    df.count.side_effect = RuntimeError("Spark dead")
    state = _mock_state(df=df)
    result = DebugTransformer("test")(state, _mock_ctx())
    assert result is state


def test_exception_warns_to_stderr_when_no_logger_set(capsys):
    df = _mock_df()
    df.count.side_effect = RuntimeError("Spark dead")
    state = _mock_state(df=df)
    t = DebugTransformer("test")
    t(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "[DebugTransformer] WARNING:" in captured.err


def test_exception_calls_logger_warning_when_logger_set():
    df = _mock_df()
    df.count.side_effect = RuntimeError("Spark dead")
    state = _mock_state(df=df)
    logger = MagicMock()
    t = DebugTransformer("test")
    t._logger = logger
    t(state, _mock_ctx())
    logger.warning.assert_called_once()


# ── Frames inspection ─────────────────────────────────────────────────────────

def test_named_frame_limit_called():
    frame_df = _mock_df()
    state = _mock_state(frames={"target": frame_df})
    DebugTransformer("test", frames=["target"])(state, _mock_ctx())
    frame_df.limit.assert_called_once()


def test_missing_frame_writes_warning(capsys):
    df = _mock_df()
    state = _mock_state(df=df, frames={})
    DebugTransformer("test", frames=["target"])(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "not found" in captured.out


def test_missing_frame_does_not_raise():
    df = _mock_df()
    state = _mock_state(df=df, frames={})
    result = DebugTransformer("test", frames=["target"])(state, _mock_ctx())
    assert result is state


# ── Return value ──────────────────────────────────────────────────────────────

def test_returns_original_state():
    state = _mock_state()
    result = DebugTransformer("test")(state, _mock_ctx())
    assert result is state


# ── Header formatting ─────────────────────────────────────────────────────────

def test_format_header_contains_label():
    header = DebugTransformer._format_header("my-checkpoint")
    assert "my-checkpoint" in header
    assert "DEBUG CHECKPOINT" in header


# ---------------------------------------------------------------------------
# Warning when sample rows land in a persisted log
# ---------------------------------------------------------------------------

def _ctx(*, suppress: bool = False, log_dir: str = "/Volumes/cat/raw/logs"):
    """LoadContext stub carrying only what the warning path reads."""
    from spark_quick_start.orchestration.models import LoggingContext

    ctx = MagicMock()
    ctx.logging_ctx = LoggingContext({
        "log_file_name": "n", "logging_level": "INFO", "log_file_dir": log_dir,
        "suppress_stdout_tail": suppress,
    })
    return ctx


class TestPersistedLogWarning:
    """Emitting rows writes real data into the run's log file.

    stdout is captured into the persisted payload, and logs land on a lakehouse
    volume whose read permissions do not generally match the source table's.
    The transformer warns rather than refuses — showing rows is its purpose,
    and blocking it would only push the same data through a hand-written print.
    """

    def test_warns_when_rows_are_emitted_into_a_persisted_log(self):
        t = DebugTransformer("after staging", n=5)
        t._logger = MagicMock()
        t(_mock_state(_mock_df(rows=[{"col": "a"}])), _ctx())

        assert t._logger.warning.called
        message = t._logger.warning.call_args[0][0]
        assert "/Volumes/cat/raw/logs" in message
        assert "suppress_stdout_tail" in message
        assert "n=0 to print no rows" in message
        assert "schema only" not in message

    def test_silent_when_stdout_is_not_persisted(self):
        t = DebugTransformer("after staging", n=5)
        t._logger = MagicMock()
        t(_mock_state(_mock_df(rows=[{"col": "a"}])), _ctx(suppress=True))

        assert not t._logger.warning.called

    def test_silent_for_schema_only_output(self):
        """n=0 emits column names, not values — nothing sensitive to flag."""
        t = DebugTransformer("after staging", n=0)
        t._logger = MagicMock()
        t(_mock_state(_mock_df(rows=[])), _ctx())

        assert not t._logger.warning.called

    def test_warns_only_once_per_run(self):
        """A transformer inspecting several frames must not warn per frame."""
        df = _mock_df(rows=[{"col": "a"}])
        t = DebugTransformer("after staging", n=5, frames=["target", "candidates"])
        t._logger = MagicMock()
        t(_mock_state(df, frames={"target": df, "candidates": df}), _ctx())

        assert t._logger.warning.call_count == 1

    def test_does_not_abort_the_pipeline(self):
        """The warning path must not break the transformer's pass-through contract."""
        state = _mock_state(_mock_df(rows=[{"col": "a"}]))
        t = DebugTransformer("after staging", n=5)
        t._logger = MagicMock()

        assert t(state, _ctx()) is state


def test_warning_latch_resets_between_runs():
    """Fabric/Databricks keep the kernel alive between pipeline runs.

    A module-level pipeline is built once and .run() called repeatedly, so a
    latch set only in __init__ would warn on the first run of the session and
    stay silent for every run after it — exactly the runs a long-lived session
    accumulates.
    """
    df = _mock_df(rows=[{"col": "a"}])
    t = DebugTransformer("after staging", n=5)
    t._logger = MagicMock()

    t(_mock_state(df), _ctx())
    t(_mock_state(df), _ctx())

    assert t._logger.warning.call_count == 2
