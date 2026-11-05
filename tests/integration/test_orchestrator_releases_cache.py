"""LoadOrchestrator.run() releases every frame a step cached when the run ends.

The frames a Materialize step cached (``materialize_strategy: "cache"``) ride on
``PipelineState.cached``; the orchestrator keeps the last state it saw after each step and
unpersists them all in ``run()``'s ``finally`` — on success, on a step's error and on an early
exit — and a failing unpersist is logged, never allowed to replace the run's real outcome.

Stub transformers and mock frames: what is under test is the orchestrator's release, not Spark.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.transformers.base import ProcessTransformer, SourceTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

_ORCH = "spark_quick_start.orchestration.orchestrator"


class _Source(SourceTransformer):
    def _produce(self, ctx):
        df = MagicMock()
        df.columns = ["id", "scdDeletedFlag"]
        df.rdd.getNumPartitions.return_value = 1
        return PipelineState(df=df)


class _Cache(ProcessTransformer):
    """Stands in for a Materialize step in cache mode."""

    def __init__(self, frame):
        self._frame = frame

    def _run(self, state, ctx):
        return state.with_cached(self._frame)


class _Halt(ProcessTransformer):
    def _run(self, state, ctx):
        return state.with_halted("nothing to do")


class _Boom(ProcessTransformer):
    def _run(self, state, ctx):
        raise RuntimeError("step failed")


def _run(*steps):
    """Run the real pipeline over *steps*; return (result, logger)."""
    orch = LoadOrchestrator(
        load_context_json=json.dumps({}), spark=MagicMock(), adapter=MockPlatformAdapter(),
    )
    orch._transformers = [_Source(), *steps]
    with patch(f"{_ORCH}.LoadContextManager") as ctx_mgr, \
         patch(f"{_ORCH}.OrchestratorLogger") as logger_cls, \
         patch("spark_quick_start.orchestration.load_pipeline.log_runtime"):
        ctx = MagicMock()
        ctx.runtime_ctx.correlation_id = "corr"
        ctx.runtime_ctx.capture_row_counts = False
        ctx.runtime_ctx.capture_partitions = False
        ctx.global_ctx.target_table_path = "/tmp/target"
        ctx.scd_ctx.business_key_column = "id"
        ctx.scd_ctx.deleted_flag_column = "scdDeletedFlag"
        ctx.scd_ctx.source_system_from_source = False
        ctx.hwm_ctx = None
        ctx_mgr.return_value = ctx
        logger = MagicMock()
        logger.finalize.return_value = ""
        logger_cls.from_logging_context.return_value = logger
        logger_cls.return_value = logger
        return orch.run(), logger


def test_every_cached_frame_is_released_after_a_successful_run():
    a, b = MagicMock(), MagicMock()
    result, _ = _run(_Cache(a), _Cache(b))
    assert result.status == "success"
    a.unpersist.assert_called_once()
    b.unpersist.assert_called_once()


def test_cached_frames_are_released_after_a_later_step_raises():
    a = MagicMock()
    result, _ = _run(_Cache(a), _Boom())
    assert result.status == "error"
    assert "step failed" in json.dumps(result.error_details)
    a.unpersist.assert_called_once()


def test_cached_frames_are_released_after_an_early_exit():
    a = MagicMock()
    result, _ = _run(_Cache(a), _Halt())
    assert result.status == "success" and result.early_exit
    a.unpersist.assert_called_once()


def test_a_failing_unpersist_is_logged_and_does_not_replace_the_runs_error():
    a, b = MagicMock(), MagicMock()
    a.unpersist.side_effect = RuntimeError("unpersist failed")
    result, logger = _run(_Cache(a), _Cache(b), _Boom())
    assert result.status == "error"
    assert "step failed" in json.dumps(result.error_details)
    assert "unpersist failed" not in json.dumps(result.error_details)
    b.unpersist.assert_called_once()  # one failure does not stop the rest
    assert any("unpersist failed" in str(c) for c in logger.warning.call_args_list)


def test_a_failing_unpersist_does_not_turn_a_successful_run_into_an_error():
    a = MagicMock()
    a.unpersist.side_effect = RuntimeError("unpersist failed")
    result, logger = _run(_Cache(a))
    assert result.status == "success"
    logger.warning.assert_called()


def test_a_run_with_nothing_cached_is_unaffected():
    result, logger = _run()
    assert result.status == "success"
    logger.warning.assert_not_called()
