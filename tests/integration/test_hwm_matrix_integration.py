"""
Integration tests: ExecutionResult HWM propagation across the
is_dqd x append_only x (direct .add() | conditional .set_source()/.add_if()) matrix.

Matrix under test (as reported):

    is_dqd          | append_only | expected hwm_value_after
    --------------- | ----------- | -------------------------
    0                 0             SCD merge's scdEffectiveFrom
    0                 1             SCD append-only's scdEffectiveFrom
    1 (no dqd hwm)    0             SCD merge's scdEffectiveFrom   (DQD provides no hwm -> fallthrough)
    1 (no dqd hwm)    1             SCD append-only's scdEffectiveFrom
    1 (dqd hwm)       0             DQD's hwm column                (DQD registered first, wins)
    1 (dqd hwm)       1             DQD's hwm column

Each row runs twice:
  - "direct": the pipeline is built by picking one source / one SCD step in
    Python and calling .add() once each — no ConditionalTransformer/
    ConditionalSourceTransformer involved.
  - "conditional": the pipeline is built the way NB_BI_LoadEntity_Gen3.ipynb
    does — .set_source(is_dqd, DQDTransformer(), StagingTransformer()) and
    two .add_if() calls, one per branch, both registered.

Both styles must produce the identical hwm_value_after for a given row — the two
sibling tests enforce it by asserting the same expected value per row. This is the
regression guard for the bug where ConditionalSourceTransformer wasn't
unwrapped by LoadOrchestrator's HWM scan, so a source's HWM (DQF's, at the time; the
matrix moved to DQD when DQF was removed, F146) silently fell back to
the target table's scdEffectiveFrom whenever .set_source() was used.

_run_pipeline() is replaced on purpose: the unit under test is run()'s `_unwrap` plus
HWM scan over preset `hwm_*` attributes, not what transformers do. Real transformer
instances are used, with their hwm_* attributes pre-set the way a real run would leave
them.
"""
from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub 'delta' before any library import touches `from delta.tables import
# DeltaTable` inside orchestrator.run()'s lazy import — matches the pattern
# in test_load_orchestrator.py.
# ---------------------------------------------------------------------------
if "delta" not in sys.modules:
    _delta_stub = types.ModuleType("delta")
    _delta_tables_stub = types.ModuleType("delta.tables")

    class _MockDeltaTable:
        @staticmethod
        def isDeltaTable(spark, path):
            return False

        @staticmethod
        def forPath(spark, path):
            return MagicMock()

    _delta_tables_stub.DeltaTable = _MockDeltaTable
    _delta_stub.tables = _delta_tables_stub
    sys.modules["delta"] = _delta_stub
    sys.modules["delta.tables"] = _delta_tables_stub

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
from spark_quick_start.orchestration.transformers.conditional_source import (
    ConditionalSourceTransformer,
)
from spark_quick_start.orchestration.transformers.dqd import DQDTransformer
from spark_quick_start.orchestration.transformers.native_scd import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers.staging import StagingTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

_LOAD_PIPELINE = "spark_quick_start.orchestration.orchestrator"

SCD_HWM = "2024-06-01 00:00:00.000000"   # what the merge/append-only step reports
DQD_HWM = "2024-09-15 12:00:00.000000"   # what a DQD reports when the load has an hwm_context


# ============================================================================
# Transformer factories — each mimics "already ran" by setting the same
# hwm_before/hwm_after/hwm_column attributes _produce()/_run() would leave.
# ============================================================================

def _dqd_source(*, with_hwm: bool) -> DQDTransformer:
    t = DQDTransformer()
    if with_hwm:
        t.hwm_before = "2024-01-01 00:00:00.000000"
        t.hwm_after = DQD_HWM
        t.hwm_column = "last_seen_at"
    # else: left at __init__ defaults (all None) — no hwm_context
    return t


def _staging_source() -> StagingTransformer:
    return StagingTransformer()


def _merge_tx(*, ran: bool) -> NativeSCDMergeTransformer:
    t = NativeSCDMergeTransformer()
    if ran:
        t.inserted = 3
        t.updated = 1
        t.hwm_before = "2024-05-01 00:00:00.000000"
        t.hwm_after = SCD_HWM
        t.hwm_column = "scdEffectiveFrom"
    return t


def _append_only_tx(*, ran: bool) -> NativeSCDAppendOnlyTransformer:
    t = NativeSCDAppendOnlyTransformer()
    if ran:
        t.inserted = 4
        t.hwm_before = "2024-05-01 00:00:00.000000"
        t.hwm_after = SCD_HWM
        t.hwm_column = "scdEffectiveFrom"
    return t


def _build_transformers(*, is_dqd: bool, dqd_has_hwm: bool, append_only: bool, conditional: bool):
    """Assemble the transformer list for one matrix row, in one assembly style."""
    dqd = _dqd_source(with_hwm=dqd_has_hwm)
    staging = _staging_source()
    merge = _merge_tx(ran=not append_only)
    append = _append_only_tx(ran=append_only)

    if conditional:
        # Mirrors .set_source(is_dqd, DQDTransformer(), StagingTransformer()):
        # both branches registered, .selected_source set the way a real
        # _produce() call would leave it after picking one.
        source = ConditionalSourceTransformer(
            predicate=is_dqd, source_if_true=dqd, source_if_false=staging,
        )
        source.selected_source = dqd if is_dqd else staging

        # Mirrors .add_if(append_only, NativeSCDAppendOnlyTransformer())
        #        .add_if(not append_only, NativeSCDMergeTransformer())
        # — both registered; only the one whose predicate is True actually ran.
        append_step = ConditionalTransformer(predicate=append_only, transformer=append)
        merge_step = ConditionalTransformer(predicate=not append_only, transformer=merge)
        return [source, append_step, merge_step]

    # "direct": the caller already picked a branch in Python — only the
    # chosen source and the chosen SCD step are ever registered.
    source = dqd if is_dqd else staging
    chosen = append if append_only else merge
    return [source, chosen]


# ============================================================================
# Harness — runs LoadOrchestrator.run() for real, _run_pipeline replaced on purpose.
# ============================================================================

def _ctx_json() -> str:
    return json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "Person",
            "source_system": "SRC",
            "target_table_path": "/tmp/target",
            "staging_table_path": "/tmp/stage",
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Person"}]},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {
            "correlation_id": "corr-xyz",
            "pipeline_name": "PL_Person",
            "application_name": "EDW",
        },
        "logging_context": {
            "logging_level": "INFO",
            "log_file_name": "test_log",
            "log_file_dir": "/tmp/logs",
        },
    })


def _run_with(transformers):
    spark = MagicMock()
    orch = LoadOrchestrator(
        load_context_json=_ctx_json(),
        spark=spark,
        adapter=MockPlatformAdapter(),
    )
    orch._transformers = transformers
    mock_state = PipelineState(df=MagicMock())

    with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
         patch(f"{_LOAD_PIPELINE}.OrchestratorLogger") as MockLogger, \
         patch("spark_quick_start.orchestration.load_pipeline.log_runtime"), \
         patch.object(LoadOrchestrator, "_run_pipeline", return_value=mock_state):

        mock_ctx = MagicMock()
        mock_ctx.runtime_ctx.correlation_id = "corr-xyz"
        mock_ctx.global_ctx.entity_name = "Person"
        mock_ctx.global_ctx.target_table_path = "/tmp/target"
        mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
        mock_ctx.global_ctx.source_system = "SRC"
        mock_ctx.global_ctx.staging_table_schema = {}
        mock_ctx.scd_ctx.scd_type = "scd2"
        mock_ctx.scd_ctx.business_key_column = "id"
        mock_ctx.scd_ctx.load_type = "Incremental"
        mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
        mock_ctx.hwm_ctx = None
        MockCtxMgr.return_value = mock_ctx

        mock_logger_instance = MagicMock()
        mock_logger_instance.finalize.return_value = ""
        MockLogger.from_logging_context.return_value = mock_logger_instance
        MockLogger.return_value = mock_logger_instance

        return orch.run()


# ============================================================================
# The matrix
# ============================================================================

_MATRIX = [
    # (is_dqd, dqd_has_hwm, append_only, expected_hwm, row_id)
    (False, False, False, SCD_HWM, "is_dqd=0,append_only=0->merge_hwm"),
    (False, False, True,  SCD_HWM, "is_dqd=0,append_only=1->append_hwm"),
    (True,  False, False, SCD_HWM, "is_dqd=1(no_hwm),append_only=0->merge_hwm"),
    (True,  False, True,  SCD_HWM, "is_dqd=1(no_hwm),append_only=1->append_hwm"),
    (True,  True,  False, DQD_HWM, "is_dqd=1(hwm),append_only=0->dqd_hwm"),
    (True,  True,  True,  DQD_HWM, "is_dqd=1(hwm),append_only=1->dqd_hwm"),
]


@pytest.mark.parametrize(
    "is_dqd,dqd_has_hwm,append_only,expected_hwm,row_id",
    _MATRIX,
    ids=[row[-1] for row in _MATRIX],
)
def test_hwm_matrix_direct_assembly(is_dqd, dqd_has_hwm, append_only, expected_hwm, row_id):
    """Pipeline built with plain .add() — no ConditionalTransformer involved."""
    transformers = _build_transformers(
        is_dqd=is_dqd, dqd_has_hwm=dqd_has_hwm, append_only=append_only, conditional=False,
    )
    result = _run_with(transformers)

    assert result.status == "success", result.error_details
    assert result.metrics["hwm_value_after"] == expected_hwm


@pytest.mark.parametrize(
    "is_dqd,dqd_has_hwm,append_only,expected_hwm,row_id",
    _MATRIX,
    ids=[row[-1] for row in _MATRIX],
)
def test_hwm_matrix_conditional_assembly(is_dqd, dqd_has_hwm, append_only, expected_hwm, row_id):
    """Pipeline built with .set_source()/.add_if() — both branches registered,
    only one predicate true per group. Must match the direct-assembly result (same
    expected value per row as the sibling test)."""
    transformers = _build_transformers(
        is_dqd=is_dqd, dqd_has_hwm=dqd_has_hwm, append_only=append_only, conditional=True,
    )
    result = _run_with(transformers)

    assert result.status == "success", result.error_details
    assert result.metrics["hwm_value_after"] == expected_hwm
