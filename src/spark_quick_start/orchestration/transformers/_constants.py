"""
_constants.py — Application-wide SCD constants for the transformers package.

Extracted so transformer modules can import constants without depending on the
wider orchestration models layer. orchestration/models.py retains its own
Constants class for backwards compatibility with non-transformer callers.

Column-name defaults are sourced from ColumnNamingPolicy.default()
(orchestration/column_policy.py) rather than hardcoded — that module is a
small standalone dataclass with no dependency on the wider models layer, so
importing it here doesn't reintroduce the coupling this file was extracted
to avoid, while keeping this class's COL_* values in sync with the single
source of truth instead of an independently-drifting copy.
"""
from __future__ import annotations

from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy

_default_policy = ColumnNamingPolicy.default()

# Internal routing columns, passed between transformers on state.df and stripped by
# NativeSCDMergeTransformer._drop_internal() before any Silver write.
#
# Deliberately NOT sourced from ColumnNamingPolicy, unlike the COL_* values below.
# A caller's naming policy renames columns that reach Silver; these never do, so
# letting a policy rename them would buy nothing and cost correctness — the merger
# locates the close-out target row by PREV_VERSION, and a renamed routing column
# yields an empty filter, not an exception (rule: data-contract).
#
# Module-level rather than attributes on Constants, which leaves this file with two
# shapes for one category of contract name — TARGET_FRAME_KEY (F2) is a class
# attribute. The reason, recorded because the inconsistency is otherwise a trap for
# the next reader: these are not policy-sourced, so sitting beside the COL_* values
# invites someone to wire them to ColumnNamingPolicy; and `import X as _Y` only
# works for module-level names, which is what lets change_detector.py and
# native_scd.py keep their existing `_CHANGE_TYPE` / `_PREV_VERSION` spellings at all
# 26 call sites in the change-classification path. TARGET_FRAME_KEY needed no alias
# because it had four. If a third contract name arrives, unify on one shape rather
# than adding a third — F2's call sites are cheap to churn, and this split is a
# consequence of that asymmetry, not a principle.
CHANGE_TYPE = "_change_type"
PREV_VERSION = "_prev_scdVersion"
INTERNAL_COLUMNS = frozenset({CHANGE_TYPE, PREV_VERSION})


class Constants:
    """Application-wide constants for SCD load processes."""

    ANSI_ENABLED: bool = False
    PARALLEL_EXECUTION_ENABLED: int = 0
    NOTEBOOK_TIMEOUT_SECONDS: int = 90

    # SCD flag values
    FLAG_CURRENT: str = "Y"
    FLAG_INACTIVE: str = "N"
    FLAG_DELETED: str = "Y"
    FLAG_NOT_DELETED: str = "N"

    # SCD column name defaults — sourced from ColumnNamingPolicy.default().
    COL_ROW_KEY: str = _default_policy.row_key_column
    COL_SOURCE_SYSTEM: str = _default_policy.source_system_column
    COL_DELETED_FLAG: str = _default_policy.deleted_flag_column
    COL_CURRENT_FLAG: str = _default_policy.current_flag_column
    COL_CORRELATION_ID: str = _default_policy.correlation_id_column
    COL_EFFECTIVE_FROM: str = _default_policy.effective_from_column
    COL_EFFECTIVE_TO: str = _default_policy.effective_to_column
    COL_VERSION: str = _default_policy.version_column
    COL_HASH: str = _default_policy.hash_column

    # The one name under which SCDTargetTableTransformer stashes the Silver frame and
    # SCDChangeDetectorTransformer / NativeSCDMergeTransformer read it back. Defined
    # here because data-contract.md requires the frames key to have exactly one
    # home: three copies of a string is not a contract, and the failure mode of a
    # mismatch is an absent frame read as a first load, not an exception.
    TARGET_FRAME_KEY: str = "target"

    DEFAULT_MAX_TIMESTAMP: str = "2999-12-31 23:59:59.999999"
    HASH_SEPARATOR: str = "~"
    # The Data Vault styles' load-audit names and hash separator (F170). Constants, not
    # ColumnNamingPolicy fields: every policy field is read as an SCD meta column, and a Data
    # Vault name there would take a same-named SCD1 business column out of the MERGE's update set.
    DV_LOAD_TS_COLUMN: str = "LoadTS"
    DV_LOAD_END_TS_COLUMN: str = "LoadEndTS"
    DV_HASH_SEPARATOR: str = "||"  # sealed on the table, so one value
    # The token a hash puts where a value is NULL: the SCD detector's default and the Data Vault
    # hash diff's, one value because both are sealed or compared across runs (F158).
    HASH_NULL_TOKEN: str = "<NULL>"
    # The Data Vault hash diff's algorithms, sha256 the default (F156, F158).
    DV_HASH_ALGORITHMS: tuple[str, ...] = ("sha256", "md5")

    # Technical columns injected by the orchestration layer (excluded from schema comparison)
    EXCLUDE_COLUMNS: tuple[str, ...] = ("NotebookName",)
