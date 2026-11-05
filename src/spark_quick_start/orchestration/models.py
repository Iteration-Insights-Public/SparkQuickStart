"""
models.py — Pure data types for the orchestration layer.

Contains: Constants, ErrorDetails, ExecutionMetrics, ExecutionResult,
    GlobalContext, StagingContext, SCDContext, RuntimeContext, LoggingContext, HWMContext,
    DQDContext, ExtensionsContext, PlatformContext.
Does NOT contain: PySpark logic, platform-specific code, Delta operations,
    or any import from spark_load_orchestration.

Usage::

    from spark_quick_start.orchestration.models import ExecutionResult, ExecutionMetrics
    from dataclasses import asdict

    result = ExecutionResult(status="success", has_error=False,
                             metrics=asdict(ExecutionMetrics(source_count=10)))
"""
from __future__ import annotations

import copy
import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy

__all__ = [
    "Constants",
    "DQDContext",
    "ErrorDetails",
    "ExecutionMetrics",
    "ExecutionResult",
    "ExtensionsContext",
    "GlobalContext",
    "HWMContext",
    "LoggingContext",
    "PlatformContext",
    "RuntimeContext",
    "SCDContext",
    "StagingContext",
]


# ============================================================================
# CONSTANTS
# ============================================================================

_default_policy = ColumnNamingPolicy.default()


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

    # SCD column name defaults — sourced from ColumnNamingPolicy.default() so
    # this stays in sync with the single source of truth instead of drifting
    # as an independent copy.
    COL_ROW_KEY: str = _default_policy.row_key_column
    COL_SOURCE_SYSTEM: str = _default_policy.source_system_column
    COL_DELETED_FLAG: str = _default_policy.deleted_flag_column
    COL_CURRENT_FLAG: str = _default_policy.current_flag_column
    COL_CORRELATION_ID: str = _default_policy.correlation_id_column
    COL_EFFECTIVE_FROM: str = _default_policy.effective_from_column
    COL_EFFECTIVE_TO: str = _default_policy.effective_to_column
    COL_VERSION: str = _default_policy.version_column
    COL_HASH: str = _default_policy.hash_column

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


# ============================================================================
# MODELS
# ============================================================================

@dataclass
class ErrorDetails:
    """Structured error information.

    The one source of the ``error_details`` shape on ``ExecutionResult`` (F114): every
    producer builds an instance and stores ``to_dict()``. ``ExecutionResult.error_details``
    stays a plain ``dict`` so the JSON a calling pipeline receives is unchanged.
    """
    error_type: str
    error_message: str
    traceback: str | None = None
    context: dict[str, Any] | None = None

    @classmethod
    def from_exception(cls, exc: BaseException, traceback_text: str | None = None) -> ErrorDetails:
        """Describe *exc*: its class name and ``str()``, plus the formatted traceback the
        caller already holds (``traceback.format_exc()``), or ``None`` when it has none."""
        return cls(error_type=type(exc).__name__, error_message=str(exc), traceback=traceback_text)

    def to_dict(self) -> dict[str, Any]:
        """The ``error_details`` dict: ``error_type``, ``error_message`` and ``traceback``,
        plus ``context`` only when it is set. ``context`` is left out when ``None`` because
        the dicts pipelines already receive have never carried the key."""
        out: dict[str, Any] = {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "traceback": self.traceback,
        }
        if self.context is not None:
            out["context"] = self.context
        return out


@dataclass
class ExecutionMetrics:
    """Metrics captured during execution.

    source_count / target_count: row counts for the pipeline's data-acquisition
        step. On this dataclass they're always plain ``int`` (0 when not
        applicable/not computed — see below), but the two source types this
        library supports produce them very differently:

        - StagingTransformer (the normal path): both are read for free from
          the staging write's own Delta commit metadata
          (``operationMetrics['numOutputRows']``, the same mechanism
          ``NativeSCDAppendOnlyTransformer``'s raw-append WRITE path uses) —
          no extra ``.count()``, always populated, no configuration needed.
          See ``delta_utils.write_staging_data``.
        - DQDTransformer (F144): counted in the aggregation it already runs for
          the ``hwm_context`` watermark's ``max`` — no added action — and
          mirrored to ``target_count``. Without ``hwm_context`` there is no
          aggregation, nothing is counted, and both show as ``0`` here, whatever
          ``verbose`` is.

        Both come from ``SourceMetricsProvider`` — LoadOrchestrator.run()
        scans the registered source transformer for it the same way it does
        for ``HWMProvider``.
    inserted_count / updated_count: set by LoadOrchestrator.run()'s harvest from the
        ``SCDMetricsProvider`` writer that ran (its own Delta commit metrics); 0 when no
        writer ran.
    hwm_value_before / hwm_value_after: set by LoadOrchestrator.run()'s harvest from an
        ``HWMProvider`` (see its docstring for which one wins), so their meaning follows
        the step that reported:

        - With ``hwm_context``: the source watermark on ``hwm_column_name`` —
          before is the incoming ``hwm_value``, after the max over the rows staged or
          read (before again when there were none).
        - Otherwise, an SCD writer: max effective-from in the target read immediately
          before and after the write. None if not read (first load before the table
          exists, or a read that failed). "" means the table was empty.
        - Otherwise, a Data Vault writer: this run's ``LoadTS`` as after, no before.
        - No provider at all: both read once from the target table after the run.
    """
    source_count: int = 0
    target_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    duration_seconds: float = 0.0
    start_time: str = ""
    end_time: str = ""
    hwm_value_before: str | None = None
    hwm_value_after: str | None = None


@dataclass
class ExecutionResult:
    """Standardized result returned by staging notebooks via notebook.exit()."""
    status: str          # "success" | "error" | "warning"
    has_error: bool
    error_details: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None
    logs: str = ""
    correlation_id: str = ""
    early_exit: bool = False
    early_exit_reason: str = ""
    # Per-transformer StageMetrics captured during run(). After a JSON round-trip
    # via to_json()/from_json(), each entry is a plain dict rather than a
    # StageMetrics instance — from_json() does not reconstruct nested dataclasses.
    stages: list[Any] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> ExecutionResult:
        data = json.loads(json_str)
        known = {k for k in data if k in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: data[k] for k in known})

    def _repr_markdown_(self) -> str:
        """Render a human-friendly Markdown summary for Fabric/Databricks notebooks."""
        status_icon = {"success": "✅", "error": "❌", "warning": "⚠️"}.get(self.status, "ℹ️")
        lines = [
            f"## {status_icon} ExecutionResult — `{self.status}`",
            "",
        ]

        if self.early_exit:
            lines += [f"> **Early exit:** {self.early_exit_reason}", ""]

        if self.error_details:
            lines += [f"> **Error:** {self.error_details}", ""]

        if self.metrics:
            m = self.metrics
            lines += [
                "### Metrics",
                "| metric | value |",
                "| ------ | ----- |",
            ]
            key_order = [
                "source_count", "target_count", "inserted_count", "updated_count",
                "duration_seconds", "start_time", "end_time",
                "hwm_value_before", "hwm_value_after",
            ]
            shown = {k: m[k] for k in key_order if k in m}
            shown.update({k: v for k, v in m.items() if k not in shown})
            for k, v in shown.items():
                lines.append(f"| `{k}` | {v} |")
            lines.append("")

        if self.logs:
            try:
                log_data = json.loads(self.logs)
            except (json.JSONDecodeError, TypeError):
                log_data = None

            if log_data:
                lines += ["### Logs", ""]
                for k in ("run_id", "base_name", "success"):
                    if k in log_data:
                        lines.append(f"- **{k}:** `{log_data[k]}`")
                if "stderr_tail" in log_data:
                    lines += [
                        "",
                        "<details><summary>stderr tail</summary>",
                        "",
                        "```",
                        log_data["stderr_tail"].strip(),
                        "```",
                        "</details>",
                    ]
                if "stdout_tail" in log_data:
                    lines += [
                        "",
                        "<details><summary>stdout tail</summary>",
                        "",
                        "```",
                        log_data["stdout_tail"].strip(),
                        "```",
                        "</details>",
                    ]

        if self.correlation_id:
            lines += ["", f"*correlation_id: `{self.correlation_id}`*"]

        return "\n".join(lines)

    def __repr__(self) -> str:
        m = self.metrics or {}
        early = f", early_exit={self.early_exit_reason!r}" if self.early_exit else ""
        return (
            f"ExecutionResult(status={self.status!r}, "
            f"inserted={m.get('inserted_count', '?')}, "
            f"updated={m.get('updated_count', '?')}, "
            f"source={m.get('source_count', '?')}, "
            f"duration={m.get('duration_seconds', '?')}s"
            f"{early}, "
            f"correlation_id={self.correlation_id!r})"
        )


# ============================================================================
# CONTEXT CLASSES
# ============================================================================

_ID_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_DATE_ONLY = re.compile(r"^\d{4}-?\d{2}-?\d{2}$")


def _parse_utc_instant(raw: Any, field: str, *, naive_is_utc: bool) -> datetime:
    """Read a context value as one instant, in UTC (F106). Pure; no clock, no Spark.

    Accepts what ``datetime.fromisoformat`` reads on Python 3.11: ``Z`` or a numeric offset, a
    ``T`` or a space between date and time, and up to any number of fractional digits (the
    seven ADF and Fabric ``utcNow()`` writes are cut to microseconds).

    Args:
        raw: the value as given in the load context.
        field: the dotted name quoted in the error, e.g. ``global_context.load_timestamp``.
        naive_is_utc: read a value with no offset as UTC (``default_max_timestamp``, whose shipped
            default is naive); otherwise refuse it, so a load time never depends on the session's
            time zone.

    Raises:
        ValueError: naming the field and value, for a non-string, a date with no time, an
            unreadable value, or (unless *naive_is_utc*) a value with no offset.
    """
    if not isinstance(raw, str):
        raise ValueError(
            f"{field} is {raw!r} ({type(raw).__name__}); it must be an ISO 8601 string, "
            "e.g. '2026-10-03T12:00:00.000000Z'."
        )
    text = raw.strip()
    if _DATE_ONLY.fullmatch(text):
        raise ValueError(
            f"{field} is {raw!r}, a date with no time; give an instant, "
            "e.g. '2026-10-03T00:00:00Z'."
        )
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(
            f"{field} is {raw!r}, which is not an ISO 8601 date and time, "
            "e.g. '2026-10-03T12:00:00.000000Z'."
        ) from None
    if parsed.tzinfo is None:
        if not naive_is_utc:
            raise ValueError(
                f"{field} is {raw!r}, which has no UTC offset, so it names a different instant "
                "in each time zone. Add Z or +00:00, e.g. "
                f"{text.replace(' ', 'T')!r}+00:00 if it is UTC."
            )
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class GlobalContext:
    """Business domain parameters — what entity to load and where.

    ``id`` identifies the load: required, non-empty, letters, digits and underscores only, and
    unique, ignoring case, among load contexts that share a staging_table_path; two concurrent runs of one context still share scratch tables (F129). It is part of every scratch table's name
    (``{staging_table_path}_{id}_{stage_index}_{name}``), lower-cased there, because it becomes
    a Delta path or a Unity Catalog name, which UC and Hive compare ignoring case.

    Note: ``parallel_execution_enabled`` moved to ``RuntimeContext`` in v0.3.
    Supplying it here raises ``ValueError`` with a migration message.

    Any other key outside ``_RESERVED`` raises ``ValueError`` too, naming the key, the accepted
    keys and ``extensions_context``, where the user's own data goes. A misspelt key is therefore
    an error, not a value the load silently never reads. Only this section is strict.
    """

    _RESERVED: frozenset = frozenset({
        "id", "entity_name", "source_system", "target_table_path", "staging_table_path",
        "staging_table_schema",
        "load_timestamp", "notebook_timeout_seconds",
    })

    def __init__(self, data: dict[str, Any]) -> None:
        if "parallel_execution_enabled" in data:
            raise ValueError(
                "GlobalContext: 'parallel_execution_enabled' has moved to runtime_context. "
                "Update your load_context JSON to place this field under 'runtime_context'."
            )
        unknown = sorted(k for k in data if k not in self._RESERVED)
        if unknown:
            raise ValueError(
                f"global_context: unknown key(s) {', '.join(repr(k) for k in unknown)}; it accepts "
                f"{', '.join(sorted(self._RESERVED))}. Put your own data in extensions_context; "
                "the lakehouse paths are set by the library in platform_context."
            )
        self.id: str = data.get("id", "")
        self.entity_name: str = data.get("entity_name", "")
        self.source_system: str | None = data.get("source_system")
        self.target_table_path: str | None = data.get("target_table_path")
        self.staging_table_path: str | None = data.get("staging_table_path")
        self.staging_table_schema: dict[str, Any] = data.get("staging_table_schema", {})
        self.notebook_timeout_seconds: int = int(
            data.get("notebook_timeout_seconds", Constants.NOTEBOOK_TIMEOUT_SECONDS)
        )
        self.load_timestamp: str = data.get(
            "load_timestamp",
            datetime.now(UTC).isoformat(timespec="microseconds"),
        )

    def validate(self) -> None:
        required = {
            "id": self.id,
            "entity_name": self.entity_name,
            "source_system": self.source_system,
            "target_table_path": self.target_table_path,
            "staging_table_path": self.staging_table_path,
        }
        missing = [k for k, v in required.items() if not v or (isinstance(v, str) and not v.strip())]
        if missing:
            raise ValueError(f"Missing required global_context parameters: {', '.join(missing)}")
        _ = self.load_timestamp_utc  # refuses an unreadable value here, before any Spark work (F106)
        if not isinstance(self.id, str) or not _ID_PATTERN.fullmatch(self.id):
            raise ValueError(
                f"global_context.id is {self.id!r}; it must be a string of letters, digits and "
                "underscores only (^[A-Za-z0-9_]+$), because it becomes part of the scratch "
                "tables' Delta path or Unity Catalog name"
            )

    @property
    def load_timestamp_utc(self) -> datetime:
        """The run's load time T as a timezone-aware UTC ``datetime`` (F106).

        ``load_timestamp`` is the string given, or the instant the context was built, with
        microseconds. It must carry an offset (``Z`` or ``+00:00``): a naive value would mean
        a different instant in each session time zone.

        Raises:
            ValueError: if the value is not a string, is a date with no time, is not ISO 8601,
                or has no offset.
        """
        return _parse_utc_instant(self.load_timestamp, "global_context.load_timestamp", naive_is_utc=False)


class ExtensionsContext:
    """The user's own data in the load context: policies, rules and parameters for custom
    transformers and pipeline control. The library never interprets it.

    Built from the ``extensions_context`` section, a JSON object whose values are any JSON. It
    reads like a read-only mapping over a deep copy, so changing the parsed JSON afterwards does
    not reach it and an assignment to it raises ``TypeError``. The protection is the top level's:
    an object nested inside a value is the plain ``dict`` that was parsed, and a step must not
    change it.

    By convention, not enforced, a step namespaces its top-level keys, such as
    ``"acme.quality"``, so two teams' steps do not collide.
    """

    __slots__ = ("_data",)

    def __init__(self, data: Mapping[str, Any] | None = None) -> None:
        self._data: Mapping[str, Any] = MappingProxyType(copy.deepcopy(dict(data or {})))

    @classmethod
    def from_raw(cls, raw: Any) -> ExtensionsContext:
        """Build from the parsed section, refusing anything that is not a JSON object.

        Raises:
            ValueError: if *raw* is not a ``dict``.
        """
        if not isinstance(raw, dict):
            raise ValueError(
                f"extensions_context must be a JSON object (dict), got {type(raw).__name__}"
            )
        return cls(raw)

    def get(self, key: str, default: Any = None) -> Any:
        """The value for *key*, or *default* when the section has none."""
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def to_dict(self) -> dict[str, Any]:
        """A deep copy of the section, as a staging notebook receives it."""
        return copy.deepcopy(dict(self._data))

    def __reduce__(self) -> tuple[Any, ...]:
        # A MappingProxyType cannot be pickled or deep-copied; rebuild from the data instead.
        return (ExtensionsContext, (self.to_dict(),))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ExtensionsContext) and dict(self._data) == dict(other._data)

    __hash__ = None  # type: ignore[assignment]  # holds mutable data, like a dict

    def __repr__(self) -> str:
        return f"ExtensionsContext({dict(self._data)!r})"


@dataclass(frozen=True)
class PlatformContext:
    """What the library knows about where it runs, filled by it and never by the user.

    A load context JSON that carries ``platform_context`` is refused. Staging notebooks receive
    it as their own ``platform_context`` section.

    Attributes:
        data_storage_lakehouse_path: The data storage lakehouse path the caller gave
            ``LoadOrchestrator``, after which ``{DataStorageLakehouse}`` is substituted.
        data_engineering_lakehouse_path: The data engineering lakehouse path, likewise.
    """

    data_storage_lakehouse_path: str = ""
    data_engineering_lakehouse_path: str = ""

    def to_dict(self) -> dict[str, str]:
        """Both paths, as a staging notebook receives them."""
        return asdict(self)


class StagingContext:
    """Multi-phase staging orchestration — list of notebooks to execute."""

    def __init__(self, data: dict[str, Any]) -> None:
        self.stages: list[dict[str, Any]] = data.get("stages", [])

    def validate(self, *, has_dqd: bool = False) -> None:
        if not self.stages:
            if not has_dqd:
                raise ValueError(
                    "staging_context.stages must contain at least one staging phase. "
                    "If the source is a DQD, supply a dqd_context."
                )
            return
        for idx, stage in enumerate(self.stages):
            if not stage.get("nb_name"):
                raise ValueError(f"staging_context.stages[{idx}] missing required 'nb_name'")
            path_override = stage.get("path")
            if path_override is not None and not str(path_override).strip():
                raise ValueError(
                    f"staging_context.stages[{idx}] 'path' is present but blank. "
                    "Provide a valid path suffix or remove the key entirely."
                )


_VALID_SCD_TYPES = frozenset({"scd1", "scd2", "dv_hub", "dv_satellite", "dv_link"})
# The Data Vault styles (F156; the accepted design of 2026-10-01); dv_link since F167.
_DATA_VAULT_TYPES = frozenset({"dv_hub", "dv_satellite", "dv_link"})
_HASH_ALGORITHMS = Constants.DV_HASH_ALGORITHMS
# Keys that belong to one style; set under the other, each is refused rather than ignored.
_SCD_ONLY_KEYS = ("version_column", "current_flag_column", "effective_from_column",
                  "effective_to_column", "scd_row_hashing", "current_flag_value",
                  "inactive_flag_value", "default_max_timestamp")
_DATA_VAULT_ONLY_KEYS = ("hash_algorithm", "load_ts_column", "load_end_ts_column",
                         "pipeline_run_id_column")
# Keys of one Data Vault style, refused under every other (F167, the link design of 2026-10-01).
_DV_LINK_ONLY_KEYS = ("link_hub_key_columns",)
_VALID_LOAD_TYPES = frozenset({"Full", "Incremental"})


class SCDContext:
    """SCD column name overrides and load behaviour.

    Meta column names resolve in order: per-column JSON key > ``policy`` > the style's policy,
    ``ColumnNamingPolicy.default()`` for ``scd1``/``scd2`` and ``ColumnNamingPolicy.data_vault()``
    for the Data Vault styles (F156). Non-naming fields (flags, timestamps, load_type, scd_type)
    continue to default to ``Constants``; under a Data Vault style ``hash_separator`` defaults to
    ``||``.
    """

    def __init__(self, data: dict[str, Any], policy: ColumnNamingPolicy | None = None) -> None:
        # A key counts as given only with a value: a template writing `null` sets nothing (F156).
        self._given_keys: frozenset[str] = frozenset(k for k, v in data.items() if v is not None)
        data_vault = data.get("scd_type") in _DATA_VAULT_TYPES
        _pol = policy or (ColumnNamingPolicy.data_vault() if data_vault else ColumnNamingPolicy.default())

        bk = data.get("business_key_column", "")
        if not bk or not bk.strip():
            raise ValueError("SCDContext requires 'business_key_column'")
        self.business_key_column: str = bk

        # Meta column names — resolution order: per-column JSON override > policy > default()
        self.row_key_column: str = data.get("row_key_column", _pol.row_key_column)
        self.hash_column: str = data.get("hash_column", _pol.hash_column)
        self.version_column: str = data.get("version_column", _pol.version_column)
        self.effective_from_column: str = data.get("effective_from_column", _pol.effective_from_column)
        self.effective_to_column: str = data.get("effective_to_column", _pol.effective_to_column)
        self.current_flag_column: str = data.get("current_flag_column", _pol.current_flag_column)
        self.deleted_flag_column: str = data.get("deleted_flag_column", _pol.deleted_flag_column)
        self.source_system_column: str = data.get("source_system_column", _pol.source_system_column)
        self.correlation_id_column: str = data.get("correlation_id_column", _pol.correlation_id_column)

        # Non-naming attrs (unchanged)
        # `null` is "not given" here, as for every key of this class; an empty string is kept, for validate().
        _dmt = data.get("default_max_timestamp")
        self.default_max_timestamp: str = Constants.DEFAULT_MAX_TIMESTAMP if _dmt is None else _dmt
        # Two vocabularies (F106). Written, and compared with the target's flag: the current-row
        # pair and the deleted pair. Read from the source: `source_deleted_flag_value`, what the
        # delete signal is compared with, which is the resolved `deleted_flag_value` unless given.
        # `null` is "not given", as for every key of this class; anything else is kept for validate().
        def _flag(key: str, default: str) -> Any:
            value = data.get(key)
            return default if value is None else value

        self.current_flag_value: str = _flag("current_flag_value", Constants.FLAG_CURRENT)
        self.inactive_flag_value: str = _flag("inactive_flag_value", Constants.FLAG_INACTIVE)
        self.deleted_flag_value: str = _flag("deleted_flag_value", Constants.FLAG_DELETED)
        self.not_deleted_flag_value: str = _flag("not_deleted_flag_value", Constants.FLAG_NOT_DELETED)
        self.source_deleted_flag_value: str = _flag("source_deleted_flag_value", self.deleted_flag_value)
        _sep = data.get("hash_separator")  # `null` is "not given" here too (F156)
        self.hash_separator: str = (Constants.DV_HASH_SEPARATOR if data_vault else Constants.HASH_SEPARATOR) if _sep is None else _sep
        self.load_type: str = data.get("load_type", "Incremental")
        self.scd_type: str = data.get("scd_type", "scd2")
        self.source_system_from_source: bool = bool(data.get("source_system_from_source", False))
        self.source_deleted_flag_column: str | None = data.get("source_deleted_flag_column") or None
        self.exclude_from_hash: list[str] = list(data.get("exclude_from_hash") or [])
        self.scd_row_hashing: bool = bool(data.get("scd_row_hashing", False))

        # Data Vault only (F156): the hash diff's algorithm and the load-audit columns.
        # `null` means "not given" and takes the default; an empty string is kept, for validate().
        def _given(key: str, default: str) -> str:
            value = data.get(key)
            return default if value is None else value

        self.hash_algorithm: str = _given("hash_algorithm", "sha256")
        self.load_ts_column: str = _given("load_ts_column", Constants.DV_LOAD_TS_COLUMN)
        self.load_end_ts_column: str = _given("load_end_ts_column", Constants.DV_LOAD_END_TS_COLUMN)
        self.pipeline_run_id_column: str | None = data.get("pipeline_run_id_column") or None
        # dv_link only (F167): the staged columns holding each connected hub's hash key.
        # Kept as given, not passed through list(): a string would split into characters.
        self.link_hub_key_columns: list[str] = data.get("link_hub_key_columns") or []

    @property
    def is_data_vault(self) -> bool:
        """True under `dv_hub`, `dv_satellite` or `dv_link` (F156)."""
        return self.scd_type in _DATA_VAULT_TYPES

    def was_given(self, key: str) -> bool:
        """Whether the load context gave *key* a value; `null` and an absent key are both "not given"."""
        return key in self._given_keys

    @property
    def default_max_timestamp_utc(self) -> datetime:
        """``default_max_timestamp`` as a timezone-aware UTC ``datetime`` (F106).

        A value with no offset is read as UTC: the shipped default, ``2999-12-31 23:59:59.999999``,
        has none.

        Raises:
            ValueError: if the value is not a string, is a date with no time, or is not ISO 8601.
        """
        return _parse_utc_instant(
            self.default_max_timestamp, "scd_context.default_max_timestamp", naive_is_utc=True)

    def require_data_vault_style(self, step: str, style: str) -> None:
        """Refuse any style but *style* in a Data Vault step, before it reads anything (F159).

        Raises:
            ValueError: naming the step, the style it is for, and the load's.
        """
        if self.scd_type != style:
            raise ValueError(
                f"{step}: scd_context.scd_type is {self.scd_type!r}; this step is for {style!r} "
                "loads only. Compose the steps of the load's own style."
            )

    def require_scd_style(self, step: str) -> None:
        """Refuse a Data Vault style in a step built for SCD, before it reads anything (F156).

        Raises:
            ValueError: naming the step, the style and what to use instead.
        """
        if self.is_data_vault:
            raise ValueError(
                f"{step}: scd_context.scd_type is {self.scd_type!r}, a Data Vault style; this step "
                "is for the SCD styles (scd1, scd2). Compose the Data Vault steps for this load."
            )


    def validate(self) -> None:
        """Reject an unusable SCD configuration before any Spark work starts.

        Every other validated context class (global, staging, runtime, logging, and hwm and
        dqd when present) owns a validate(), and validate_all() calls all of them. This one owned none: a missing business_key_column raised in the
        constructor, and scd_type and load_type were checked inside
        SCDChangeDetectorTransformer._run() — which runs after every staging notebook has
        executed and written Delta tables. So a typo'd scd_type cost a full staging run
        before anyone heard about it, against configuration-and-context.md's "validation
        happens once, up front, before any Spark work" (F4).

        Each message names the offending value, not just the field, per errors-and-logging:
        the caller is looking at a JSON document written by a pipeline outside this repo and
        needs to find the token.

        Raises:
            ValueError: if scd_type or load_type is not one of its permitted values, if
                the row key differs from the business key only by case, if scd_row_hashing
                is set while the row key is the business key, or if a meta
                column name other than the row key collides with the business key or source
                system column, or the row key with the source system column.
        """
        if self.scd_type not in _VALID_SCD_TYPES:
            raise ValueError(
                f"scd_context.scd_type is {self.scd_type!r}; must be one of "
                f"{sorted(_VALID_SCD_TYPES)}. Note the comparison is case-sensitive."
            )
        if self.load_type not in _VALID_LOAD_TYPES:
            raise ValueError(
                f"scd_context.load_type is {self.load_type!r}; must be one of "
                f"{sorted(_VALID_LOAD_TYPES)}. Note the comparison is case-sensitive."
            )
        self._validate_flag_values()
        self._validate_hash_separator()
        # A key of the other style is refused rather than ignored (F156): a setting that has no
        # effect reads as one that does.
        if self.is_data_vault:
            foreign = [k for k in _SCD_ONLY_KEYS if k in self._given_keys]
            if foreign:
                raise ValueError(
                    f"scd_context.{foreign[0]} is an SCD key, but scd_type is {self.scd_type!r}, a "
                    f"Data Vault style, which does not use it; remove {foreign}."
                )
            blank = [k for k in ("load_ts_column", "load_end_ts_column")
                     if not isinstance(getattr(self, k), str) or not getattr(self, k).strip()]
            if blank:
                raise ValueError(
                    f"scd_context.{blank[0]} is empty; a Data Vault style writes it, so name the "
                    "column or leave the key out for its default."
                )
            if self.hash_algorithm not in _HASH_ALGORITHMS:
                raise ValueError(
                    f"scd_context.hash_algorithm {self.hash_algorithm!r} is not one of "
                    f"{list(_HASH_ALGORITHMS)}."
                )
            if self.scd_type == "dv_link":
                self._validate_link_hub_key_columns()
        else:
            # The Data Vault styles refuse the key above, so only these read it (F106).
            _ = self.default_max_timestamp_utc  # refuses an unreadable value before any Spark work
            foreign = [k for k in _DATA_VAULT_ONLY_KEYS if k in self._given_keys]
            if foreign:
                raise ValueError(
                    f"scd_context.{foreign[0]} is a Data Vault key, but scd_type is "
                    f"{self.scd_type!r}, an SCD style, which does not use it; remove {foreign}."
                )

        if self.scd_type != "dv_link":
            foreign = [k for k in _DV_LINK_ONLY_KEYS if k in self._given_keys]
            if foreign:
                raise ValueError(
                    f"scd_context.{foreign[0]} is a dv_link key, but scd_type is {self.scd_type!r}, "
                    f"which does not use it; remove {foreign}."
                )

        # The row key may be the business key itself (F148, case 1): the staged key is the
        # target's row key under one name, and the detector passes it through as staged, so
        # scd_row_hashing would have no effect and is refused rather than ignored. A spelling
        # that differs only by case is refused too: whether Spark reads it as the same column
        # depends on the session's spark.sql.caseSensitive, which validation cannot see, and
        # under the default it would be the key, rewritten before the target is matched on it.
        if self.row_key_column != self.business_key_column \
                and self.row_key_column.lower() == self.business_key_column.lower():
            raise ValueError(
                f"scd_context.row_key_column {self.row_key_column!r} differs from "
                f"business_key_column {self.business_key_column!r} only by case. Spell them "
                "the same to make the business key the row key, or give the row key a name of "
                "its own."
            )
        if self.row_key_column == self.business_key_column and self.scd_row_hashing:
            raise ValueError(
                f"scd_context.scd_row_hashing is true, but row_key_column is the business_key_column "
                f"{self.business_key_column!r}, which passes through as staged, so the hashing would "
                "have no effect. Hash the key where it is built, or give the row key its own name."
            )

        # Any other meta column that collides with a business column silently overwrites it —
        # the hash excludes meta names, so the collision removes a business column from
        # the hash and every change to it becomes invisible.
        meta = {
            "row_key_column": self.row_key_column,
            "hash_column": self.hash_column,
            "version_column": self.version_column,
            "current_flag_column": self.current_flag_column,
            "deleted_flag_column": self.deleted_flag_column,
            "effective_from_column": self.effective_from_column,
            "effective_to_column": self.effective_to_column,
            "correlation_id_column": self.correlation_id_column,
        }
        if self.is_data_vault:
            meta |= {"load_ts_column": self.load_ts_column, "load_end_ts_column": self.load_end_ts_column}
            if self.pipeline_run_id_column:
                meta["pipeline_run_id_column"] = self.pipeline_run_id_column
        reserved = {self.business_key_column, self.source_system_column}
        collisions = sorted(
            f"{field}={value!r}" for field, value in meta.items()
            if value in reserved
            and not (field == "row_key_column" and value == self.business_key_column)
        )
        if collisions:
            raise ValueError(
                f"scd_context meta column name(s) collide with the business key or source "
                f"system column {sorted(reserved)!r}: {collisions}. A meta column that "
                "shares a name is excluded from the hash, so every change to the business "
                "column it shadows would be invisible."
            )

    def _validate_hash_separator(self) -> None:
        """The hash separator, under every style (F106): a non-empty string without a quote,
        backslash, backtick, `$` or control character.

        A Data Vault table seals it as a table property, which may not hold those characters
        (F157, F158), and refusing it here is before any staging runs, rather than at the seal
        after the first write. An SCD style hashes with it too, since F106, so the one rule holds
        for both: an empty separator makes ("a", "bc") and ("ab", "c") hash alike.

        Raises:
            ValueError: naming the value.
        """
        value = self.hash_separator
        if not isinstance(value, str) or not value or any(
                ch in "'\\`$" or ord(ch) < 32 or ord(ch) == 127 for ch in value):
            where = "sealed on a Data Vault table" if self.is_data_vault else "used"
            raise ValueError(
                f"scd_context.hash_separator {value!r} cannot be {where}: it must be a non-empty "
                "string with no quote, backslash, backtick, `$` or control character."
            )

    def _validate_flag_values(self) -> None:
        """The five flag values (F106): non-empty strings the merge SQL can carry, and distinct pairs.

        The current, inactive and deleted values are written into the SQL of the merge as
        quoted literals (load_pipeline.py), so a quote, backslash, backtick, `$` or control
        character is refused as `hash_separator` is for the seal (F157): such a value would break
        the statement or, with `$`, be substituted by Spark inside a quoted string.

        Raises:
            ValueError: naming the key and the value, or the pair that is equal.
        """
        values = {
            "current_flag_value": self.current_flag_value,
            "inactive_flag_value": self.inactive_flag_value,
            "deleted_flag_value": self.deleted_flag_value,
            "not_deleted_flag_value": self.not_deleted_flag_value,
            "source_deleted_flag_value": self.source_deleted_flag_value,
        }
        for key, value in values.items():
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"scd_context.{key} is {value!r}; it must be a non-empty string."
                )
            if any(ch in "'\\`$" or ord(ch) < 32 or ord(ch) == 127 for ch in value):
                raise ValueError(
                    f"scd_context.{key} {value!r} may not contain a quote, backslash, backtick, "
                    "`$` or control character: it is written into the merge statement."
                )
        for first, second in (("current_flag_value", "inactive_flag_value"),
                              ("deleted_flag_value", "not_deleted_flag_value")):
            if values[first] == values[second]:
                raise ValueError(
                    f"scd_context.{first} and scd_context.{second} are both {values[first]!r}; "
                    "the two must differ, or the flag could not tell its states apart."
                )

    def _validate_link_hub_key_columns(self) -> None:
        """A link's hub key columns (F167): two or more, distinct, none the key or a load column.

        Names are compared case-insensitively, as Spark and Delta match them under the default
        `spark.sql.caseSensitive`: two spellings of one name would be one column.
        """
        columns = self.link_hub_key_columns
        if not isinstance(columns, list):
            raise ValueError(
                "scd_context.link_hub_key_columns must be a list of column names; it is "
                f"{type(columns).__name__} {columns!r}."
            )
        if len(columns) < 2:
            raise ValueError(
                "scd_context.link_hub_key_columns must name at least two staged columns, the hash "
                f"key of each hub the link connects; it has {columns!r}."
            )
        taken = {c.lower(): f for f, c in (
            ("business_key_column", self.business_key_column),
            ("source_system_column", self.source_system_column),
            ("correlation_id_column", self.correlation_id_column),
            ("load_ts_column", self.load_ts_column),
            ("load_end_ts_column", self.load_end_ts_column),
            ("pipeline_run_id_column", self.pipeline_run_id_column),
        ) if isinstance(c, str) and c}
        seen: set[str] = set()
        for column in columns:
            if not isinstance(column, str) or not column.strip():
                raise ValueError(
                    f"scd_context.link_hub_key_columns holds {column!r}, which is not a column name."
                )
            if column.lower() in taken:
                raise ValueError(
                    f"scd_context.link_hub_key_columns holds {column!r}, which is the "
                    f"{taken[column.lower()]}; a hub key needs a column of its own."
                )
            if column.lower() in seen:
                raise ValueError(
                    f"scd_context.link_hub_key_columns holds {column!r} twice (names are matched "
                    "case-insensitively)."
                )
            seen.add(column.lower())


# Keys RuntimeContext once parsed and nothing read, so a setting had no effect and no message
# (F198, F106). Each is refused with its reason, as GlobalContext refuses its moved key.
_REMOVED_RUNTIME_KEYS: dict[str, str] = {
    "num_partitions": "the library never repartitioned by it, so it had no effect.",
    "scd_append_only": "it never selected a load style; an append-only load is built by hand with "
                       "NativeSCDAppendOnlyTransformer (docs/guides/choose-a-load-style.md).",
}


_MATERIALIZE_STRATEGIES: tuple[str, ...] = ("delta", "cache")


class RuntimeContext:
    """Pipeline execution metadata.

    Execution-concurrency fields: ``parallel_execution_enabled`` (toggle) and
    ``max_parallel_staging`` (degree). Both live here so parallelism behavior
    is configured in one place.
    """

    def __init__(self, data: dict[str, Any]) -> None:
        # `in`, not `.get`: a removed key is refused whatever its value, `null` included, and the
        # documentation check's recorder counts only reads (F198, F106).
        for removed, why in _REMOVED_RUNTIME_KEYS.items():
            if removed in data:
                raise ValueError(
                    f"runtime_context.{removed} was removed: {why} Remove it from your "
                    "load_context JSON."
                )
        self.correlation_id: str | None = data.get("correlation_id")
        # The calling platform's run id, written to a Data Vault table when it names a
        # pipeline_run_id_column (F156); unused otherwise.
        self.pipeline_run_id: str | None = data.get("pipeline_run_id") or None
        self.pipeline_name: str | None = data.get("pipeline_name")
        self.application_name: str | None = data.get("application_name")
        self.max_parallel_staging: int | None = data.get("max_parallel_staging")
        self.serverless: bool = data.get("serverless", False)
        self.capture_partitions: bool = bool(data.get("capture_partitions", True))
        self.capture_row_counts: bool = bool(data.get("capture_row_counts", False))
        self.capture_plans: bool = bool(data.get("capture_plans", False))
        # Kept as given (not coerced) so validate() can refuse a wrong value by name.
        self.materialize_strategy: str = data.get("materialize_strategy", "delta")
        self.parallel_execution_enabled: bool = (
            data.get("parallel_execution_enabled", Constants.PARALLEL_EXECUTION_ENABLED) == 1
        )

    def validate(self) -> None:
        required = {
            "correlation_id": self.correlation_id,
            "pipeline_name": self.pipeline_name,
            "application_name": self.application_name,
        }
        missing = [k for k, v in required.items() if not v or (isinstance(v, str) and not v.strip())]
        if missing:
            raise ValueError(f"Missing required runtime_context parameters: {', '.join(missing)}")
        if (
            not isinstance(self.materialize_strategy, str)
            or self.materialize_strategy not in _MATERIALIZE_STRATEGIES
        ):
            raise ValueError(
                f"runtime_context.materialize_strategy must be 'delta' or 'cache'; "
                f"got {self.materialize_strategy!r}."
            )


class LoggingContext:
    """Log output configuration.

    Attributes:
        logging_level: Python log level string (e.g. ``"INFO"``).
        log_file_name: Base name for the output log file.
        log_file_dir: Destination directory (OneLake path or local path).
        verbose: When ``True``, all log messages are emitted. When ``False``
            (default), the orchestrator suppresses low-signal diagnostic
            messages and only emits always-on events (pipeline start/end,
            merge stats, errors).
        suppress_stdout_tail: When ``True``, the captured stdout of the run is
            omitted from the persisted log payload. Everything printed during a
            run is otherwise recorded in the log file's ``stdout_tail`` — which
            includes real row data whenever something prints it, e.g.
            ``DebugTransformer``. Logs are typically written to a lakehouse
            volume whose read permissions differ from the source table's, so a
            pipeline over sensitive data should set this. Defaults to ``False``,
            preserving the stdout trail that makes a run diagnosable.
    """

    def __init__(self, data: dict[str, Any]) -> None:
        self.logging_level: str | None = data.get("logging_level")
        self.log_file_name: str | None = data.get("log_file_name")
        self.log_file_dir: str | None = data.get("log_file_dir")
        self.verbose: bool = bool(data.get("verbose", False))
        self.suppress_stdout_tail: bool = bool(data.get("suppress_stdout_tail", False))

    def validate(self) -> None:
        required = {
            "logging_level": self.logging_level,
            "log_file_name": self.log_file_name,
            "log_file_dir": self.log_file_dir,
        }
        missing = [k for k, v in required.items() if not v or (isinstance(v, str) and not v.strip())]
        if missing:
            raise ValueError(f"Missing required logging_context parameters: {', '.join(missing)}")


class HWMContext:
    """The load's high-water mark: the source column that grows with every change, and the
    value the previous run reached (F138). Read by staging-notebook and DQD loads.

    Two keys for now, by the owner's decision of 2026-09-29; any other is refused, so an old
    DQF-style `hwmColumnName` fails loudly instead of being dropped.

    Attributes:
        hwm_column_name: The source column the watermark is taken from.
        hwm_value: The previous run's watermark, or ``None`` on the first run. A string, such
            as a timestamp formatted by Spark, or a number.
    """

    _KEYS = frozenset({"hwm_column_name", "hwm_value"})

    def __init__(self, data: dict[str, Any]) -> None:
        self._keys = sorted(data)
        self.hwm_column_name: Any = data.get("hwm_column_name")
        self.hwm_value: Any = data.get("hwm_value")

    def validate(self) -> None:
        unknown = [k for k in self._keys if k not in self._KEYS]
        if unknown:
            raise ValueError(f"hwm_context: unknown key '{unknown[0]}'; its keys are {sorted(self._KEYS)}")
        if not isinstance(self.hwm_column_name, str) or not self.hwm_column_name.strip():
            raise ValueError("hwm_context: hwm_column_name is required and must be a non-empty string")
        if self.hwm_value is not None and (
                isinstance(self.hwm_value, bool) or not isinstance(self.hwm_value, (str, int, float))):
            raise ValueError(
                f"hwm_context: hwm_value must be a string or a number, not {type(self.hwm_value).__name__}")

    def to_dict(self) -> dict[str, Any]:
        """Both keys, as a staging notebook receives them."""
        return {"hwm_column_name": self.hwm_column_name, "hwm_value": self.hwm_value}


class DQDContext:
    """A Declarative Query Definition: one source, row by row, into the load (F139).

    Only the shape is checked here, before any Spark work; the expressions inside `filters`,
    `transformations` and `selection` are parsed by `DQDTransformer`, through the DQD library,
    which this pure-data module does not import (rule: layering §7). The design is
    docs/architecture/decisions/2026-09-29-declarative-query-definition-design.md.

    Attributes:
        source_kind: ``delta``, ``parquet``, ``view`` (a catalog view) or ``warehouse_view`` (a
            Fabric Warehouse or SQL-endpoint view, F140).
        source_identifier: A catalog name, or a path for ``delta`` and ``parquet``.
        source_columns: The columns to read, before anything else; empty reads them all.
        filters: Predicates, AND-ed, on the source columns.
        transformations: ``{"name", "expression"}`` entries added in order.
        selection: ``{"name", "expression"?}`` entries, the output in order; empty passes
            every column through.
        workspace_id, lakehouse_id: For ``warehouse_view`` only: the workspace and the
            lakehouse whose SQL endpoint holds the view, when not the notebook's own; the old
            DQF spec's ``defaultWorkspaceId`` and ``defaultLakehouseId``.
        eager_materialize_source: ``false`` by default. When ``true`` the step writes the filtered,
            watermarked source once to a scratch table under ``global_context.staging_table_path``
            and everything after reads that (F201); worth it for an expensive source, not a cheap
            Delta one. Must be a boolean.
    """

    _KEYS = frozenset({"source_kind", "source_identifier", "source_columns", "filters",
                       "transformations", "selection", "workspace_id", "lakehouse_id",
                       "eager_materialize_source"})
    _KINDS = ("delta", "parquet", "view", "warehouse_view")

    def __init__(self, data: dict[str, Any]) -> None:
        self._keys = sorted(data)
        self.source_kind: Any = data.get("source_kind")
        self.source_identifier: Any = data.get("source_identifier")
        self.source_columns: Any = data.get("source_columns", [])
        self.filters: Any = data.get("filters", [])
        self.transformations: Any = data.get("transformations", [])
        self.selection: Any = data.get("selection", [])
        self.workspace_id: Any = data.get("workspace_id")
        self.lakehouse_id: Any = data.get("lakehouse_id")
        self.eager_materialize_source: Any = data.get("eager_materialize_source", False)

    def validate(self) -> None:
        unknown = [k for k in self._keys if k not in self._KEYS]
        if unknown:
            raise ValueError(f"dqd_context: unknown key '{unknown[0]}'; its keys are {sorted(self._KEYS)}")
        if self.source_kind not in self._KINDS:
            raise ValueError(f"dqd_context: source_kind {self.source_kind!r} is not one of {list(self._KINDS)}")
        if not isinstance(self.source_identifier, str) or not self.source_identifier.strip():
            raise ValueError("dqd_context: source_identifier is required and must be a non-empty string")
        for key in ("workspace_id", "lakehouse_id"):
            value = getattr(self, key)
            if value is None:
                continue
            if self.source_kind != "warehouse_view":
                raise ValueError(f"dqd_context: {key} applies to source_kind 'warehouse_view' only")
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"dqd_context: {key} must be a non-empty string")
        if not isinstance(self.eager_materialize_source, bool):
            raise ValueError(
                f"dqd_context: eager_materialize_source must be true or false, got "
                f"{self.eager_materialize_source!r}"
            )
        if not isinstance(self.source_columns, list) or not all(
                isinstance(c, str) and c.strip() for c in self.source_columns):
            raise ValueError("dqd_context: source_columns must be a list of non-empty strings")
        if not isinstance(self.filters, list) or not all(isinstance(f, dict) for f in self.filters):
            raise ValueError("dqd_context: filters must be a list of predicate objects")
        self._check_entries("transformations", self.transformations, expression_required=True)
        self._check_entries("selection", self.selection, expression_required=False)

    @staticmethod
    def _check_entries(section: str, entries: Any, *, expression_required: bool) -> None:
        if not isinstance(entries, list):
            raise ValueError(f"dqd_context: {section} must be a list")
        seen: set[str] = set()
        for i, entry in enumerate(entries):
            where = f"dqd_context.{section}[{i}]"
            if not isinstance(entry, dict):
                raise ValueError(f"{where}: must be an object")
            unknown = [k for k in entry if k not in ("name", "expression")]
            if unknown:
                raise ValueError(f"{where}: unknown key '{unknown[0]}'; the keys are 'name' and 'expression'")
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"{where}: missing required key 'name'")
            if expression_required and "expression" not in entry:
                raise ValueError(f"{where}: missing required key 'expression'")
            if name in seen:
                raise ValueError(f"dqd_context.{section}: names '{name}' more than once")
            seen.add(name)

