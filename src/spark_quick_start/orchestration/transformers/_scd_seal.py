"""
_scd_seal.py — the seal of an SCD table's flag values, open-row sentinel and hash separator.

Contains: `ScdSettings`, `scd_settings`, `check_scd_seal`, `write_scd_seal` and
    `seal_after_write`, which keep one SCD table written with one vocabulary (F211).
Does NOT contain: the Data Vault seal, which stays in _hashing.py (F158); or any operation that
    changes a sealed value, which is a manual procedure (docs/reference/load-context.md).

Seven values describe rows already written: the current/inactive and deleted/not-deleted flag
values, the open row's `default_max_timestamp`, the separator the change hash was computed
with, and the Spark session time zone it was computed under (a timestamp column is cast to
string in that zone, so a new zone changes the hash of every row with a timestamp column).
Changing one on a table that holds rows makes keys load as new again, reads a deleted key
as live, leaves old open rows at the old end, or re-versions every key. The zone is sealed on
every table whether or not it has a timestamp column: simpler, and a column added later is
covered. So the first write
records them as Delta table properties and every later write compares before it writes.

Unlike the Data Vault seal, a populated table with no seal is not refused: that would refuse
every SCD table written before F211. It is sealed after its next successful write with the
context's values, and a warning says so (the owner's decision, 2026-10-04).

The seal is never a reason to fail a load whose data is committed: a seal that cannot be written
after the write (a permission, a metadata conflict, a seal another load wrote first) is a WARNING
and the load stays `success`; the table is still unsealed, so the next load tries again. A target
the property helpers do not accept is neither checked nor sealed: for an existing table the check
finds it before any write; for one this load creates it is found at the seal, after the write, and
is a warning like any other (the owner's decisions, 2026-10-04). Only a seal that is present and
differs is refused, and that before any write.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from spark_quick_start.adapters.platform_adapter import UnsupportedTableIdentifier
from spark_quick_start.orchestration.transformers._hashing import _UNSEALABLE

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from spark_quick_start.adapters.platform_adapter import PlatformAdapter
    from spark_quick_start.orchestration.models import SCDContext

__all__ = ["ScdSettings", "check_scd_seal", "scd_settings", "seal_after_write", "write_scd_seal"]

_PREFIX = "spark_quick_start.scd_"
_UNSUPPORTED = "not a target the seal supports"


class _SealedByAnotherLoad(ValueError):
    """`write_scd_seal` found, after this load's write, a seal with other values than this load's.

    A `ValueError` subclass so `write_scd_seal` still raises a `ValueError`; private because only
    `seal_after_write` tells it apart from a seal that failed to write.
    """


@dataclass(frozen=True)
class ScdSettings:
    """The values an SCD table's existing rows depend on, and so what its seal records.

    `default_max_timestamp` is the UTC instant in ISO 8601, so two spellings of one instant
    seal alike. `hash_separator` is the one the change hash was computed with, which is not
    always the context's (the detector's constructor can supply it). `time_zone` is the Spark
    session's `spark.sql.session.timeZone` the change hash's string casts used.
    """

    current_flag_value: str
    inactive_flag_value: str
    deleted_flag_value: str
    not_deleted_flag_value: str
    default_max_timestamp: str
    hash_separator: str
    time_zone: str

    def __post_init__(self) -> None:
        for name, value in self.as_properties().items():
            if type(value) is not str or not value or _UNSEALABLE.search(value):
                raise ValueError(
                    f"SCD setting {name} = {value!r} cannot be sealed on a table: it must be a "
                    "non-empty string with no quote, backslash, backtick, `$` or control character."
                )

    def as_properties(self) -> dict[str, str]:
        """The settings as the table properties that seal them."""
        return {f"{_PREFIX}{k}": v for k, v in vars(self).items()}


def scd_settings(scd: SCDContext, hash_separator: str | None = None, *, spark: SparkSession) -> ScdSettings:
    """The settings this load writes with: the context's, the separator the hash used, and the
    session's time zone.

    Cost: one session conf read, no Spark action. Read once per run, here.

    Args:
        spark: The session; its `spark.sql.session.timeZone` (`"UTC"` when unset, as the Data
            Vault seal does) is sealed whether or not the table has a timestamp column.
        scd: The load's SCD context.
        hash_separator: The separator the change detector resolved (`PipelineState.hash_separator`);
            `None`, when no detector stated one, reads the context's.
    """
    return ScdSettings(
        current_flag_value=scd.current_flag_value,
        inactive_flag_value=scd.inactive_flag_value,
        deleted_flag_value=scd.deleted_flag_value,
        not_deleted_flag_value=scd.not_deleted_flag_value,
        default_max_timestamp=scd.default_max_timestamp_utc.isoformat(),
        hash_separator=scd.hash_separator if hash_separator is None else hash_separator,
        time_zone=spark.conf.get("spark.sql.session.timeZone") or "UTC",
    )


def check_scd_seal(adapter: PlatformAdapter, spark: SparkSession, identifier: str,
                   settings: ScdSettings, *, target_exists: bool | None, logger: Any) -> str:
    """Compare *settings* with the table's seal, before a write; say whether to seal after it.

    Cost: for an existing table one metadata read (`DESCRIBE DETAIL`, a Spark action that reads
    no data). A target the step stated absent costs nothing here; `target_exists=None` (no target
    step) costs one existence check first.

    Args:
        target_exists: The target step's answer (`PipelineState.target_exists`); decided on as
            given, with no second check, so a swallowed error cannot make an existing table look
            unsealed (F209, F210).
        logger: An `OrchestratorLogger`, for the unsupported-target warning.

    Returns:
        `"unsupported"` when the adapter's `get_table_properties` refuses the identifier of an
        existing target with `UnsupportedTableIdentifier` (a hyphenated or backtick-quoted name
        part, a four-part name, a path with a backtick, `$` or control character): it is neither
        checked nor sealed, one WARNING names it, and the load writes as it did before F211. Only
        that refusal is caught; any other error propagates, before any write. `"first_write"`
        when the table is absent (seal after the write; an identifier the seal cannot handle
        is found then, and is a warning, `seal_after_write`), `"unsealed"` when it exists with
        one or more of the properties missing (seal after the write, with a warning), `"sealed"`
        when every property is there and equal.

    Raises:
        ValueError: when a sealed property differs from this load's value; nothing has been
            written. The message names the table, the property, both values, and the manual
            procedure.
    """
    if target_exists is None:
        target_exists = adapter.delta_table_exists(spark, identifier)
    if not target_exists:
        return "first_write"
    wanted = settings.as_properties()
    try:
        properties = adapter.get_table_properties(spark, identifier)
    except UnsupportedTableIdentifier as exc:
        logger.warning(f"{identifier!r} is {_UNSUPPORTED}: {exc}. Its SCD values were neither "
                       "checked nor sealed, and the load writes as it would without a seal.")
        return "unsupported"
    sealed = {k: v for k, v in properties.items() if k in wanted}
    _refuse_difference(identifier, wanted, sealed)
    return "sealed" if len(sealed) == len(wanted) else "unsealed"


def write_scd_seal(adapter: PlatformAdapter, spark: SparkSession, identifier: str,
                   settings: ScdSettings) -> list[str]:
    """Record the properties the table lacks, after the write `check_scd_seal` allowed.

    Compare-and-set: the properties are read again first, and one that now differs is refused,
    never overwritten (two first loads racing with different values). The read and the write are
    two steps, so the race is narrowed, not closed.

    Cost: one metadata read (`DESCRIBE DETAIL`), then, when something is missing, one Spark
    action and one Delta commit on table metadata, which conflicts with a write to the table
    running at the same time. This read is a second one for an `"unsealed"` table, whose check
    read it already.

    Returns:
        The property names that were set (empty when the seal was already complete).

    Raises:
        ValueError: when the table now carries a different value; nothing is set.
    """
    wanted = settings.as_properties()
    sealed = {k: v for k, v in adapter.get_table_properties(spark, identifier).items() if k in wanted}
    _refuse_difference(identifier, wanted, sealed, written=True)
    missing = {k: v for k, v in wanted.items() if k not in sealed}
    if missing:
        adapter.set_table_properties(spark, identifier, missing)
    return sorted(missing)


def seal_after_write(adapter: PlatformAdapter, spark: SparkSession, identifier: str,
                     settings: ScdSettings, plan: str, logger: Any) -> None:
    """Seal the table after a successful write, as `check_scd_seal` planned; never fails the load.

    A no-op for `"sealed"` and `"unsupported"`. The data is committed by now, so any failure to
    seal (the compare-and-set refusal of `write_scd_seal`, a metadata conflict with a concurrent
    writer, a missing privilege: on Unity Catalog `SET TBLPROPERTIES` needs the owner or MANAGE)
    is a WARNING naming the table, the reason and the values not sealed, and the load stays
    `success`. The table is still unsealed, so the next load tries again, except when another
    load sealed it with different values meanwhile: that has its own WARNING, says the table may
    hold rows written two ways, and that the next load will be refused. This is the one
    sanctioned swallow of this module, by the owner's decision (2026-10-04). An identifier the
    adapter's property helpers refuse (`UnsupportedTableIdentifier`), found here for a table this
    load created, gets its own wording: "not a target the seal supports".

    An `"unsealed"` table (rows written before F211, or a load whose seal failed) that is sealed
    here gets a WARNING naming the values: the rows already there are assumed to have been written
    with them, which only the owner can know. Cost as `write_scd_seal`.

    Args:
        plan: What `check_scd_seal` returned.
        logger: An `OrchestratorLogger`.
    """
    if plan in ("sealed", "unsupported"):
        return
    try:
        set_now = write_scd_seal(adapter, spark, identifier, settings)
    except UnsupportedTableIdentifier as exc:
        logger.warning(f"{identifier!r} was written but is {_UNSUPPORTED}: {exc}. Its SCD values "
                       "were not sealed, and every load of it will say so.")
        return
    except _SealedByAnotherLoad as exc:
        logger.warning(
            f"{identifier!r}: another load sealed the table with different values while this load "
            f"wrote it ({exc}). This load's rows were written under its own values "
            f"{settings.as_properties()}, so the table may now hold rows written two ways. The load "
            "stays successful, but the next load will be refused: check the rows before it "
            "(docs/reference/load-context.md)."
        )
        return
    except Exception as exc:  # the data is committed; the next load retries the seal
        logger.warning(
            f"{identifier!r} was written but could not be sealed ({type(exc).__name__}: {exc}). "
            f"Not sealed: {settings.as_properties()}. The load stays successful; the table is "
            "still unsealed, so the next load tries again."
        )
        return
    if plan == "unsealed" and set_now:
        logger.warning(
            f"{identifier!r} held rows but had no SCD seal: sealed it with this load's values "
            f"{ {k: settings.as_properties()[k] for k in set_now} }. The rows already there are "
            "assumed to have been written with them; if they were not, rewrite them and reseal "
            "(docs/reference/load-context.md)."
        )


def _refuse_difference(identifier: str, wanted: dict[str, str], sealed: dict[str, str],
                       *, written: bool = False) -> None:
    """Raise for the first sealed property whose value is not this load's."""
    for prop, value in wanted.items():
        if prop in sealed and sealed[prop] != value:
            if written:
                outcome = ("This load's rows are already written and another load sealed the table "
                           "first, so its rows may now be written two ways: check the table before "
                           "the next load.")
            else:
                outcome = "Nothing was written."
            source = (" (scd_context.hash_separator, or the hash_separator argument of "
                      "SCDChangeDetectorTransformer)" if prop.endswith("hash_separator") else
                      " (the Spark session time zone, spark.sql.session.timeZone)"
                      if prop.endswith("time_zone") else "")
            if prop.endswith("time_zone"):
                remedy = (f"Run the load in a session with spark.sql.session.timeZone = {sealed[prop]!r}; "
                          "changing it on purpose re-versions every key whose row has a timestamp "
                          "column (no UPDATE applies), then ALTER TABLE ... SET TBLPROPERTIES "
                          f"('{prop}' = '<new value>') (or UNSET it), as docs/reference/load-context.md describes.")
            else:
                remedy = ("Align the load's scd_context with the seal; to change it on purpose, rewrite "
                          "the rows (UPDATE) to the new value, then ALTER TABLE ... SET TBLPROPERTIES "
                          f"('{prop}' = '<new value>') (or UNSET it), as docs/reference/load-context.md describes.")
            raise (_SealedByAnotherLoad if written else ValueError)(
                f"{identifier!r} is sealed with {prop} = {sealed[prop]!r}, and this load's "
                f"value{source} is {value!r}. Rows already written depend on it: a changed flag "
                "value stops matching them, a changed default_max_timestamp leaves old open rows "
                "at the old end, a changed hash_separator re-versions every key, and a changed session "
                "time zone would change the hash of every timestamp column, so every key with one "
                f"is re-versioned. {outcome} {remedy}"
            )
