"""
_hashing.py — the Data Vault hash diff, and the seal of its settings on a table.

Contains: `HashSettings`, `hash_hex` (the column expression a satellite's hash diff is), and
    `check_hash_seal` / `write_hash_seal`, which keep one table's diff computed one way.
Does NOT contain: the SCD hash, which stays `xxhash64` in change_detector.py; or the hash key,
    which is built upstream (the accepted Data Vault design, decision 5).

A satellite whose hash diff was computed two ways, by two load contexts or after a settings
change, reports changes that did not happen, silently. So the first write records the settings
as Delta table properties, and every later write compares (F158). Sealing is check-then-act and
the seal is a commit of its own after the data write, so only a missing or empty table counts as
a first write; a populated table with no seal is refused rather than sealed late (the
data-platform-architect, 2026-10-01).
"""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import Column
from pyspark.sql import functions as F

from spark_quick_start.orchestration.transformers._constants import Constants

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from spark_quick_start.adapters.platform_adapter import PlatformAdapter
    from spark_quick_start.orchestration.models import SCDContext

__all__ = ["HASH_ALGORITHMS", "HashSettings", "check_hash_seal", "hash_hex", "hash_settings", "write_hash_seal"]

HASH_ALGORITHMS = Constants.DV_HASH_ALGORITHMS
_PREFIX = "spark_quick_start.hash_"
# What a sealed value may not hold: the adapter refuses the same characters in
# set_table_properties (F157). Checked here too so a setting that cannot be sealed is refused
# before the first data write, not after it, which would leave a populated, unsealed table.
_UNSEALABLE = re.compile(r"['\\`$\x00-\x1f\x7f]")


@dataclass(frozen=True)
class HashSettings:
    """How a hash diff is computed, and so what its seal records.

    `time_zone` is the session's `spark.sql.session.timeZone`: a timestamp is cast to string in
    it, so the same row hashes differently under two time zones; the seal holds it so a load in
    another zone is refused rather than reporting every timestamp column as changed.
    """

    algorithm: str
    separator: str
    null_token: str
    time_zone: str

    def __post_init__(self) -> None:
        if self.algorithm not in HASH_ALGORITHMS:
            raise ValueError(f"hash algorithm {self.algorithm!r} is not one of {list(HASH_ALGORITHMS)}")
        if not self.separator:
            # concat_ws with no separator makes ("a", "bc") and ("ab", "c") the same string.
            raise ValueError("hash separator is empty, which would make different rows hash alike")
        for name, value in self.as_properties().items():
            if type(value) is not str or not value or _UNSEALABLE.search(value):
                raise ValueError(
                    f"hash setting {name} = {value!r} cannot be sealed on a table: it must be a "
                    "non-empty string with no quote, backslash, backtick, `$` or control character."
                )

    def as_properties(self) -> dict[str, str]:
        """The settings as the table properties that seal them."""
        return {f"{_PREFIX}algorithm": self.algorithm, f"{_PREFIX}separator": self.separator,
                f"{_PREFIX}null_token": self.null_token, f"{_PREFIX}time_zone": self.time_zone}


def hash_settings(scd: SCDContext, spark: SparkSession) -> HashSettings:
    """The settings a Data Vault load hashes with: the context's algorithm and separator, the
    one null token, and the session's time zone. Shared by the satellite detector, which
    computes the diff, and the writers, which seal it, so the two cannot disagree."""
    return HashSettings(algorithm=scd.hash_algorithm, separator=scd.hash_separator,
                        null_token=Constants.HASH_NULL_TOKEN,
                        time_zone=spark.conf.get("spark.sql.session.timeZone") or "UTC")


def hash_hex(columns: Sequence[str], settings: HashSettings) -> Column:
    """The hash diff over *columns*, in the order given, as lower-case hex.

    Each column is cast to string with NULL replaced by the token, the values are joined by the
    separator, and the result is `sha2(…, 256)` or `md5`. Reproducible in Spark SQL under the
    same session time zone; another engine casts booleans, decimals, doubles and timestamps to
    string its own way, and whether a consumer's SQL endpoint rebuilds the same digest is a live
    check the Data Vault design owes (F162). No Spark action: a column expression.
    """
    joined = F.concat_ws(settings.separator,
                         *[F.coalesce(F.col(c).cast("string"), F.lit(settings.null_token)) for c in columns])
    return F.sha2(joined, 256) if settings.algorithm == "sha256" else F.md5(joined)


def check_hash_seal(adapter: PlatformAdapter, spark: SparkSession, identifier: str,
                    settings: HashSettings) -> bool:
    """Compare *settings* with the table's seal, before a write; return whether to seal after it.

    Cost: one existence check, then for an existing table one metadata read, and for an
    existing table with no seal one `isEmpty()` over it, the only action that reads data.

    Returns:
        True when the table is missing, or present and empty with no seal: the write about to
        happen is its first, and `write_hash_seal` follows it. False when the seal matches.

    Raises:
        ValueError: when the seal differs from *settings*, is partial, or is absent from a table
            that already has rows; nothing has been written.
    """
    if not adapter.delta_table_exists(spark, identifier):
        return True
    wanted = settings.as_properties()
    sealed = {k: v for k, v in adapter.get_table_properties(spark, identifier).items() if k in wanted}
    if sealed:
        if sealed != wanted:
            raise ValueError(
                f"{identifier!r} is sealed with hash settings {sealed}, and this load would compute "
                f"its hash diff with {wanted}. A table hashed two ways reports changes that did not "
                "happen; align the load's scd_context with the seal."
            )
        return False
    if not adapter.read_delta(spark, identifier).isEmpty():
        raise ValueError(
            f"{identifier!r} has rows but no hash seal, so the settings its hash diff was computed "
            "with are unknown. It was written outside this library, or a load stopped between its "
            "first write and the seal; seal it deliberately with set_table_properties once the "
            "settings are known."
        )
    return True


def write_hash_seal(adapter: PlatformAdapter, spark: SparkSession, identifier: str,
                    settings: HashSettings) -> None:
    """Record *settings* on the table, after the first write `check_hash_seal` allowed.

    Compare-and-set: the table's properties are read again first, and a seal that differs or is
    partial is refused, never overwritten. Two first writers racing with different settings, or a
    `delta_table_exists` that answered False for a table it failed to see, would otherwise leave
    the last seal standing over rows hashed the other way. The read and the write are still two
    steps, so the race is narrowed, not closed: two concurrent first loads with different
    settings can both pass the read.

    Cost: one metadata read, then one Spark action and one Delta commit on table metadata, which
    conflicts with any write to the table running at the same time.

    Raises:
        ValueError: when the table already carries a different or partial seal; nothing is set.
    """
    wanted = settings.as_properties()
    sealed = {k: v for k, v in adapter.get_table_properties(spark, identifier).items() if k in wanted}
    if sealed == wanted:
        return
    if sealed:
        raise ValueError(
            f"{identifier!r} was sealed with hash settings {sealed} while this load wrote it with "
            f"{wanted}; another load sealed it first. Its rows may now be hashed two ways: check "
            "the table before the next load."
        )
    adapter.set_table_properties(spark, identifier, wanted)
