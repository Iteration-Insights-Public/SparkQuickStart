"""
platform_adapter.py — PlatformAdapter ABC and MockPlatformAdapter.

Contains: PlatformAdapter abstract base class, MockPlatformAdapter test double.
Does NOT contain: any platform-specific SDK imports (notebookutils, dbutils, mssparkutils).
    Contains: default Delta routing methods (read_delta, write_delta_overwrite,
    delta_table_exists, resolve_delta_table) that dispatch based on identifier type.

Usage (testing)::

    from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
    from spark_quick_start.orchestration.models import ExecutionResult, ExecutionMetrics
    from dataclasses import asdict

    adapter = MockPlatformAdapter(
        notebook_results={
            "NB_Stage_Orders": ExecutionResult(
                status="success", has_error=False,
                metrics=asdict(ExecutionMetrics(source_count=10, target_count=10))
            ).to_json()
        }
    )
    assert adapter.run_calls == []
"""
from __future__ import annotations

import ntpath
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Prefixes that mark a path without a ``//``: an absolute path, ``dbfs:/x``, ``file:x``.
# Any ``scheme://`` URI is caught by _URI_SCHEME_RE instead, so no scheme is listed here (F163).
_PATH_PREFIXES = ("/", "dbfs:/", "file:")
_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")

try:
    from delta.tables import DeltaTable  # type: ignore
except ImportError:
    DeltaTable = None  # type: ignore


_UC_NAME_RE = re.compile(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){0,2}")


def _quote_uc_name(identifier: str) -> str:
    """Backtick-quote a UC name for interpolation into DDL.

    drop_delta() is the one place this codebase builds SQL by interpolation —
    DROP TABLE takes an identifier, which cannot be a bind parameter. The
    identifier is derived from caller configuration (staging_table_path and
    global_context.id, which build the scratch table names), so it is validated against a
    strict shape first and quoted second.

    Statement chaining is not the risk — Spark's sql() parses a single
    statement and rejects "a; DROP TABLE b" outright. The risk is a value that
    is itself a valid identifier pointing somewhere unintended. Validation
    cannot decide intent, but it does guarantee the string is nothing but a
    one-to-three-part name, so no quoting or comment syntax can escape.

    Raises:
        ValueError: if *identifier* is not a plain 1-3 part name.
    """
    if not _UC_NAME_RE.fullmatch(identifier):
        raise ValueError(
            f"Refusing to build DDL for unsafe table identifier {identifier!r}. "
            "Expected a plain catalog.schema.table name (letters, digits, "
            "underscores, at most three dot-separated parts)."
        )
    return ".".join(f"`{part}`" for part in identifier.split("."))


# Table properties the library writes live under its own namespace (F157); the values are
# settings such as an algorithm name or a separator, never free text. `$` is refused everywhere
# SQL text is built here: Spark substitutes `${…}` (spark.sql.variable.substitute, on by default)
# inside quoted literals and backtick-quoted names alike, before parsing, so a value or a path
# holding it can be rewritten into another key, another table or another clause (F157's
# check-adversary, reproduced on Spark 3.5.1).
_PROPERTY_KEY_RE = re.compile(r"spark_quick_start\.[a-z_]+")
_UNSAFE_VALUE_RE = re.compile(r"['\\`$\x00-\x1f\x7f]")
# A backslash is literal inside a backtick-quoted name, so Windows and UNC paths keep theirs.
_UNSAFE_PATH_RE = re.compile(r"[`$\x00-\x1f\x7f]")


def _quote_delta_path(path: str) -> str:
    """Quote a filesystem path or URI as a `delta.` table reference for SQL (F157).

    Raises:
        ValueError: if *path* is not a plain `str`, or holds a backtick, a `$` or a control
            character, which could end the quoted name or be rewritten by Spark's `${…}`
            variable substitution.
    """
    if type(path) is not str or not path or _UNSAFE_PATH_RE.search(path):
        raise ValueError(
            f"Refusing to build SQL for unsafe table path {path!r}: it must be a plain string "
            "with no backtick, `$` or control character."
        )
    return f"delta.`{path}`"


class UnsupportedTableIdentifier(ValueError):
    """The identifier cannot be turned into SQL by `get_table_properties` or `set_table_properties`.

    Raised for a name that is not 1 to 3 plain parts (hyphenated, backtick-quoted, four parts) and
    for a path with a backtick, `$` or control character, so a caller can tell this refusal from
    any other `ValueError`, such as a refused property key or value (F211). A `ValueError`
    subclass: code that catches `ValueError` is unaffected.
    """


def _table_reference(identifier: str) -> str:
    """The SQL reference for *identifier*: a quoted catalog name, or a quoted `delta.` path.

    A `str` subclass is refused outright: its `split` or `__format__` could differ from what
    the checks saw.

    Raises:
        UnsupportedTableIdentifier: for every refusal, a `ValueError` carrying the reason.
    """
    try:
        if type(identifier) is not str:
            raise ValueError(f"Refusing to build SQL for unsafe table identifier {identifier!r}: not a str.")
        return _quote_uc_name(identifier) if _is_uc_name(identifier) else _quote_delta_path(identifier)
    except UnsupportedTableIdentifier:
        raise
    except ValueError as exc:
        raise UnsupportedTableIdentifier(str(exc)) from exc


def _property_literals(properties: Mapping[str, str]) -> str:
    """`'key' = 'value'` pairs for TBLPROPERTIES, each key and value checked first."""
    pairs = []
    for key, value in properties.items():
        if type(key) is not str or not _PROPERTY_KEY_RE.fullmatch(key):
            raise ValueError(
                f"Refusing table property key {key!r}: the library writes only "
                "'spark_quick_start.<lower_snake_case>' keys."
            )
        if type(value) is not str or _UNSAFE_VALUE_RE.search(value):
            raise ValueError(
                f"Refusing table property value {value!r} for {key!r}: it must be a plain string "
                "with no quote, backslash, backtick, `$` or control character."
            )
        pairs.append(f"'{key}' = '{value}'")
    return ", ".join(pairs)


def _is_uc_name(identifier: str) -> bool:
    """Return True when *identifier* looks like a Unity Catalog name.

    Returns True for any string that is not an absolute filesystem path or URI. Any
    ``scheme://`` prefix (``hdfs://``, ``abfs://``, ``s3n://``, a scheme not yet invented)
    marks a path, never a name (F163).
    This includes three-part UC names (``catalog.schema.table``), two-part names
    (``schema.table``), and bare table names (``table``). Callers that need strict
    three-part validation must check separately.
    """
    if any(identifier.startswith(p) for p in _PATH_PREFIXES):
        return False
    if _URI_SCHEME_RE.match(identifier):
        return False
    drive, _ = ntpath.splitdrive(identifier)
    if drive:
        return False
    if identifier.startswith("\\\\"):
        return False
    return True


class PlatformAdapter(ABC):
    """Abstract adapter for platform-specific operations.

    Implement this interface for each target platform so the orchestration
    logic stays platform-agnostic and fully unit-testable.

    All platform runtime calls (notebook execution, filesystem, notebook exit,
    log persistence adapter resolution) go through this interface. Core
    orchestration modules import only this ABC; concrete implementations live
    in separate files and are never imported from core modules.
    """

    def is_table_identifier(self, identifier: str) -> bool:
        """Is *identifier* a table name rather than a filesystem path or URI?

        The public form of the routing decision every path-or-name argument in this class
        already makes internally. Concrete, with a portable default, and deliberately not
        abstract: the answer is pure string inspection with no platform SDK in it, so there
        is nothing for a platform to override and nothing a MockPlatformAdapter needs to
        record. Rule 3's override-plus-stub shape applies to capabilities that differ per
        platform; this one does not.

        Added because `orchestration/delta_utils.py` was importing the private
        `_is_uc_name` across the layer boundary to make the same decision itself — which
        works, and makes the next routing change a two-place edit. Rule 6 puts the decision
        behind the adapter so callers pass an identifier and stay ignorant of which it is
        (F6).

        Args:
            identifier: A Unity Catalog name, a bare table name, an absolute filesystem
                path, or a URI.

        Returns:
            True for anything that is not an absolute path, a Windows drive path, a UNC
            path or a URI — which covers three-part, two-part and bare table names.
            Callers needing strict three-part validation must check separately.
        """
        return _is_uc_name(identifier)


    @abstractmethod
    def run_notebook(self, name: str, timeout: int, args: dict[str, Any]) -> str:
        """Run a notebook synchronously. Return the exit value as a JSON string."""

    @abstractmethod
    def run_notebooks_parallel(self, dag: dict[str, Any]) -> dict[str, Any]:
        """Run multiple notebooks in parallel. Return {activity_name: {exitVal: ...}}."""

    @abstractmethod
    def delete_path(self, path: str) -> None:
        """Delete a filesystem path (best-effort; ignore if not found)."""

    @abstractmethod
    def get_current_notebook_name(self) -> str | None:
        """Return the name of the currently executing notebook, or None."""

    def notebook_exit(self, value: str) -> None:
        """Signal notebook completion with the given exit value string.

        Default implementation is a no-op. Override on Fabric
        (``notebookutils.notebook.exit``) and Databricks
        (``dbutils.notebook.exit``).

        Args:
            value: Exit value string (typically ``ExecutionResult.to_json()``).
        """

    def get_log_persistence_adapter(self) -> Any | None:
        """Return a log persistence adapter appropriate for this platform.

        Used by ``OrchestratorLogger.from_logging_context`` to wire up the
        correct log persistence backend without platform-specific code in core.

        Returns:
            A log persistence adapter instance, or ``None`` when no persistence
            is desired (local/test mode). Default implementation returns ``None``.
        """
        return None

    def read_delta(self, spark: SparkSession, identifier: str) -> DataFrame:
        """Read a Delta table from an absolute path or Unity Catalog name.

        Routes to ``spark.table()`` for UC names and ``spark.read.format('delta').load()``
        for filesystem paths. Default implementation works on Fabric and Databricks.
        """
        if _is_uc_name(identifier):
            return spark.table(identifier)
        return spark.read.format("delta").load(identifier)

    def read_parquet(self, spark: SparkSession, identifier: str) -> DataFrame:
        """Read parquet files at a path: a DQD source of kind ``parquet`` (F139).

        The default, ``spark.read.parquet``, is portable: it works on Fabric, Databricks and
        locally, so no adapter overrides it. A platform whose paths need resolving first would.
        """
        return spark.read.parquet(identifier)

    def read_table(self, spark: SparkSession, identifier: str) -> DataFrame:
        """Read a catalog table or view by name: a DQD source of kind ``view`` (F139).

        The default, ``spark.table``, reads a Unity Catalog or metastore view the same way on
        every platform. A Fabric Warehouse view is another capability (backlog F140).
        """
        return spark.table(identifier)

    def read_warehouse_view(
        self,
        spark: SparkSession,
        identifier: str,
        *,
        workspace_id: str | None = None,
        lakehouse_id: str | None = None,
    ) -> DataFrame:
        """Read a Fabric Warehouse or SQL-endpoint view: a DQD source of kind ``warehouse_view`` (F140).

        Only Fabric has one; `FabricAdapter` overrides this. Every other platform refuses,
        pointing at the portable ``view`` kind for a catalog view.

        Raises:
            NotImplementedError: on any platform but Fabric.
        """
        raise NotImplementedError(
            f"{type(self).__name__}: a warehouse view is a Fabric source, read through Fabric's "
            "Warehouse connector; for a catalog table or view use source_kind 'view'"
        )

    def write_delta_overwrite(
        self,
        df: DataFrame,
        identifier: str,
        overwrite_schema: bool = False,
    ) -> None:
        """Write *df* to a Delta location in overwrite mode.

        Routes to ``.saveAsTable()`` for UC names and ``.save()`` for filesystem paths.
        When *overwrite_schema* is True, adds ``.option('overwriteSchema', 'true')``.
        """
        writer = df.write.format("delta").mode("overwrite")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if _is_uc_name(identifier):
            writer.saveAsTable(identifier)
        else:
            writer.save(identifier)

    def write_delta_create(self, df: DataFrame, identifier: str) -> None:
        """Create a Delta table from *df*, failing if anything is already there (F159).

        `mode("errorifexists")`: an existing table makes it raise instead of writing. The SCD
        merge (F209) and the Data Vault writers create their tables through it, so an existence
        check that answered False for a table it failed to see cannot turn the first write into
        an overwrite of, or an append over, existing rows. A path holding other files but no Delta log is something Delta's check does not see and
        writes over, so a local path is first asked about through `path_has_content` and refused when it
        holds files and is not a Delta table (F164). The check sees local paths only: a URI path
        or catalog name answers False there and is left to `errorifexists`. Routes like
        `write_delta_append`; a portable default no platform overrides. Cost: one write and one
        Delta commit, plus, for a path, one local directory listing (and, only when the listing
        finds entries, a second one of `_delta_log`; no `DeltaTable` call and no Spark action).

        Raises:
            ValueError: the path already holds files and is not a Delta table; nothing is written.
            Exception: Spark's own error when the Delta table or path already exists.
        """
        is_name = _is_uc_name(identifier)
        if not is_name:
            spark = df.sparkSession
            if (self.path_has_content(spark, identifier)
                    and not self.path_has_content(spark, identifier.rstrip("/\\") + "/_delta_log")):
                raise ValueError(
                    f"Refusing to create a Delta table at {identifier!r}: the path already exists "
                    "and holds files, but is not a Delta table; Delta would write its log over them. "
                    "This is usually a mistyped path or a configuration copied from another "
                    "environment. Check the path before retrying; nothing has been written. If the directory holds an "
                    "empty _delta_log, a crashed first write left it: remove the directory by hand."
                )
        writer = df.write.format("delta").mode("errorifexists")
        if is_name:
            writer.saveAsTable(identifier)
        else:
            writer.save(identifier)

    def write_delta_append(self, df: DataFrame, identifier: str) -> None:
        """Append *df* to a Delta location, creating it if it doesn't exist yet.

        Routes to ``.saveAsTable()`` for UC names and ``.save()`` for filesystem paths.
        """
        writer = df.write.format("delta").mode("append")
        if _is_uc_name(identifier):
            writer.saveAsTable(identifier)
        else:
            writer.save(identifier)

    def delta_table_exists(self, spark: SparkSession, identifier: str) -> bool:
        """Return True if a Delta table exists at *identifier*.

        Uses ``spark.catalog.tableExists()`` for UC names and
        ``DeltaTable.isDeltaTable()`` for filesystem paths. Any error of either check
        answers False, with one exception: a path checked without delta-spark installed.

        Raises:
            RuntimeError: *identifier* is a path and delta-spark is not installed. Answering
                False there would send the load down its first-load branch, to fail at the
                create with an "already exists" error that names neither cause.
        """
        if DeltaTable is None and not _is_uc_name(identifier):
            raise RuntimeError(
                "delta-spark is not installed. Install it to check a Delta table by path "
                f"({identifier!r}), or name the table instead."
            )
        try:
            if _is_uc_name(identifier):
                return spark.catalog.tableExists(identifier)
            return DeltaTable.isDeltaTable(spark, identifier)
        except Exception:
            return False

    def drop_delta(self, spark: SparkSession, identifier: str) -> None:
        """Drop a Delta table completely — metadata, data files, and history.

        Routes the same way the other Delta helpers do: ``DROP TABLE`` for a
        Unity Catalog name, a filesystem delete for a path. ``delete_path``
        alone is not sufficient — it is a filesystem operation, so on a UC name
        it would be interpreted as a relative path, and even if it removed the
        files it would leave the metastore entry behind.

        This is the recycle step for disposable scratch tables. Overwriting in
        place leaves every superseded file on disk until VACUUM runs, which is
        why an unvacuumed materialize scratch path was observed at 45 GB
        holding 2.3 GB of live data across 22 versions. Dropping reclaims the
        data, the log, and any stale table properties or schema in one step.

        Dropping something that is already absent is the intended end state, not an
        error. Errors from the drop itself are swallowed. A path the driver's local
        filesystem shows with entries (see ``path_has_content``, e.g. a Databricks
        ``/Volumes/...`` path) is decided by ``<path>/_delta_log`` holding entries, and a
        directory of other files is refused; errors from that check propagate, so an
        unverified path is never deleted. Any other path (a URI, a Databricks ``/mnt/...``
        path, an empty or absent directory) goes to ``delta_table_exists``
        (``DeltaTable.isDeltaTable``), as before F164, and is dropped only when that answers
        True; it answers False on any error, and then the drop does nothing (the pre-F164
        behaviour).
        Cost: for a local path, one or two directory listings; otherwise whatever
        ``delta_table_exists`` costs; no Spark action of its own.

        Args:
            spark: Active SparkSession (needed to issue DROP TABLE).
            identifier: UC table name or filesystem path.
        """
        is_name = _is_uc_name(identifier)
        if not (self.delta_table_exists(spark, identifier) if is_name
                else self._path_is_delta(spark, identifier)):
            # Either nothing is there — already the intended end state — or
            # something visible is there that is not a Delta table. The method
            # never deletes that, so the guard turns a silent no-op into a
            # diagnostic for a mistyped path. The hazard behind it is a chain:
            # a mistyped path lands on a foreign directory, a Delta write puts
            # a _delta_log in it, and a later drop sees Delta and deletes the
            # lot. Scratch paths are {staging_table_path}_{id}_{n}_{...} since
            # F129, which makes the chain near-implausible.
            if self.path_has_content(spark, identifier):
                raise ValueError(
                    f"Refusing to drop {identifier!r}: it exists but is not a Delta "
                    "table. This is usually a mistyped path or a configuration "
                    "copied from another environment. Check the staging or scratch "
                    "path before retrying — nothing has been deleted. If the directory holds an empty "
                    "_delta_log, a crashed first write left it: remove the directory by hand."
                )
            return

        try:
            if is_name:
                spark.sql(f"DROP TABLE IF EXISTS {_quote_uc_name(identifier)}")
            else:
                self.delete_path(identifier)
        except ValueError:
            raise
        except Exception:
            pass

    def _path_is_delta(self, spark: SparkSession, path: str) -> bool:
        """Whether *path* is a Delta table (F164).

        A path the driver's local filesystem shows with entries (`path_has_content`) is Delta
        when its ``_delta_log`` holds entries. Any
        other path (a URI, a Databricks ``/mnt/...`` path, an empty or absent directory) goes
        to `delta_table_exists`, as before F164, so it is dropped only when `isDeltaTable`
        answers True and is otherwise left to grow. The visibility
        predicate is `path_has_content` itself, defined once. Errors from the listing propagate.
        """
        if self.path_has_content(spark, path):
            return self.path_has_content(spark, path.rstrip("/\\") + "/_delta_log")
        return self.delta_table_exists(spark, path)

    def path_has_content(self, spark: SparkSession, path: str) -> bool:
        """True when the filesystem *path* exists and holds at least one entry (F164).

        Tells "nothing there" from "something there that is not a Delta table", which decides
        between a silent no-op and a loud refusal in `drop_delta` and `write_delta_create`.
        An empty directory has no content: Delta may write into it. A file counts as content.

        Reads the local filesystem only. A URI-scheme path (``abfss://``, ``dbfs:/``) or a
        catalog name is not something it can see and answers False; no platform overrides it,
        because a listing whose failure shape nobody has observed could fail closed on a live
        runtime (F207). That is enough after F129: every scratch path is
        ``{staging_table_path}_{id}_{n}_{materialize|dqd}``, which makes the mistyped-path
        chain `drop_delta` guards against near-implausible. Returns False whenever it cannot
        tell, so an inability to check never becomes a spurious refusal. Cost: one local
        directory listing; no Spark action (`spark` is unused).

        Args:
            spark: Active SparkSession; unused by the default.
            path: A filesystem path or URI.
        """
        if _is_uc_name(path):
            return False
        try:
            from pathlib import Path
            target = Path(path)
            if target.is_dir():
                return next(target.iterdir(), None) is not None
            return target.exists()
        except OSError:
            return False

    def get_table_properties(self, spark: SparkSession, identifier: str) -> dict[str, str]:
        """Return the Delta table's properties, by catalog name or path (F157).

        Concrete, with a portable default and no per-platform override: one SQL statement,
        `DESCRIBE DETAIL`, so it needs no `DeltaTable` and holds on Spark Connect and serverless,
        where `resolve_delta_table` raises. Cost: one Spark action on table metadata only.

        Args:
            spark: The session.
            identifier: A catalog name (1 to 3 parts) or a path or URI.

        Returns:
            Every property the table has, the library's (`spark_quick_start.*`) and Delta's
            alike; empty when it has none.

        Raises:
            ValueError: if *identifier* is not a plain `str`, is a name that is not 1 to 3
                plain parts (a relative `Tables/x` counts as a name and is refused), or is a
                path with a backtick, `$` or control character.
            Exception: Spark's own error when nothing is there or it is not a Delta table;
                a caller tells "missing" from "present without properties" with
                `delta_table_exists` first.
        """
        row = spark.sql(f"DESCRIBE DETAIL {_table_reference(identifier)}").first()
        return dict(row["properties"] or {}) if row is not None else {}

    def set_table_properties(
        self, spark: SparkSession, identifier: str, properties: Mapping[str, str]
    ) -> None:
        """Set properties on the Delta table, by catalog name or path (F157).

        Concrete, with a portable default and no per-platform override:
        `ALTER TABLE … SET TBLPROPERTIES`, a Delta commit of its own. Keys must be the
        library's (`spark_quick_start.<name>`) and values plain settings, because both are
        interpolated into the statement (spark-delta-cost 5). Nothing to set issues no SQL.
        Cost: one Spark action and one Delta commit on table metadata. The commit changes the
        table's metadata, so it conflicts with any write to the table running at the same time,
        and that conflict is not an append conflict `run_with_delta_retry` retries; the caller
        decides whether to seal only when no other writer can be active, or to retry.

        Raises:
            ValueError: for an identifier refused as `get_table_properties` refuses it, a key
                outside the namespace, or a value that is not a plain `str` or holds a quote,
                backslash, backtick, `$` or control character; nothing is written.
        """
        if not properties:
            return
        reference = _table_reference(identifier)
        literals = _property_literals(properties)
        spark.sql(f"ALTER TABLE {reference} SET TBLPROPERTIES ({literals})")

    def resolve_delta_table(self, spark: SparkSession, identifier: str) -> Any:
        """Return a ``DeltaTable`` instance for *identifier*."""
        if DeltaTable is None:
            raise RuntimeError(
                "delta-spark is not installed. Install it to use resolve_delta_table."
            )
        if _is_uc_name(identifier):
            return DeltaTable.forName(spark, identifier)
        return DeltaTable.forPath(spark, identifier)


class MockPlatformAdapter(PlatformAdapter):
    """In-process mock adapter for unit testing — no platform runtime required.

    Records all calls for assertion in tests. Returns configurable results per
    notebook name; falls back to a default success payload for unregistered names.

    Args:
        notebook_results: Maps notebook name → ``ExecutionResult.to_json()`` string.
            Unregistered names receive the default success payload.
        current_nb_name: Value returned by ``get_current_notebook_name()``.
        raise_on_delete: When True, ``delete_path`` raises ``FileNotFoundError``.

    Example::

        from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
        from spark_quick_start.orchestration.models import ExecutionResult, ExecutionMetrics
        from dataclasses import asdict

        adapter = MockPlatformAdapter(
            notebook_results={
                "NB_Stage_Person": ExecutionResult(
                    status="success", has_error=False,
                    metrics=asdict(ExecutionMetrics(source_count=5, target_count=5))
                ).to_json()
            }
        )
        # After execution:
        assert adapter.run_calls[0][0] == "NB_Stage_Person"
    """

    def __init__(
        self,
        notebook_results: dict[str, str] | None = None,
        current_nb_name: str | None = "TestNotebook",
        raise_on_delete: bool = False,
        delta_read_result: Any | None = None,
        existing_tables: set | None = None,
        non_empty_paths: set[str] | None = None,
        table_properties: dict[str, dict[str, str]] | None = None,
    ) -> None:
        self._results: dict[str, str] = notebook_results or {}
        self._current_nb_name = current_nb_name
        self._raise_on_delete = raise_on_delete
        self._delta_read_result = delta_read_result
        self._existing_tables: set = existing_tables or set()
        self._non_empty_paths: set[str] = non_empty_paths or set()
        self.path_has_content_calls: list[str] = []
        self.run_calls: list[tuple[str, int, dict]] = []
        self.deleted_paths: list[str] = []
        self.read_delta_calls: list[str] = []
        self.read_parquet_calls: list[str] = []
        self.read_table_calls: list[str] = []
        self.read_warehouse_view_calls: list[tuple[str, str | None, str | None]] = []
        self.write_delta_overwrite_calls: list[tuple[str, bool]] = []
        self.write_delta_append_calls: list[str] = []
        self.write_delta_create_calls: list[str] = []
        self.delta_table_exists_calls: list[str] = []
        self.resolve_delta_table_calls: list[str] = []
        self.drop_delta_calls: list[str] = []
        self._table_properties: dict[str, dict[str, str]] = {
            k: dict(v) for k, v in (table_properties or {}).items()}
        self.get_table_properties_calls: list[str] = []
        self.set_table_properties_calls: list[tuple[str, dict[str, str]]] = []

    def _default_success(self) -> str:
        # Import here to avoid a circular dependency at module load time.
        # MockPlatformAdapter lives in adapters/; models lives in orchestration/.
        # The lazy import is intentional and documented.
        from spark_quick_start.orchestration.models import ExecutionMetrics, ExecutionResult
        return ExecutionResult(
            status="success",
            has_error=False,
            metrics=asdict(ExecutionMetrics(source_count=10, target_count=10, duration_seconds=0.1)),
        ).to_json()

    def run_notebook(self, name: str, timeout: int, args: dict[str, Any]) -> str:
        self.run_calls.append((name, timeout, args))
        return self._results.get(name, self._default_success())

    def run_notebooks_parallel(self, dag: dict[str, Any]) -> dict[str, Any]:
        results = {}
        for act in dag.get("activities", []):
            exit_val = self.run_notebook(
                act["path"],
                act.get("timeoutPerCellInSeconds", 90),
                act.get("args", {}),
            )
            results[act["name"]] = {"exitVal": exit_val}
        return results

    def delete_path(self, path: str) -> None:
        if self._raise_on_delete:
            raise FileNotFoundError(f"Path not found: {path}")
        self.deleted_paths.append(path)

    def get_current_notebook_name(self) -> str | None:
        return self._current_nb_name

    def read_delta(self, spark: SparkSession, identifier: str) -> Any:
        from unittest.mock import MagicMock  # deferred — test-only dependency
        self.read_delta_calls.append(identifier)
        if self._delta_read_result is not None:
            return self._delta_read_result
        return MagicMock()

    def read_parquet(self, spark: SparkSession, identifier: str) -> Any:
        from unittest.mock import MagicMock  # deferred — test-only dependency
        self.read_parquet_calls.append(identifier)
        return self._delta_read_result if self._delta_read_result is not None else MagicMock()

    def read_table(self, spark: SparkSession, identifier: str) -> Any:
        from unittest.mock import MagicMock  # deferred — test-only dependency
        self.read_table_calls.append(identifier)
        return self._delta_read_result if self._delta_read_result is not None else MagicMock()

    def read_warehouse_view(
        self,
        spark: SparkSession,
        identifier: str,
        *,
        workspace_id: str | None = None,
        lakehouse_id: str | None = None,
    ) -> Any:
        from unittest.mock import MagicMock  # deferred — test-only dependency
        self.read_warehouse_view_calls.append((identifier, workspace_id, lakehouse_id))
        return self._delta_read_result if self._delta_read_result is not None else MagicMock()

    def write_delta_overwrite(
        self,
        df: Any,
        identifier: str,
        overwrite_schema: bool = False,
    ) -> None:
        self.write_delta_overwrite_calls.append((identifier, overwrite_schema))

    def write_delta_create(self, df: Any, identifier: str) -> None:
        # Refuses what errorifexists refuses: anything the mock knows is there.
        if identifier in self._existing_tables or identifier in self._table_properties:
            raise RuntimeError(f"MockPlatformAdapter: {identifier!r} already exists")
        self.write_delta_create_calls.append(identifier)
        self._existing_tables.add(identifier)

    def write_delta_append(self, df: Any, identifier: str) -> None:
        self.write_delta_append_calls.append(identifier)

    def delta_table_exists(self, spark: SparkSession, identifier: str) -> bool:
        self.delta_table_exists_calls.append(identifier)
        return identifier in self._existing_tables

    def drop_delta(self, spark: SparkSession, identifier: str) -> None:
        self.drop_delta_calls.append(identifier)

    def path_has_content(self, spark: SparkSession, path: str) -> bool:
        self.path_has_content_calls.append(path)
        return path in self._non_empty_paths

    def get_table_properties(self, spark: SparkSession, identifier: str) -> dict[str, str]:
        _table_reference(identifier)  # the same identifier refusals as the default
        self.get_table_properties_calls.append(identifier)
        # The default raises on a missing table, as DESCRIBE DETAIL does; so does the mock, for
        # an identifier in neither existing_tables nor table_properties.
        if identifier not in self._table_properties and identifier not in self._existing_tables:
            raise RuntimeError(f"MockPlatformAdapter: no Delta table at {identifier!r}")
        return dict(self._table_properties.get(identifier, {}))

    def set_table_properties(
        self, spark: SparkSession, identifier: str, properties: Mapping[str, str]
    ) -> None:
        _table_reference(identifier)  # the same refusals as the default, before recording
        _property_literals(properties)
        self.set_table_properties_calls.append((identifier, dict(properties)))
        self._table_properties.setdefault(identifier, {}).update(properties)

    def resolve_delta_table(self, spark: SparkSession, identifier: str) -> Any:
        from unittest.mock import MagicMock  # deferred — test-only dependency
        self.resolve_delta_table_calls.append(identifier)
        return MagicMock()
