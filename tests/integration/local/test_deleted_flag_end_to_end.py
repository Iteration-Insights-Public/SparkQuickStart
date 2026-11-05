"""End to end: the staged deleted flag through StagingTransformer, the detector and the merge
writer, over a first load, an SCD2 change and a Full load's delete (F127).

The owner's decisions (2026-09-29): the staged `scdDeletedFlag` is optional, and the detector
adds "N" only when it is missing; when present and `source_deleted_flag_column` is unset it is
the delete signal; any value other than `deleted_flag_value`, NULL included, means not deleted;
an explicit `source_deleted_flag_column` naming another column wins, and the staged flag is
dropped. The output always has one flag column, Y or N. Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import default_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_materialize_scratch_integration."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


# Each configuration: the staged columns beyond (customer_id, name), and the scd_context keys.
_CONFIGS = {
    "no flag column": ([], {}),
    "a staged flag, no explicit column": ([("scdDeletedFlag", "string")], {}),
    "today's configuration": ([("scdDeletedFlag", "string")], {"source_deleted_flag_column": "scdDeletedFlag"}),
    "an explicit other column": ([("scdDeletedFlag", "string"), ("is_deleted", "string")],
                                 {"source_deleted_flag_column": "is_deleted"}),
}


def _context(root, extra_columns, scd, load_type):
    fields = [("customer_id", "integer"), ("name", "string"), *extra_columns]
    return json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": n, "type": t, "nullable": True, "metadata": {}} for n, t in fields]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "load_type": load_type, **scd},
        "runtime_context": {"correlation_id": "f127", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


def _current(spark, root):
    rows = spark.read.format("delta").load(f"{root}/silver/customer") \
        .filter("scdCurrentFlag = 'Y'").select("customer_id", "name", "scdVersion", "scdDeletedFlag").collect()
    return sorted(tuple(r) for r in rows)


@pytest.mark.parametrize("config", sorted(_CONFIGS))
def test_the_deleted_flag_through_a_whole_load(spark, tmp_path, config):
    extra, scd = _CONFIGS[config]
    names = ["customer_id", "name", *(n for n, _ in extra)]
    all_rows = lambda: spark.read.format("delta").load(f"{tmp_path}/silver/customer").count()  # noqa: E731
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        schema = ", ".join(f"{n} {'int' if n == 'customer_id' else 'string'}" for n in names)
        return spark.createDataFrame(staged["rows"], schema)

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def load(rows, load_type="Incremental"):
        staged["rows"] = rows
        result = default_pipeline(_context(tmp_path, extra, scd, load_type), spark, adapter=adapter).run()
        assert result.status == "success", result.error_details
        load.result = result
        return _current(spark, tmp_path)

    def row(key, name, flag):
        # (customer_id, name, then the staged flag columns): the signal is `flag`, and a staged
        # scdDeletedFlag beside an explicit column says the opposite, to prove it is ignored.
        if not extra:
            return (key, name)
        if config == "an explicit other column":
            return (key, name, "N" if flag == "Y" else "Y", flag)
        return (key, name, flag)

    # First load: 1 and 2 not deleted, 3 flagged deleted, 4 with a NULL flag.
    first = load([row(1, "Ada", "N"), row(2, "Grace", "N"), row(3, "Alan", "Y"), row(4, "Edsger", None)])
    loaded = {1, 2, 3, 4} if not extra else {1, 2, 4}
    assert {r[0] for r in first} == loaded
    assert {r[3] for r in first} == {"N"}

    # An SCD2 change to 1.
    second = load([row(1, "Ada L.", "N"), row(2, "Grace", "N"), row(4, "Edsger", None)])
    assert (1, "Ada L.", 2, "N") in second

    # A Full load without 2: it is deleted; with a staged signal, 4 flagged Y is deleted too.
    full = [row(1, "Ada L.", "N")] + ([] if not extra else [row(4, "Edsger", "Y")]) + \
        ([(3, "Alan")] if not extra else [])
    third = load(full, load_type="Full")
    current = {r[0]: r for r in third}
    assert current[1][3] == "N"
    assert current[2][3] == "Y"
    if extra:
        assert current[4][3] == "Y"

    # F142: a fourth load repeats the delete. Nothing is written and the load halts early.
    if extra:
        repeat = [row(1, "Ada L.", "N"), row(4, "Edsger", "Y")]
        deleted_key = 4
    else:
        repeat = [row(1, "Ada L.", "N"), (3, "Alan")]  # 3 stays live, as in the third load
        deleted_key = 2
    # The only candidates are repeat deletes (2, and 4 when flagged), so the guard halts the load.
    total = all_rows()
    fourth = load(repeat, load_type="Full")
    assert fourth == third
    assert all_rows() == total
    assert load.result.early_exit is True

    # A fifth load brings the deleted key back not deleted: the next version, flag "N".
    deleted_version = {r[0]: r[2] for r in third}[deleted_key]
    back = [row(1, "Ada L.", "N"), row(deleted_key, "Back", "N")]
    fifth = {r[0]: r for r in load(back, load_type="Full")}
    assert fifth[deleted_key][2] == deleted_version + 1
    assert fifth[deleted_key][3] == "N"
