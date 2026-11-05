"""F168 end to end: two hubs, the link between them and a link satellite, from one DQD source.

Order lines connect orders and products. Each run loads both hubs, then the link, then the link
satellite keyed on the link hash key: a relationship is inserted once, a second source adding
one relationship and repeating another, a NULL hub key refused before the link exists. Unique
keys are the source's business logic (the owner, 2026-10-01): each hub reads a source at its
own key's grain, and the order lines hold each order and product pair once. Requires a working
Delta install.
"""
from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import dv_hub_pipeline, dv_link_pipeline, dv_satellite_pipeline


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _norm(column: str) -> dict:
    return {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"column": column}]}]}


def _hash(name: str, *columns: str) -> dict:
    joined = _norm(columns[0]) if len(columns) == 1 else {
        "function": "concat_ws", "arguments": [{"literal": "||"}, *(_norm(c) for c in columns)]}
    return {"name": name, "expression": {"function": "sha2", "arguments": [joined, {"literal": 256}]}}


_KEYS = [_hash("OrderHashKey", "ORDER_ID"), _hash("ProductHashKey", "PRODUCT_ID"),
         _hash("LinkHashKey", "ORDER_ID", "PRODUCT_ID")]


def _context(root, source, table, scd, selection, bronze):
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "OrderLine", "source_system": source,
                           "target_table_path": f"{root}/vault/{table}", "staging_table_path": f"{root}/unused"},
        "scd_context": scd,
        "runtime_context": {"correlation_id": f"run-{source}", "pipeline_name": table, "application_name": "vault"},
        "logging_context": {"logging_level": "INFO", "log_file_name": table, "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "delta", "source_identifier": f"{root}/bronze/{source}/{bronze}",
                        "transformations": _KEYS, "selection": selection},
    })


_LINK_SCD = {"scd_type": "dv_link", "business_key_column": "LinkHashKey",
             "link_hub_key_columns": ["OrderHashKey", "ProductHashKey"]}
_LINK_SELECTION = [{"name": "LinkHashKey"}, {"name": "OrderHashKey"}, {"name": "ProductHashKey"}]


def _land(spark, root, source, lines):
    """Land the order lines, and one source per hub at its key's grain, as business logic would."""
    schema = "ORDER_ID string, PRODUCT_ID string, QTY int"
    df = spark.createDataFrame(lines, schema)
    for name, frame in (("lines", df),
                        ("orders", df.select("ORDER_ID").distinct().withColumn("PRODUCT_ID", F.lit(""))),
                        ("products", df.select("PRODUCT_ID").distinct().withColumn("ORDER_ID", F.lit("")))):
        frame.write.format("delta").mode("overwrite").save(f"{root}/bronze/{source}/{name}")


def _load(spark, root, source, lines):
    """Land the sources, then load both hubs, the link and its satellite."""
    _land(spark, root, source, lines)
    adapter = LocalAdapter()
    for hub, column, bronze in (("order_h", "OrderHashKey", "orders"), ("product_h", "ProductHashKey", "products")):
        r = dv_hub_pipeline(_context(root, source, hub, {"scd_type": "dv_hub", "business_key_column": column},
                                     [{"name": column}], bronze), spark, adapter=adapter).run()
        assert r.status == "success", r.error_details
    link = dv_link_pipeline(_context(root, source, "order_product_l", _LINK_SCD, _LINK_SELECTION, "lines"),
                            spark, adapter=adapter).run()
    if link.status != "success":
        return link, None
    satellite = dv_satellite_pipeline(_context(
        root, source, "order_product_s", {"scd_type": "dv_satellite", "business_key_column": "LinkHashKey"},
        [{"name": "LinkHashKey"}, {"name": "Qty", "expression": {"column": "QTY"}}], "lines"),
        spark, adapter=adapter).run()
    return link, satellite


def _table(spark, root, name):
    return spark.read.format("delta").load(f"{root}/vault/{name}")


def test_a_link_takes_each_relationship_once_across_runs_and_sources(spark, tmp_path):
    link, sat = _load(spark, tmp_path, "CRM", [("o1", "p1", 2), ("o1", "p2", 1)])
    assert link.status == "success", link.error_details
    assert (link.metrics["inserted_count"], link.metrics["updated_count"]) == (2, 0)
    assert sat.metrics["inserted_count"] == 2

    again, sat2 = _load(spark, tmp_path, "CRM", [("o1", "p1", 3), ("o1", "p2", 1)])
    assert again.metrics["inserted_count"] == 0
    assert sat2.metrics["inserted_count"] == 1, "the quantity of o1-p1 changed"

    erp, _ = _load(spark, tmp_path, "ERP", [(" o1 ", "p2", 1), ("o2", "p1", 5)])  # " o1 " is o1
    assert erp.metrics["inserted_count"] == 1

    rows = _table(spark, tmp_path, "order_product_l").collect()
    assert len(rows) == 3
    assert {"LinkHashKey", "OrderHashKey", "ProductHashKey", "RecordSource", "LoadTS", "LoadEndTS",
            "CorrelationId"} <= set(rows[0].asDict())
    hubs = {r.OrderHashKey for r in _table(spark, tmp_path, "order_h").collect()}
    assert {r.OrderHashKey for r in rows} <= hubs, "every link points at a hub row"


def test_a_null_hub_key_is_refused_before_the_link_exists(spark, tmp_path):
    _land(spark, tmp_path, "CRM", [("o1", None, 1)])
    ctx = _context(tmp_path, "CRM", "order_product_l", _LINK_SCD, _LINK_SELECTION, "lines")
    result = dv_link_pipeline(ctx, spark, adapter=LocalAdapter()).run()
    assert result.status == "error"
    assert "ProductHashKey" in result.error_details["error_message"]
    assert not (tmp_path / "vault" / "order_product_l").exists()


def test_a_pair_staged_twice_is_refused_as_the_business_logic_s_to_fix(spark, tmp_path):
    """Two lines for one order and product are one relationship twice: the source decides the
    grain, the library refuses rather than choose a row (the owner, 2026-10-01)."""
    _land(spark, tmp_path, "CRM", [("o1", "p1", 2), ("o1", "p1", 5)])
    ctx = _context(tmp_path, "CRM", "order_product_l", _LINK_SCD, _LINK_SELECTION, "lines")
    result = dv_link_pipeline(ctx, spark, adapter=LocalAdapter()).run()
    assert result.status == "error"
    assert "staged more than once" in result.error_details["error_message"]
