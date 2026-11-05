"""DQDTransformer and dqd_context (F139): one source, per-row filters, transformations and selection.

The design: docs/architecture/decisions/2026-09-29-declarative-query-definition-design.md.
"""
from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from spark_quick_start.adapters import MockPlatformAdapter
from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.orchestration import DQDTransformer
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.dqd import DQDDefinitionError
from spark_quick_start.orchestration.models import (
    DQDContext,
    GlobalContext,
    HWMContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, SourceTransformer
from spark_quick_start.orchestration.transformers.protocols import HWMProvider

_DEFINITION = {
    "source_kind": "delta",
    "source_identifier": "bronze.crm.customers",
    "filters": [{"operator": "=", "left": {"column": "STATUS"}, "right": {"literal": "active"}}],
    "transformations": [{"name": "full_name", "expression": {"function": "concat_ws", "arguments": [
        {"literal": " "}, {"column": "FIRST"}, {"column": "LAST"}]}}],
    "selection": [
        {"name": "customer_id", "expression": {"column": "CUST_ID"}},
        {"name": "full_name"},
        {"name": "birth_date", "expression": {"cast": {"expression": {"column": "DOB"}, "data_type": "date"}}},
    ],
}

# --- the context section ---------------------------------------------------------------------


class TestDQDContext:
    def test_the_documented_shape_is_valid(self):
        DQDContext(_DEFINITION).validate()
        DQDContext({"source_kind": "parquet", "source_identifier": "/lake/x", "source_columns": ["a"]}).validate()

    @pytest.mark.parametrize("change, message", [
        ({"source_kind": "sql"}, "source_kind 'sql'"),
        ({"source_identifier": ""}, "source_identifier"),
        ({"source_identifier": None}, "source_identifier"),
        ({"source_columns": "CUST_ID"}, "source_columns"),
        ({"source_columns": ["CUST_ID", ""]}, "source_columns"),
        ({"filters": {"operator": "="}}, "filters"),
        ({"transformations": [{"expression": {"literal": 1}}]}, r"transformations\[0\].*'name'"),
        ({"transformations": [{"name": "x"}]}, r"transformations\[0\].*'expression'"),
        ({"transformations": [{"name": "x", "expression": {"literal": 1}, "as": "y"}]}, "unknown key 'as'"),
        ({"selection": [{"name": "a"}, {"name": "a"}]}, "'a' more than once"),
        ({"selection": [{"expression": {"column": "a"}}]}, r"selection\[0\].*'name'"),
        ({"tables": []}, "unknown key 'tables'"),
    ])
    def test_a_malformed_section_is_refused(self, change, message):
        with pytest.raises(ValueError, match=message):
            DQDContext({**_DEFINITION, **change}).validate()

    def test_the_eager_flag_defaults_to_false_and_accepts_a_bool(self):
        assert DQDContext(_DEFINITION).eager_materialize_source is False
        for value in (True, False):
            definition = DQDContext({**_DEFINITION, "eager_materialize_source": value})
            definition.validate()
            assert definition.eager_materialize_source is value

    @pytest.mark.parametrize("value", [0, 1, "true", "false", None, []])
    def test_a_non_bool_eager_flag_is_refused(self, value):
        with pytest.raises(ValueError, match="eager_materialize_source must be true or false"):
            DQDContext({**_DEFINITION, "eager_materialize_source": value}).validate()

    def test_a_missing_kind_is_refused(self):
        with pytest.raises(ValueError, match="source_kind"):
            DQDContext({"source_identifier": "x"}).validate()


def _load_context(**extra) -> str:
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "E", "source_system": "S", "target_table_path": "/t",
                           "staging_table_path": "/s"},
        "scd_context": {"business_key_column": "customer_id"},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "f", "log_file_dir": "/l"},
        **extra,
    })


class TestLoadContextManager:
    def test_a_dqd_load_needs_no_staging_notebooks(self):
        manager = LoadContextManager(_load_context(dqd_context=_DEFINITION)).parse("s", "e")
        manager.validate_all()
        assert manager.dqd_ctx.source_kind == "delta"

    def test_without_staging_or_a_source_definition_the_load_is_refused(self):
        manager = LoadContextManager(_load_context()).parse("s", "e")
        with pytest.raises(ValueError, match="dqd_context"):
            manager.validate_all()

    def test_a_section_that_is_not_an_object_is_refused(self):
        with pytest.raises(ValueError, match="dqd_context must be a JSON object"):
            LoadContextManager(_load_context(dqd_context=["x"])).parse("s", "e")

    def test_validation_checks_the_section(self):
        manager = LoadContextManager(_load_context(dqd_context={"source_kind": "sql"})).parse("s", "e")
        with pytest.raises(ValueError, match="source_kind"):
            manager.validate_all()

    def test_a_watermarked_dqd_refuses_a_full_load(self):
        context = json.loads(_load_context(dqd_context=_DEFINITION, hwm_context={"hwm_column_name": "M"}))
        context["scd_context"]["load_type"] = "Full"
        manager = LoadContextManager(json.dumps(context)).parse("s", "e")
        with pytest.raises(ValueError, match="load_type 'Full'.*hwm_context"):
            manager.validate_all()

    def test_a_dqd_beside_stages_without_a_schema_is_refused_and_told_how_out(self):
        manager = LoadContextManager(_load_context(
            dqd_context=_DEFINITION, staging_context={"stages": [{"nb_name": "NB"}]})).parse("s", "e")
        with pytest.raises(ValueError, match=r"staging_table_schema.*remove staging_context\.stages if the source "
                                             r"is the dqd_context"):
            manager.validate_all()

    def test_a_dqd_beside_staging_stages_is_warned_about(self, caplog):
        # Stages need a staging_table_schema (F107), DQD or not.
        global_context = {"id": "ent_1", "entity_name": "E", "source_system": "S", "target_table_path": "/t",
                          "staging_table_path": "/s",
                          "staging_table_schema": {"type": "struct", "fields": [
                              {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}}]}}
        manager = LoadContextManager(_load_context(
            dqd_context=_DEFINITION, staging_context={"stages": [{"nb_name": "NB"}]},
            global_context=global_context)).parse("s", "e")
        with caplog.at_level("WARNING"):
            manager.validate_all()
        assert "dqd_context is set alongside staging_context.stages" in caplog.text


# --- the adapter methods ---------------------------------------------------------------------


def test_the_new_read_methods_have_portable_defaults():
    for name in ("read_parquet", "read_table"):
        assert name in vars(PlatformAdapter) and not getattr(getattr(PlatformAdapter, name), "__isabstractmethod__", False)


def test_the_mock_records_the_new_reads():
    adapter = MockPlatformAdapter(delta_read_result="frame")
    assert adapter.read_parquet(None, "/p") == "frame" and adapter.read_table(None, "c.s.v") == "frame"
    assert adapter.read_parquet_calls == ["/p"] and adapter.read_table_calls == ["c.s.v"]


# --- the transformer -------------------------------------------------------------------------


def _ctx(definition=None, hwm=None, scd=None) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S", "target_table_path": "/t",
                                  "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext(scd or {"business_key_column": "customer_id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
        hwm_ctx=None if hwm is None else HWMContext(hwm),
        dqd_ctx=None if definition is None else DQDContext(definition),
    )


@pytest.fixture(scope="module")
def customers(spark):
    return spark.createDataFrame(
        [(1, "Ada", "Lovelace", "1815-12-10", "active", "2026-09-01 10:00:00"),
         (2, "Grace", "Hopper", "1906-12-09", "inactive", "2026-09-02 10:00:00"),
         (3, "Alan", "Turing", "1912-06-23", "active", "2026-09-03 10:00:00")],
        "CUST_ID int, FIRST string, LAST string, DOB string, STATUS string, MODIFIED_AT string",
    ).selectExpr("*", "timestamp(MODIFIED_AT) AS MODIFIED_TS").drop("MODIFIED_AT")


def _produce(spark, frame, ctx, transformer=None):
    transformer = transformer or DQDTransformer()
    transformer._spark = spark
    transformer._adapter = MockPlatformAdapter(delta_read_result=frame)
    return transformer, transformer.produce(ctx).df


def test_it_is_a_source_step_and_a_watermark_provider():
    transformer = DQDTransformer()
    assert isinstance(transformer, SourceTransformer) and isinstance(transformer, HWMProvider)
    assert (transformer.hwm_before, transformer.hwm_after, transformer.hwm_column) == (None, None, None)


def test_filters_transformations_and_selection_apply_in_order(spark, customers):
    transformer, out = _produce(spark, customers, _ctx(_DEFINITION))
    assert transformer._adapter.read_delta_calls == ["bronze.crm.customers"]
    assert out.columns == ["customer_id", "full_name", "birth_date"]
    rows = sorted(tuple(r) for r in out.collect())
    assert [(r[0], r[1], str(r[2])) for r in rows] == [(1, "Ada Lovelace", "1815-12-10"), (3, "Alan Turing", "1912-06-23")]


def test_without_a_selection_every_source_column_and_transformation_passes(spark, customers):
    definition = {"source_kind": "delta", "source_identifier": "t", "source_columns": ["CUST_ID", "FIRST"],
                  "transformations": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    _, out = _produce(spark, customers, _ctx(definition))
    assert out.columns == ["CUST_ID", "FIRST", "customer_id"]


def test_a_definition_given_to_the_constructor_wins(spark, customers):
    definition = {"source_kind": "delta", "source_identifier": "given",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    transformer, out = _produce(spark, customers, _ctx(_DEFINITION), DQDTransformer(dqd_definition=definition))
    assert transformer._adapter.read_delta_calls == ["given"] and out.count() == 3


def test_a_load_with_no_definition_is_refused(spark, customers):
    with pytest.raises(ValueError, match="dqd_context"):
        _produce(spark, customers, _ctx())


def test_an_error_names_the_entry(spark, customers):
    definition = {**_DEFINITION, "selection": [{"name": "customer_id", "expression": {"column": "NOPE"}}]}
    with pytest.raises(DQDDefinitionError, match=r"dqd_context\.selection\[0\]\.expression.*'NOPE'"):
        _produce(spark, customers, _ctx(definition))


def test_dqf_keys_are_refused_before_the_source_is_read(spark, customers):
    definition = {**_DEFINITION, "selection": [{"name": "customer_id", "expression": {"col": "CUST_ID"}}]}
    transformer = DQDTransformer()
    with pytest.raises(DQDDefinitionError, match="'col'.*'column'"):
        _produce(spark, customers, _ctx(definition), transformer)
    assert transformer._adapter.read_delta_calls == []


def test_the_watermark_filters_the_source_and_is_reported(spark, customers):
    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    hwm = {"hwm_column_name": "MODIFIED_TS", "hwm_value": "2026-09-01 12:00:00"}
    transformer, out = _produce(spark, customers, _ctx(definition, hwm))
    assert sorted(r.customer_id for r in out.collect()) == [2, 3]
    assert (transformer.hwm_column, transformer.hwm_before, transformer.hwm_after) == \
        ("MODIFIED_TS", "2026-09-01 12:00:00", "2026-09-03 10:00:00")


def test_with_nothing_new_the_watermark_stays(spark, customers):
    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    transformer, out = _produce(spark, customers, _ctx(definition, {"hwm_column_name": "modified_ts",
                                                                    "hwm_value": "2026-09-09 00:00:00"}))
    assert out.count() == 0 and transformer.hwm_after == transformer.hwm_before == "2026-09-09 00:00:00"


def _portable_adapter() -> PlatformAdapter:
    """The base class's own defaults, with its abstract methods stubbed. Not LocalAdapter: the
    platform-filtered builds delete local_adapter.py and still run this module (F140 review)."""
    stubs = {name: (lambda self, *a, **k: None) for name in PlatformAdapter.__abstractmethods__}
    return type("_PortableAdapter", (PlatformAdapter,), stubs)()


def test_parquet_and_catalog_views_are_read_through_the_adapter(spark, customers, tmp_path):
    path = str(tmp_path / "customers")
    customers.write.parquet(path)
    customers.createOrReplaceTempView("dqd_customers_view")
    for kind, identifier in (("parquet", path), ("view", "dqd_customers_view")):
        definition = {"source_kind": kind, "source_identifier": identifier,
                      "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
        transformer = DQDTransformer()
        transformer._spark = spark
        transformer._adapter = _portable_adapter()
        assert transformer.produce(_ctx(definition)).df.count() == 3, kind


@pytest.fixture(params=["false", "true"], ids=["ansi_off", "ansi_on"])
def ansi(spark, request):
    """Run the test with `spark.sql.ansi.enabled` set: off is Spark 3's default, on Spark 4's (F165)."""
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield request.param
    spark.conf.set("spark.sql.ansi.enabled", previous)


def test_a_malformed_watermark_is_refused_not_matched_against_nothing(spark, customers, ansi):
    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    with pytest.raises(ValueError, match=r"hwm_value '2026-13-01' cannot be read as the type of hwm_column_name"):
        _produce(spark, customers, _ctx(definition, {"hwm_column_name": "MODIFIED_TS", "hwm_value": "2026-13-01"}))


@pytest.mark.parametrize("value", ["it's", "a\\b", "'; DROP TABLE t; --"])
def test_a_watermark_with_a_quote_or_backslash_is_refused_not_read_as_sql(spark, customers, ansi, value):
    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    with pytest.raises(ValueError, match=r"cannot be read as the type of hwm_column_name"):
        _produce(spark, customers, _ctx(definition, {"hwm_column_name": "MODIFIED_TS", "hwm_value": value}))


_WATERMARK_COLUMNS = {
    "date": ["2026-09-01", "2026-09-02", "2026-09-03"],
    "int": ["1", "2", "3"],
    "bigint": ["10000000000", "10000000001", "10000000002"],
    "decimal(10,2)": ["1.00", "2.50", "3.00"],
    "string": ["a", "b", "c"],
}


@pytest.mark.parametrize("sql_type, value, kept", [
    ("date", "2026-09-01", 2),
    ("int", "1", 2), ("int", 1, 2), ("int", 1.0, 2),
    ("bigint", "10000000000", 2), ("bigint", 10000000000, 2),
    ("decimal(10,2)", "1.00", 2), ("decimal(10,2)", 1.5, 2), ("decimal(10,2)", 1, 2),
    ("string", "a", 2),
])
def test_a_watermark_of_each_column_type_filters_in_both_ansi_modes(spark, ansi, sql_type, value, kept):
    """A numeric `hwm_value` keeps its numeric meaning (`5.0` is 5 in an int column), as `F.lit` gave it (F165)."""
    frame = spark.createDataFrame([(v,) for v in _WATERMARK_COLUMNS[sql_type]], "w string") \
        .select(F.col("w").cast(sql_type).alias("w"))
    definition = {"source_kind": "delta", "source_identifier": "t", "selection": [{"name": "customer_id", "expression": {"column": "w"}}]}
    transformer, out = _produce(spark, frame, _ctx(definition, {"hwm_column_name": "w", "hwm_value": value}))
    assert out.count() == kept
    assert transformer.source_count == kept and transformer.hwm_after == str(
        frame.agg(F.max("w").cast("string")).collect()[0][0])


def test_an_epoch_number_is_a_watermark_for_a_timestamp_column(spark, ansi):
    frame = spark.createDataFrame([(1,), (1700000001,), (1700000002,)], "e long") \
        .select(F.col("e").cast("timestamp").alias("w"))
    definition = {"source_kind": "delta", "source_identifier": "t", "selection": [{"name": "customer_id", "expression": {"column": "w"}}]}
    _, out = _produce(spark, frame, _ctx(definition, {"hwm_column_name": "w", "hwm_value": 1700000000.0}))
    assert out.count() == 2


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_watermark_is_refused(spark, customers, ansi, value):
    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    with pytest.raises(ValueError, match=r"hwm_value .* cannot be read as the type of hwm_column_name 'CUST_ID': "
                                         r"it is not a finite number"):
        _produce(spark, customers, _ctx(definition, {"hwm_column_name": "CUST_ID", "hwm_value": value}))


def test_a_malformed_watermark_is_refused_with_the_eager_flag_too(spark, customers, ansi):
    definition = {"source_kind": "delta", "source_identifier": "t", "eager_materialize_source": True,
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    with pytest.raises(ValueError, match=r"hwm_value '2026-13-01' cannot be read as the type of hwm_column_name"):
        _produce(spark, customers, _ctx(definition, {"hwm_column_name": "MODIFIED_TS", "hwm_value": "2026-13-01"}))


def test_a_valid_watermark_filters_and_reports_with_the_eager_flag(spark, customers, ansi):
    class _Adapter(MockPlatformAdapter):
        def read_delta(self, spark, identifier):  # the scratch table, read back: what was written
            return self._scratch

        def write_delta_overwrite(self, df, path, *args, **kwargs):
            self._scratch = df  # stands in for the write and read-back; the plan is what is under test
            return super().write_delta_overwrite(df, path, *args, **kwargs)

    adapter = _Adapter(delta_read_result=customers)
    definition = {"source_kind": "view", "source_identifier": "t", "eager_materialize_source": True,
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    transformer = DQDTransformer()
    transformer._spark, transformer._adapter = spark, adapter
    df = transformer.produce(_ctx(definition, {"hwm_column_name": "MODIFIED_TS", "hwm_value": "2026-09-01 12:00:00"})).df
    assert sorted(r.customer_id for r in df.collect()) == [2, 3]
    assert transformer.hwm_after == "2026-09-03 10:00:00" and transformer.source_count == 2


def test_a_struct_field_read_in_source_columns_is_found_by_its_whole_name(spark):
    frame = spark.createDataFrame([(1, ("Kyiv", "UA")), (2, ("Oslo", "NO"))],
                                  "customer_id int, address struct<city:string,country:string>")
    definition = {"source_kind": "delta", "source_identifier": "t", "source_columns": ["customer_id", "address.city"],
                  "filters": [{"operator": "=", "left": {"column": "address.city"}, "right": {"literal": "Kyiv"}}],
                  "selection": [{"name": "customer_id"}, {"name": "city", "expression": {"column": "address.city"}}]}
    _, out = _produce(spark, frame, _ctx(definition))
    assert [tuple(r) for r in out.collect()] == [(1, "Kyiv")]



# --- F140: Fabric Warehouse views -----------------------------------------------------------


def test_a_warehouse_view_is_refused_where_the_platform_has_none():
    with pytest.raises(NotImplementedError, match="Fabric.*'view'"):
        _portable_adapter().read_warehouse_view(None, "wh.dbo.v")


def test_a_warehouse_view_definition_is_valid_and_read_through_the_adapter(spark, customers):
    definition = {"source_kind": "warehouse_view", "source_identifier": "wh.dbo.v_customer",
                  "workspace_id": "ws", "lakehouse_id": "wh",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    DQDContext(definition).validate()
    transformer = DQDTransformer()
    transformer._spark = spark
    transformer._adapter = MockPlatformAdapter(delta_read_result=customers)
    assert transformer.produce(_ctx(definition)).df.count() == 3
    assert transformer._adapter.read_warehouse_view_calls == [("wh.dbo.v_customer", "ws", "wh")]


def test_the_lakehouse_ids_belong_to_a_warehouse_view_only():
    with pytest.raises(ValueError, match="lakehouse_id.*warehouse_view"):
        DQDContext({**_DEFINITION, "lakehouse_id": "wh"}).validate()


# --- F144: the source count, from the watermark's aggregation ---------------------------------


def test_with_a_watermark_it_reports_the_rows_it_passed_on(spark, customers):
    from spark_quick_start.orchestration.transformers.protocols import SourceMetricsProvider

    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    transformer, _ = _produce(spark, customers, _ctx(definition, {"hwm_column_name": "MODIFIED_TS",
                                                                  "hwm_value": "2026-09-01 12:00:00"}))
    assert isinstance(transformer, SourceMetricsProvider)
    assert (transformer.source_count, transformer.target_count) == (2, 2)


def test_without_a_watermark_the_count_is_not_computed(spark, customers):
    definition = {"source_kind": "delta", "source_identifier": "t",
                  "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}]}
    transformer, _ = _produce(spark, customers, _ctx(definition))
    assert (transformer.source_count, transformer.target_count) == (None, None)



@pytest.mark.parametrize("path", [None, "", "  "])
def test_the_eager_flag_without_a_staging_table_path_is_refused_before_any_spark_work(spark, customers, path):
    definition = {"source_kind": "delta", "source_identifier": "t", "eager_materialize_source": True}
    ctx = _ctx(definition)
    ctx.global_ctx.staging_table_path = path
    adapter = MockPlatformAdapter(delta_read_result=customers)
    transformer = DQDTransformer()
    transformer._spark, transformer._adapter = spark, adapter
    with pytest.raises(ValueError, match=r"eager_materialize_source.*staging_table_path"):
        transformer.produce(ctx)
    assert adapter.read_delta_calls == [] and adapter.drop_delta_calls == []


def _eager(**change):
    return {"source_kind": "view", "source_identifier": "t", "eager_materialize_source": True, **change}


@pytest.mark.parametrize("section, entries, where", [
    ("selection", [{"name": "a", "expression": {"column": "CUST_ID"}}, {"name": "FIRST"},
                   {"name": "f", "expression": {"column": "_metadata.file_path"}}], r"dqd_context\.selection\[2\]"),
    ("selection", [{"name": "_metadata"}], r"dqd_context\.selection\[0\]"),
    ("transformations", [{"name": "f", "expression": {"function": "upper", "arguments": [
        {"column": "_metadata.file_name"}]}}], r"dqd_context\.transformations\[0\]"),
])
def test_the_eager_flag_refuses_a_metadata_reference_before_any_spark_work(spark, customers, section, entries, where):
    adapter = MockPlatformAdapter(delta_read_result=customers)
    transformer = DQDTransformer()
    transformer._spark, transformer._adapter = spark, adapter
    with pytest.raises(ValueError, match=rf"{where}.*_metadata.*scratch"):
        transformer.produce(_ctx(_eager(**{section: entries})))
    assert adapter.read_table_calls == [] and adapter.read_delta_calls == [] and adapter.drop_delta_calls == []


def test_the_eager_flag_refuses_a_source_that_is_the_scratch_table(spark, customers):
    adapter = MockPlatformAdapter(delta_read_result=customers)
    transformer = DQDTransformer()
    transformer._spark, transformer._adapter = spark, adapter
    with pytest.raises(ValueError, match=r"source_identifier.*/s_ent_1_0_dqd"):
        transformer.produce(_ctx(_eager(source_kind="delta", source_identifier="/s_ent_1_0_dqd")))
    assert adapter.read_delta_calls == [] and adapter.drop_delta_calls == []


def test_the_eager_flag_drops_writes_and_continues_from_the_read_back(spark, customers):
    class _Adapter(MockPlatformAdapter):
        def read_delta(self, spark, identifier):  # the scratch table: positional names, other rows
            super().read_delta(spark, identifier)
            return spark.createDataFrame([(9,)], "_c0 int")

    adapter = _Adapter(delta_read_result=customers.select("CUST_ID"))
    transformer = DQDTransformer()
    transformer._spark, transformer._adapter = spark, adapter
    df = transformer.produce(_ctx(_eager(selection=[
        {"name": "customer_id", "expression": {"column": "CUST_ID"}}]))).df
    assert adapter.read_table_calls == ["t"]
    assert adapter.drop_delta_calls == ["/s_ent_1_0_dqd"]
    assert adapter.write_delta_overwrite_calls == [("/s_ent_1_0_dqd", False)]
    assert adapter.read_delta_calls == ["/s_ent_1_0_dqd"]
    assert [r.customer_id for r in df.collect()] == [9]
