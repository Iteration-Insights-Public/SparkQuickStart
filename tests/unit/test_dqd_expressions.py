"""The DQD expression library (F137): parsing, function resolution, and parity with DQF.

A Declarative Query Definition carries over DQF's per-row expression language with its keys
renamed to the load context's style (docs/architecture/decisions/
2026-09-29-declarative-query-definition-design.md). The parity cases hold each expression to the
rows DQF's own resolver produced for it over the same DataFrame. DQF itself was removed (F146);
its results were pinned first, case by case, so the proof that the same input gives the same
column outlives the engine.
"""
from __future__ import annotations

import datetime
from datetime import date

import pytest

from spark_quick_start.orchestration.dqd import (
    FORBIDDEN_FUNCTIONS,
    NOT_ROW_FUNCTIONS,
    RAW_VALUE_POSITIONS,
    Cast,
    ColumnRef,
    Comparison,
    DQDDefinitionError,
    FunctionCall,
    Literal,
    Logical,
    Negation,
    NullCheck,
    parse_expression,
    parse_predicate,
    resolve_expression,
    resolve_function,
    resolve_predicate,
)

# --- parsing: no engine ----------------------------------------------------------------------


class TestParseExpression:
    def test_each_kind_parses_to_its_node(self):
        assert parse_expression({"column": "id"}) == ColumnRef("id")
        assert parse_expression({"literal": 3}) == Literal(3)
        assert parse_expression({"cast": {"expression": {"column": "d"}, "data_type": "date"}}) \
            == Cast(ColumnRef("d"), "date")
        assert parse_expression({"function": "upper", "arguments": [{"column": "n"}]}) \
            == FunctionCall("upper", (ColumnRef("n"),))
        assert parse_expression({"operator": "is_null", "left": {"column": "n"}}) \
            == NullCheck("is_null", ColumnRef("n"))

    def test_a_function_name_is_lower_cased_and_arguments_default_to_none(self):
        assert parse_expression({"function": "CURRENT_DATE"}) == FunctionCall("current_date", ())

    def test_a_literal_list_is_kept_as_a_tuple(self):
        assert parse_expression({"literal": [1, 2]}) == Literal((1, 2))

    @pytest.mark.parametrize("dqf_key, dqd_key", [
        ("col", "column"), ("lit", "literal"), ("fn", "function"), ("op", "operator"), ("expr", "expression"),
    ])
    def test_a_dqf_key_is_refused_with_its_dqd_name(self, dqf_key, dqd_key):
        with pytest.raises(DQDDefinitionError, match=rf"'{dqf_key}'.*'{dqd_key}'"):
            parse_expression({dqf_key: "x"})

    def test_dqf_cast_spelling_is_refused_with_its_dqd_name(self):
        with pytest.raises(DQDDefinitionError, match=r"'dataType'.*'data_type'"):
            parse_expression({"cast": {"expression": {"column": "d"}, "dataType": "date"}})

    def test_dqf_args_is_refused_with_its_dqd_name(self):
        with pytest.raises(DQDDefinitionError, match=r"'args'.*'arguments'"):
            parse_expression({"function": "upper", "args": [{"column": "n"}]})

    @pytest.mark.parametrize("bad, message", [
        ({}, "exactly one of"),
        ({"column": "a", "literal": 1}, "exactly one of"),
        ("id", "must be an object"),
        ({"column": ""}, "non-empty string"),
        ({"column": 3}, "non-empty string"),
        ({"column": "a", "extra": 1}, "unknown key 'extra'"),
        ({"cast": {"expression": {"column": "d"}}}, "'data_type'"),
        ({"cast": {"data_type": "date"}}, "'expression'"),
        ({"cast": {"expression": {"column": "d"}, "data_type": ""}}, "non-empty string"),
        ({"function": ""}, "non-empty string"),
        ({"function": "upper", "arguments": {"column": "n"}}, "must be a list"),
        ({"function": "when", "arguments": [{"literal": True}]}, "2 or 3 arguments"),
        ({"function": "when", "arguments": [{"literal": True}] * 4}, "2 or 3 arguments"),
        ({"function": "+", "arguments": [{"literal": 1}]}, "exactly 2 arguments"),
        ({"function": "=", "arguments": [{"literal": 1}] * 3}, "exactly 2 arguments"),
        ({"function": "cast", "arguments": [{"column": "d"}]}, "exactly 2 arguments"),
    ])
    def test_a_malformed_expression_is_refused(self, bad, message):
        with pytest.raises(DQDDefinitionError, match=message):
            parse_expression(bad)

    def test_the_error_names_where_it_is(self):
        with pytest.raises(DQDDefinitionError, match=r"selection\[2\]\.expression\.arguments\[1\]"):
            parse_expression({"function": "coalesce", "arguments": [{"column": "a"}, {"col": "b"}]},
                             path="selection[2].expression")

    def test_a_refused_function_is_refused_when_parsed(self):
        with pytest.raises(DQDDefinitionError, match="'reflect'"):
            parse_expression({"function": "reflect", "arguments": []})


class TestParsePredicate:
    def test_each_operator_parses_to_its_node(self):
        a, b = {"column": "a"}, {"literal": 1}
        assert parse_predicate({"operator": "=", "left": a, "right": b}) \
            == Comparison("=", ColumnRef("a"), Literal(1), null_safe=False)
        assert parse_predicate({"operator": "=", "left": a, "right": b, "null_safe": True}).null_safe is True
        both = {"operator": "and", "left": {"operator": "is_null", "left": a},
                "right": {"operator": "is_not_null", "left": a}}
        assert parse_predicate(both) == Logical("and", NullCheck("is_null", ColumnRef("a")),
                                                NullCheck("is_not_null", ColumnRef("a")))
        assert parse_predicate({"operator": "NOT", "expression": {"operator": "is_null", "left": a}}) \
            == Negation(NullCheck("is_null", ColumnRef("a")))

    @pytest.mark.parametrize("bad, message", [
        ({"operator": "between", "left": {"column": "a"}, "right": {"literal": 1}}, "unknown operator 'between'"),
        ({"operator": "=", "left": {"column": "a"}}, "'right'"),
        ({"operator": "is_null", "left": {"column": "a"}, "right": {"literal": 1}}, "unknown key 'right'"),
        ({"operator": "like", "left": {"column": "a"}, "right": {"column": "b"}}, "a literal string"),
        ({"operator": "starts_with", "left": {"column": "a"}, "right": {"literal": 1}}, "a literal string"),
        ({"operator": "not", "left": {"column": "a"}}, "'expression'"),
        ({"operator": "and", "left": {"column": "a"}, "right": {"column": "b"}}, "must be a predicate"),
        ({"operator": "in", "left": {"column": "a"}, "right": {"literal": 1}}, "not a single literal"),
        ({"operator": "!=", "left": {"column": "a"}, "right": {"literal": 1}, "null_safe": True},
         "unknown key 'null_safe'"),
        ({"operator": "=", "left": {"column": "a"}, "right": {"literal": 1}, "null_safe": "yes"}, "true or false"),
        ({"operator": "=", "left": {"column": "a"}, "right": {"literal": 1}, "null_safe": True, "x": 1},
         "unknown key 'x'"),
        ({"left": {"column": "a"}}, "'operator'"),
    ])
    def test_a_malformed_predicate_is_refused(self, bad, message):
        with pytest.raises(DQDDefinitionError, match=message):
            parse_predicate(bad)


# --- function resolution: no engine ----------------------------------------------------------


class TestResolveFunction:
    def test_an_allowed_function_resolves_to_pyspark(self):
        from pyspark.sql import functions as F

        assert resolve_function("upper") is F.upper
        assert resolve_function("UPPER") is F.upper

    @pytest.mark.parametrize("name", sorted(FORBIDDEN_FUNCTIONS))
    def test_every_forbidden_function_is_refused(self, name):
        with pytest.raises(DQDDefinitionError, match=rf"'{name}'.*arbitrary code"):
            resolve_function(name)

    @pytest.mark.parametrize("name", sorted(NOT_ROW_FUNCTIONS))
    def test_every_function_that_is_not_one_row_in_one_row_out_is_refused(self, name):
        with pytest.raises(DQDDefinitionError, match=rf"'{name}'.*one row in"):
            resolve_function(name)

    def test_an_unknown_function_is_refused(self):
        with pytest.raises(DQDDefinitionError, match="'no_such_function'.*not a pyspark.sql.functions"):
            resolve_function("no_such_function")

    def test_the_forbidden_set_is_dqfs(self):
        # DQF's _FORBIDDEN_FUNCTIONS, pinned when DQF was removed (F146).
        assert frozenset({
            "reflect", "try_reflect", "java_method", "try_java_method", "expr", "udf", "pandas_udf",
            "call_udf", "call_function", "unwrap_udt", "broadcast",
        }) == FORBIDDEN_FUNCTIONS

    def test_the_raw_value_positions_are_dqfs_less_substr(self):
        # DQF's table, less `substr`, which PySpark 3.5 takes as columns only (F141), plus
        # `sha2`'s bit length, for the Data Vault hash key (F159).
        assert {
            "cast": {1}, "concat_ws": {0}, "date_format": {1}, "format_string": {0},
            "format_number": {1}, "regexp_replace": {1, 2}, "regexp_extract": {1, 2},
            "substring": {1, 2}, "date_add": {1}, "date_sub": {1},
            "add_months": {1}, "split": {1}, "trim": {1}, "ltrim": {1}, "rtrim": {1},
            "round": {1}, "bround": {1}, "sha2": {1},
        } == RAW_VALUE_POSITIONS

    def test_the_named_aggregates_windows_sorts_and_generators_are_all_refused(self):
        for name in ("sum", "count", "collect_list", "row_number", "rank", "lag", "asc", "desc",
                     "explode", "posexplode", "inline", "approx_percentile"):
            assert name in NOT_ROW_FUNCTIONS


# --- resolution and parity with DQF: engine --------------------------------------------------


def test_every_aggregate_window_and_generator_spark_registers_is_refused(spark):
    """The refused set against Spark's own registry: a Python name whose SQL function Spark groups
    as an aggregate, a window or a generator must be refused (F137 review: `approx_percentile`
    was missing). Classic sessions only: the registry is read through the JVM."""
    import inspect as _inspect

    from pyspark.sql import functions as F

    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        pytest.skip("the function registry is read through the JVM, which Spark Connect does not expose")
    registry = jvm.org.apache.spark.sql.catalyst.analysis.FunctionRegistry.builtin()
    identifier = jvm.org.apache.spark.sql.catalyst.FunctionIdentifier
    missing = []
    for name in dir(F):
        obj = getattr(F, name)
        if name.startswith("_") or _inspect.ismodule(obj) or not callable(obj) \
                or not (getattr(obj, "__module__", "") or "").startswith("pyspark.sql"):
            continue
        # `listagg_distinct`, `string_agg_distinct` are PySpark 4 spellings of `DISTINCT` on
        # an aggregate the registry knows without the suffix.
        lookup = name.lower().removesuffix("_distinct")
        info = registry.lookupFunction(identifier.apply(lookup))
        if info.isDefined() and info.get().getGroup() in ("agg_funcs", "window_funcs", "generator_funcs") \
                and name.lower() not in NOT_ROW_FUNCTIONS:
            missing.append(name)
    assert not missing, f"Spark registers these as aggregates, windows or generators: {missing}"


@pytest.fixture(scope="module")
def df(spark):
    return spark.createDataFrame(
        [
            (1, "ada", "2024-01-15", 10.456, ["x", "y"], ("p", 1), "active", None),
            (2, "Grace", "2023-12-31", -2.5, ["z"], ("q", 2), "inactive", "n"),
            (3, None, None, 0.0, [], ("r", None), "active", "m"),
        ],
        "id int, name string, dob string, amount double, tags array<string>, "
        "s struct<a:string,b:int>, status string, note string",
    )


def _rows(frame, column):
    return [r[0] for r in frame.select(column.alias("v")).orderBy("id").collect()] \
        if "id" in frame.columns else [r[0] for r in frame.select(column.alias("v")).collect()]


def _dqd(df, expr):
    return _rows(df, resolve_expression(parse_expression(expr), df.columns))


# Each DQD expression with the rows DQF's resolve_expression produced for the same expression in
# DQF's spelling, over this module's DataFrame, pinned when DQF was removed (F146).
_PARITY = {'cast': ({'cast': {'data_type': 'date', 'expression': {'column': 'dob'}}},
          [datetime.date(2024, 1, 15), datetime.date(2023, 12, 31), None]),
 'cast as a function': ({'arguments': [{'column': 'id'}, {'literal': 'string'}], 'function': 'cast'},
                        ['1', '2', '3']),
 'column': ({'column': 'name'}, ['ada', 'Grace', None]),
 'function': ({'arguments': [{'column': 'name'}], 'function': 'upper'}, ['ADA', 'GRACE', None]),
 'literal': ({'literal': 7}, [7, 7, 7]),
 'literal list': ({'literal': [1, 2]}, [[1, 2], [1, 2], [1, 2]]),
 'nested': ({'arguments': [{'column': 'note'},
                           {'arguments': [{'column': 'name'}], 'function': 'lower'},
                           {'literal': '?'}],
             'function': 'coalesce'},
            ['ada', 'n', 'm']),
 'operator !=': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '!='}, [True, True, True]),
 'operator %': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '%'},
                [0.4559999999999995, -0.5, 0.0]),
 'operator *': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '*'}, [20.912, -5.0, 0.0]),
 'operator +': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '+'}, [12.456, -0.5, 2.0]),
 'operator -': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '-'}, [8.456, -4.5, -2.0]),
 'operator /': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '/'}, [5.228, -1.25, 0.0]),
 'operator <': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '<'}, [False, True, True]),
 'operator <=': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '<='},
                 [False, True, True]),
 'operator =': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '='},
                [False, False, False]),
 'operator ==': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '=='},
                 [False, False, False]),
 'operator >': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '>'}, [True, False, False]),
 'operator >=': ({'arguments': [{'column': 'amount'}, {'literal': 2}], 'function': '>='},
                 [True, False, False]),
 'predicate as a value': ({'left': {'column': 'amount'}, 'operator': '>', 'right': {'literal': 0}},
                          [True, False, False]),
 'raw format': ({'arguments': [{'arguments': [{'column': 'dob'}], 'function': 'to_date'},
                               {'literal': 'yyyy/MM'}],
                 'function': 'date_format'},
                ['2024/01', '2023/12', None]),
 'raw pattern and replacement': ({'arguments': [{'column': 'name'}, {'literal': '[aeiou]'}, {'literal': '*'}],
                                  'function': 'regexp_replace'},
                                 ['*d*', 'Gr*c*', None]),
 'raw position and length': ({'arguments': [{'column': 'name'}, {'literal': 1}, {'literal': 2}],
                              'function': 'substring'},
                             ['ad', 'Gr', None]),
 'raw scale': ({'arguments': [{'column': 'amount'}, {'literal': 1}], 'function': 'round'}, [10.5, -2.5, 0.0]),
 'raw separator': ({'arguments': [{'literal': '-'}, {'column': 'name'}, {'column': 'status'}],
                    'function': 'concat_ws'},
                   ['ada-active', 'Grace-inactive', 'active']),
 'raw: add_months': ({'arguments': [{'arguments': [{'column': 'dob'}], 'function': 'to_date'},
                                    {'literal': 2}],
                      'function': 'add_months'},
                     [datetime.date(2024, 3, 15), datetime.date(2024, 2, 29), None]),
 'raw: bround': ({'arguments': [{'column': 'amount'}, {'literal': 1}], 'function': 'bround'},
                 [10.5, -2.5, 0.0]),
 'raw: date_add': ({'arguments': [{'arguments': [{'column': 'dob'}], 'function': 'to_date'}, {'literal': 3}],
                    'function': 'date_add'},
                   [datetime.date(2024, 1, 18), datetime.date(2024, 1, 3), None]),
 'raw: date_add with a column where a raw value may go': ({'arguments': [{'arguments': [{'column': 'dob'}],
                                                                          'function': 'to_date'},
                                                                         {'column': 'id'}],
                                                           'function': 'date_add'},
                                                          [datetime.date(2024, 1, 16),
                                                           datetime.date(2024, 1, 2),
                                                           None]),
 'raw: date_sub': ({'arguments': [{'arguments': [{'column': 'dob'}], 'function': 'to_date'}, {'literal': 3}],
                    'function': 'date_sub'},
                   [datetime.date(2024, 1, 12), datetime.date(2023, 12, 28), None]),
 'raw: format_number': ({'arguments': [{'column': 'amount'}, {'literal': 1}], 'function': 'format_number'},
                        ['10.5', '-2.5', '0.0']),
 'raw: format_string': ({'arguments': [{'literal': '%s-%d'}, {'column': 'name'}, {'column': 'id'}],
                         'function': 'format_string'},
                        ['ada-1', 'Grace-2', 'null-3']),
 'raw: regexp_extract': ({'arguments': [{'column': 'name'}, {'literal': '([a-z]+)'}, {'literal': 1}],
                          'function': 'regexp_extract'},
                         ['ada', 'race', None]),
 'raw: split': ({'arguments': [{'column': 'name'}, {'literal': 'a'}], 'function': 'split'},
                [['', 'd', ''], ['Gr', 'ce'], None]),
 'when alone': ({'arguments': [{'left': {'column': 'name'}, 'operator': 'is_null'}, {'literal': True}],
                 'function': 'when'},
                [None, None, True]),
 'when on an expression': ({'arguments': [{'arguments': [{'column': 'note'}], 'function': 'isnull'},
                                          {'literal': 1},
                                          {'literal': 0}],
                            'function': 'when'},
                           [1, 0, 0]),
 'when with otherwise': ({'arguments': [{'left': {'column': 'status'},
                                         'operator': '=',
                                         'right': {'literal': 'active'}},
                                        {'literal': 'A'},
                                        {'literal': 'I'}],
                          'function': 'when'},
                         ['A', 'I', 'A'])}

# The ids each predicate kept under DQF's evaluate_predicate, pinned the same way.
_PREDICATES = {'!=': ({'left': {'column': 'id'}, 'operator': '!=', 'right': {'literal': 2}}, [1, 3]),
 '<': ({'left': {'column': 'id'}, 'operator': '<', 'right': {'literal': 2}}, [1]),
 '<=': ({'left': {'column': 'id'}, 'operator': '<=', 'right': {'literal': 2}}, [1, 2]),
 '=': ({'left': {'column': 'note'}, 'operator': '=', 'right': {'literal': 'n'}}, [2]),
 '>': ({'left': {'column': 'id'}, 'operator': '>', 'right': {'literal': 1}}, [2, 3]),
 '>=': ({'left': {'column': 'id'}, 'operator': '>=', 'right': {'literal': 2}}, [2, 3]),
 'in': ({'left': {'column': 'id'}, 'operator': 'in', 'right': {'literal': [1, 3]}}, [1, 3]),
 'is_not_null': ({'left': {'column': 'note'}, 'operator': 'is_not_null'}, [2, 3]),
 'is_null': ({'left': {'column': 'note'}, 'operator': 'is_null'}, [1]),
 'like': ({'left': {'column': 'name'}, 'operator': 'like', 'right': {'literal': '%a%'}}, [1, 2]),
 'starts_with': ({'left': {'column': 'name'}, 'operator': 'starts_with', 'right': {'literal': 'G'}}, [2])}

_NOTE_N = {"operator": "=", "left": {"column": "note"}, "right": {"literal": None}, "null_safe": True}
_ACTIVE = {"operator": "=", "left": {"column": "status"}, "right": {"literal": "active"}}
_LOGICAL = {
    "null-safe =": (_NOTE_N, [1]),
    "and": ({"operator": "and", "left": _ACTIVE, "right": {"operator": "is_null", "left": {"column": "note"}}}, [1]),
    "or": ({"operator": "or", "left": _ACTIVE, "right": _NOTE_N}, [1, 3]),
    "not": ({"operator": "not", "expression": _ACTIVE}, [2]),
}


def _kept(df, predicate):
    return sorted(r.id for r in df.filter(resolve_predicate(parse_predicate(predicate), df.columns)).collect())


@pytest.mark.parametrize("name", sorted(_PARITY))
def test_an_expression_gives_the_same_rows_as_dqf(df, name):
    expression, dqf_rows = _PARITY[name]
    assert _dqd(df, expression) == dqf_rows


@pytest.mark.parametrize("operator", sorted(_PREDICATES))
def test_a_predicate_filters_the_same_rows_as_dqf(df, operator):
    predicate, dqf_ids = _PREDICATES[operator]
    assert _kept(df, predicate) == dqf_ids


@pytest.mark.parametrize("name", sorted(_LOGICAL))
def test_the_null_safe_equality_and_the_logical_operators_match_dqf(df, name):
    predicate, dqf_ids = _LOGICAL[name]
    assert _kept(df, predicate) == dqf_ids


def test_in_against_an_array_column_matches_dqf(df):
    assert _kept(df, {"operator": "in", "left": {"literal": "x"}, "right": {"column": "tags"}}) == [1]


def test_a_cast_to_date_gives_dates(df):
    assert _dqd(df, {"cast": {"expression": {"column": "dob"}, "data_type": "date"}}) \
        == [date(2024, 1, 15), date(2023, 12, 31), None]


def test_a_dotted_name_reads_a_struct_field(df):
    # Not a parity case: DQF reads a struct field only through a table prefix (`t1.s.a`),
    # and a DQD has one source and no prefix.
    assert _dqd(df, {"column": "s.a"}) == ["p", "q", "r"]
    assert _dqd(df, {"column": "s.b"}) == [1, 2, None]


def test_a_column_is_matched_whatever_its_case_as_spark_does(df):
    assert _dqd(df, {"column": "NAME"}) == ["ada", "Grace", None]


def test_a_column_the_source_does_not_have_is_named(df):
    with pytest.raises(DQDDefinitionError, match="'missing'.*not a column of the source"):
        resolve_expression(parse_expression({"column": "missing"}), df.columns)


def test_a_struct_field_of_a_missing_column_is_named(df):
    with pytest.raises(DQDDefinitionError, match="'t.a'.*not a column of the source"):
        resolve_expression(parse_expression({"column": "t.a"}), df.columns)


def test_a_column_whose_name_holds_a_dot_is_read_whole(spark):
    frame = spark.createDataFrame([(1, 2)], "id int, `a.b` int")
    assert _rows(frame, resolve_expression(parse_expression({"column": "a.b"}), frame.columns)) == [2]


def test_substr_with_literal_position_and_length_works_where_dqf_failed(df):
    # DQF passed substr's literals raw and failed with NOT_COLUMN_OR_STR on PySpark 3.5 (F141).
    expression = {"function": "substr", "arguments": [{"column": "name"}, {"literal": 2}, {"literal": 2}]}
    assert _dqd(df, expression) == ["da", "ra", None]


def test_sha2_with_a_literal_bit_length_builds_the_data_vault_key(df):
    """F159: the canonical hash key, sha2(upper(trim(key)), 256), is built in a DQD."""
    import hashlib
    expr = {"function": "sha2", "arguments": [
        {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"column": "name"}]}]},
        {"literal": 256}]}
    got = _dqd(df, expr)
    names = _rows(df, df["name"])
    assert got == [None if n is None else hashlib.sha256(n.strip().upper().encode()).hexdigest() for n in names]


def test_a_case_sensitive_session_matches_exactly(df):
    with pytest.raises(DQDDefinitionError, match="'NAME'.*not a column of the source"):
        resolve_expression(parse_expression({"column": "NAME"}), df.columns, case_sensitive=True)


def test_a_name_two_columns_share_but_for_case_is_refused():
    with pytest.raises(DQDDefinitionError, match="'id'.*more than one column.*'Id'.*'ID'"):
        resolve_expression(parse_expression({"column": "id"}), ["Id", "ID"])


def test_the_hidden_metadata_column_is_left_to_spark(spark, tmp_path):
    path = str(tmp_path / "source")
    spark.createDataFrame([(1,)], "id int").write.parquet(path)
    frame = spark.read.parquet(path)
    assert "_metadata" not in frame.columns
    name = resolve_expression(parse_expression({"column": "_metadata.file_name"}), frame.columns)
    assert frame.select(name).collect()[0][0].endswith(".parquet")
