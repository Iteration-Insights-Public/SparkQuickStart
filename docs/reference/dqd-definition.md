# DQD definition

The expression language of a Declarative Query Definition: what may appear in `dqd_context`'s
`filters`, `transformations` and `selection`. It is the per-row language of the removed DQF engine (backlog F146) with its keys renamed
to the load context's style, parsed by `orchestration/dqd/parse.py` and resolved to Spark
columns by `orchestration/dqd/resolve.py`. The section itself is on the
[load-context](load-context.md#dqdcontext) page; why it is this shape is in the
[design](../architecture/decisions/2026-09-29-declarative-query-definition-design.md).

## Expressions

An expression is an object with exactly one of these keys, or a predicate standing for a boolean.

| Key | Shape | Meaning |
|---|---|---|
| `column` | `{"column": "NAME"}` | A source column, matched as the session matches names. A dotted name reads a struct field when no column has the whole name. The hidden `_metadata` column is left to Spark. |
| `literal` | `{"literal": 42}` | A value. A list becomes an array. |
| `cast` | `{"cast": {"expression": e, "data_type": "date"}}` | `e` cast to a Spark type name. |
| `function` | `{"function": "upper", "arguments": [e, ...]}` | A `pyspark.sql.functions` function, its name in any case; `arguments` defaults to none. |
| `operator` | a predicate, below | A boolean value, such as a flag column. |

Four function names are handled by the library itself rather than looked up:

| Name | Arguments | Meaning |
|---|---|---|
| `when` | 2 or 3 | A condition, a value, and an optional value otherwise. The condition may be a predicate. |
| `cast` | 2 | An expression and a type name. |
| `=`, `==`, `!=`, `>`, `>=`, `<`, `<=` | 2 | Comparison; `=` and `==` are plain equality. |
| `+`, `-`, `*`, `/`, `%` | 2 | Arithmetic. |

## Predicates

A predicate is an object with an `operator`.

| Operator | Keys | Meaning |
|---|---|---|
| `=` | `left`, `right`, optional `null_safe` | Equality; with `"null_safe": true`, NULL equals NULL. |
| `!=`, `>`, `>=`, `<`, `<=` | `left`, `right` | Comparison. |
| `in` | `left`, `right` | `right` a literal list, or an array expression. |
| `like`, `starts_with` | `left`, `right` | `right` a literal string pattern. |
| `is_null`, `is_not_null` | `left` | NULL test. |
| `and`, `or` | `left`, `right` | Both predicates. |
| `not` | `expression` | A predicate. |

## Function resolution

- A function is one of `pyspark.sql.functions` as installed, its name lower-cased.
- Refused, as DQF refused them, because they run arbitrary code or SQL: `reflect`, `try_reflect`,
  `java_method`, `try_java_method`, `expr`, `udf`, `pandas_udf`, `call_udf`, `call_function`,
  `unwrap_udt`, `broadcast`.
- Refused, because a DQD maps one row in to one row out: every aggregate, window, sort and
  generator function, such as `sum`, `count`, `approx_percentile`, `row_number`, `lag`, `asc`,
  `explode`. The list is `NOT_ROW_FUNCTIONS` in `orchestration/dqd/functions.py`, held against
  Spark's own function registry by a test.
- An argument is a column, except where the function takes a raw value and the definition gives
  a literal there: `cast` 1, `concat_ws` 0, `date_format` 1, `format_string` 0, `format_number` 1,
  `regexp_replace` 1 and 2, `regexp_extract` 1 and 2, `substring` 1 and 2, `date_add` 1,
  `date_sub` 1, `add_months` 1, `split` 1, `trim`, `ltrim` and `rtrim` 1, `round` and `bround` 1,
  `sha2` 1, the bit length, so a Data Vault hash key can be built in a DQD (zero-based positions).

## From a DQF spec

| DQF | DQD |
|---|---|
| `col` | `column` |
| `lit` | `literal` |
| `cast` with `expr` and `dataType` | `cast` with `expression` and `data_type` |
| `fn` and `args` | `function` and `arguments` |
| `op` | `operator` |
| `not` with `expr` | `not` with `expression` |
| a transformation's or selection's `as` | `name` |
| a table's `kind`, `identifier` and `columns` | `source_kind`, `source_identifier` and `source_columns`; one table only, so no `alias` and no `alias.` prefix on a column |
| `Sequence` | the entry's place in its list |
| a table's `hwmColumnName` and `hwmCurrentValue` | the load context's `hwm_context`: `hwm_column_name` and `hwm_value` |
| `excludeColumnsFromCDC` | nothing: DQF did not read it either; `scd_context.exclude_from_hash` keeps a column out of the hash |

DQF checked its result against `global_context.staging_table_schema` and put its columns in that
order; a DQD does neither, and its columns come in the selection's order.

The table is for migrating a DQF spec: DQF itself was removed (backlog F146), and a
`dqf_context` is refused. A DQF key in a DQD is refused with the name to use. A DQD is stricter than DQF in six places: there is
no `{"expr": ...}` wrapper node; an object with two kinds is refused; `null_safe` is accepted on
`=` only; `like` and `starts_with` take a literal string only; `in` against a single literal is
refused; and `substr`'s position and length pass as literal columns, because DQF's raw values
fail on PySpark 3.5 (backlog F141).

## Errors

An error is a `DQDDefinitionError` (a `ValueError`) that names where it is, such as
`dqd_context.selection[2].expression.arguments[1]`, with two exceptions: a name that matches more
than one source column but for case is a `DQDDefinitionError` naming the column and its matches,
not a path; and an `hwm_context.hwm_column_name` the source lacks is a plain `ValueError`
prefixed `hwm_context:`. A malformed expression is refused before the
source is read; a column the source lacks, when the source's columns are known.
