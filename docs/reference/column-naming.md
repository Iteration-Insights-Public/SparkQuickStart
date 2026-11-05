# Column naming

`ColumnNamingPolicy` names the nine SCD meta columns the library writes to a target table. It is a
frozen dataclass defined in `src/spark_quick_start/orchestration/column_policy.py` and exported
from the package root. `ColumnNamingPolicy.default()` is the single source of truth for the
built-in names: the `SCDContext` defaults on the [load-context reference](load-context.md#scdcontext)
and the `Constants` classes in `orchestration/models.py` and `orchestration/transformers/_constants.py`
all read from it.

A policy reaches the library only through code that builds `SCDContext` itself, as
`SCDContext(data, policy=policy)`. A load run through `LoadOrchestrator` or the pipeline
factories uses `ColumnNamingPolicy.default()` for the SCD styles and
`ColumnNamingPolicy.data_vault()` for the `dv_*` styles, and its names are changed one at a time with the
`scd_context` keys of the same names, which take precedence over the policy.

## `ColumnNamingPolicy`

Every field is required: a policy built by hand names all nine columns.

| Member | Type | Default | Meaning |
|---|---|---|---|
| `row_key_column` | `str` | required | The row key column: the business key as a string, or its SHA-256 when `scd_row_hashing` is set. When `SCDContext.business_key_column` has the same name, the staged key passes through with its value and type. `default()` names it `scdRowKey`. |
| `hash_column` | `str` | required | The change hash column. `default()` names it `scdHash`. |
| `version_column` | `str` | required | The version column: 1 for a new row; the target's version plus 1 for an SCD2 change; 1 for an SCD1 change; for a deleted row, the target's version plus 1 on the new version an SCD2 load inserts (the old row is closed out), and the target's version, unchanged, under SCD1's soft delete. `default()` names it `scdVersion`. |
| `effective_from_column` | `str` | required | The column holding when a version became current. `default()` names it `scdEffectiveFrom`. |
| `effective_to_column` | `str` | required | The column holding when a version stopped being current. `default()` names it `scdEffectiveTo`. |
| `current_flag_column` | `str` | required | The current-version flag column. `default()` names it `scdCurrentFlag`. |
| `deleted_flag_column` | `str` | required | The deleted flag column. `default()` names it `scdDeletedFlag`. |
| `source_system_column` | `str` | required | The source system column. `default()` names it `scdSourceSystem`. |
| `correlation_id_column` | `str` | required | The column holding the correlation id of the run that wrote the row; under SCD1, of the last run that changed it. `default()` names it `scdCorrelationId`. |
| `default` | classmethod, `() -> ColumnNamingPolicy` | — | Returns the built-in `scd*` names listed above. |
| `data_vault` | classmethod, `() -> ColumnNamingPolicy` | — | Returns the Data Vault 2.0 names a hub or satellite load uses by default: `HashKey`, `HashDiff`, `IsDeleted`, `RecordSource` and `CorrelationId`; the version, effective-date and current-flag columns keep their `scd*` names, because a Data Vault style does not write them. |

A policy is immutable. To change one name, copy it with `dataclasses.replace`:

```python
import dataclasses

from spark_quick_start import ColumnNamingPolicy

policy = dataclasses.replace(ColumnNamingPolicy.default(), hash_column="rowHash")
```
