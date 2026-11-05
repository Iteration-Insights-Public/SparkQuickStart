# Pipeline state

## `PipelineState`

`PipelineState` is the immutable bundle a transformer receives and returns, defined in
`src/spark_quick_start/orchestration/transformers/state.py` and exported by
`spark_quick_start.orchestration`. A transformer never mutates one: each
`with_*` method returns a new state that carries every other field. Construction enforces the
invariants: `frames` never holds `None` (`TypeError`), `target_exists=True` without the `"target"`
key and `target_exists=False` with it are `ValueError`s. Writes through `with_frame` or
`with_updates` under the `"target"` key route to `with_target`, so only direct construction can
reach those `ValueError`s.

| Member | Type or signature | Default | Meaning |
|---|---|---|---|
| `df` | `DataFrame` | required | The primary DataFrame flowing through the pipeline. |
| `frames` | `dict[str, DataFrame]` | `{}` | Named secondary DataFrames, such as `"target"`. Never holds `None`. |
| `halted` | `bool` | `False` | When true, process transformers skip and pass the state through. Set by `with_halted`. |
| `halt_reason` | `str` | `""` | Why the pipeline halted; surfaces as `ExecutionResult.early_exit_reason`. |
| `target_exists` | `bool \| None` | `None` | `None` until a target step has run; then `True` (the `"target"` frame is present, possibly empty) or `False` (the table does not exist, the key is absent). An existing empty table is `True`. |
| `debug_columns` | `frozenset[str]` | `frozenset()` | Columns on `df` that exist for troubleshooting, such as the detector's `debug_hash_string_column`. The `NativeSCD*` writers drop them before the write. Set by `with_debug_columns`. |
| `hash_separator` | `str \| None` | `None` | The separator the SCD change detector hashed the candidates with, which the `NativeSCD*` writers seal on the target (F211). `None` until the detector runs. Set by `with_hash_separator`. |
| `cached` | `tuple[DataFrame, ...]` | `()` | Frames a step persisted in memory: `MaterializeTransformer` and `MaterializeFrameTransformer` under `runtime_context.materialize_strategy` `"cache"`. `LoadOrchestrator.run()` unpersists every one when the run ends, whether it succeeded, failed or exited early; an `unpersist` that raises is logged as a warning and does not change the result. A step that fails after caching releases its own frame before raising, so this holds only frames of steps that completed. Set by `with_cached`. |
| `with_df` | `(df)` | n/a | A new state with `df` replaced. |
| `with_debug_columns` | `(*names)` | n/a | A new state that also declares `names` as debug columns. |
| `with_hash_separator` | `(separator)` | n/a | A new state stating the separator the change hash used. |
| `with_cached` | `(df)` | n/a | A new state that also declares `df` as persisted, to be released when the run ends. Declaring the same frame twice records it once. |
| `with_frame` | `(name, df)` | n/a | A new state with one named frame added or replaced. A `None` `df` raises `TypeError`, except under `"target"`, where it is deprecated and means `with_target(None)`. A DataFrame under `"target"` is `with_target(df)`. |
| `with_target` | `(frame)` | n/a | The setter every target write goes through. A DataFrame sets `frames["target"]` and `target_exists=True`, from any prior value; `None` removes the key and sets `target_exists=False`. |
| `with_halted` | `(reason)` | n/a | A new state with `halted=True` and `halt_reason` set. |
| `with_updates` | `(df=None, **frames)` | n/a | A new state with `df` and any named frames replaced at once; `df=None` keeps the current one. A `target` keyword routes to `with_target`, `None` deprecated as above. A `None` for any other key raises `TypeError`. |

The deprecated `None`-target forms are listed beside the renamed steps on the
[transformer reference](transformers.md).

## See also

- [Transformer reference](transformers.md): the steps that receive and return a state.
