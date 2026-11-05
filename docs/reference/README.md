# Reference

The stable configuration and API facts of the library, one subject per page. Each page is held
to the code by a test in the architecture suite, so a row here names something the code has.

| Page | What it is |
|---|---|
| [load-context.md](load-context.md) | Every field of the load-context JSON, by section |
| [column-naming.md](column-naming.md) | `ColumnNamingPolicy`: the names of the nine SCD meta columns |
| [dqd-definition.md](dqd-definition.md) | The expression language of a Declarative Query Definition: expressions, predicates and function resolution |
| [transformers.md](transformers.md) | Every export of the transformers package, each constructor's parameters, and the three metric protocols |
| [pipeline-state.md](pipeline-state.md) | `PipelineState`: its fields, its `with_*` methods and the target-exists flag |
| [platform-adapters.md](platform-adapters.md) | `PlatformAdapter`'s methods, the shipped adapters, and `MockPlatformAdapter`'s parameters and recorded calls |
| [orchestrator.md](orchestrator.md) | `LoadOrchestrator`, the `platform` values, and the two pipeline factories |
| [execution-result.md](execution-result.md) | What `LoadOrchestrator.run()` returns: `ExecutionResult`, its metrics and its per-stage metrics |
| [logging.md](logging.md) | `OrchestratorLogger`, `LogSession` and the `spark_logging` functions, and the log persistence adapters |
