# Repository layout

Where everything in the repository lives, for someone looking for the file that owns a thing. The
tree shows every tracked entry at the top level; the module table below it names every module of the
`spark_quick_start` package and is held to the package by a test. How the modules work together
is on the [overview](overview.md) and the pages it links.

## The tree

```
SparkQuickStart/
├── src/
│   └── spark_quick_start/     # the package: shipped as a wheel and as a source copy
├── tests/
│   ├── unit/                  # one unit under test (+ fabric/, databricks/, local/)
│   ├── integration/           # units wired together (+ local/)
│   └── architecture/          # the shape of the source tree and the docs
├── docs/
│   ├── architecture/          # how the library is built, and dated records in decisions/
│   ├── guides/                # task-oriented guides
│   └── reference/             # configuration and API reference, held to the code
├── reference_design/          # reference notebooks for local runs; Fabric and Databricks ones to come
├── data/                      # sample Delta tables for local runs
├── scripts/                   # release filtering and its pyproject strip, trunk check, mypy baseline, mermaid check
├── .claude/                   # agent instructions: rules, standards, skills, commands, backlog
├── .github/                   # CI and release workflows, and CODEOWNERS
├── CLAUDE.md                  # the always-loaded instruction index
├── README.md                  # the project's front door
├── CHANGELOG.md               # what changed in each version
├── CONTRIBUTING.md            # how to contribute
├── CODE_OF_CONDUCT.md         # the code of conduct, by link
├── SECURITY.md                # how to report a vulnerability
├── SUPPORT.md                 # where to get help
├── LICENSE
├── pyproject.toml             # package metadata, extras, pytest and coverage configuration
├── requirements.txt           # the lock, compiled from pyproject.toml
├── mypy-baseline.txt          # the accepted type errors; a gate file (CLAUDE.md §3)
├── Dockerfile                 # the image CI runs the suites in
├── docker-compose.yaml        # the same image for local runs
└── .gitignore, .dockerignore
```

The three suites and how a test is placed are the
[test-classification rule](../../.claude/rules/test-classification.md).

## Module responsibilities

Paths are relative to `src/spark_quick_start/`. A package row describes its `__init__.py`. The
package root's own `__init__.py` has no row: it refuses a Python older than 3.11, before any
submodule imports, sets `__version__` from the installed metadata (`0.0.0+unknown` on a source copy), and re-exports
`ColumnNamingPolicy`.

| Module | Responsibility |
|---|---|
| `adapters/` | The adapter package; imports each concrete adapter defensively, so a single-platform build still imports. |
| `adapters/platform_adapter.py` | `PlatformAdapter`, the platform interface, and `MockPlatformAdapter`, its recording test double. |
| `adapters/fabric_adapter.py` | `FabricAdapter`, through `notebookutils`. |
| `adapters/databricks_adapter.py` | `DatabricksAdapter`, through `dbutils`. |
| `adapters/local_adapter.py` | `LocalAdapter`, for local development and CI. |
| `local_notebookutils/` | A local stand-in for `notebookutils`, injected into `sys.modules`. |
| `local_notebookutils/_notebook.py` | `NotebookStub`: runs a notebook as a registered callable or an `.ipynb` file. |
| `logger/` | The logger package. |
| `logger/orchestrator_logger.py` | `OrchestratorLogger`, the logger every step is given. |
| `orchestration/` | The orchestration package and its public re-exports. |
| `orchestration/orchestrator.py` | `LoadOrchestrator`: builds and runs a pipeline and returns an `ExecutionResult`. |
| `orchestration/pipeline_factory.py` | `default_pipeline` and the three Data Vault factories. |
| `orchestration/context_manager.py` | `LoadContextManager`: parses and validates the load-context JSON. |
| `orchestration/models.py` | The context classes, `ExecutionResult`, `ExecutionMetrics`, `ErrorDetails` and `Constants`. |
| `orchestration/column_policy.py` | `ColumnNamingPolicy`: the SCD meta column names. |
| `orchestration/metrics.py` | `StageMetrics` and `StageMetricsCollector`. |
| `orchestration/load_pipeline.py` | Staging-table validation and merge, staged and target reads, the SCD `MERGE`, the append writes and the Data Vault insert-only write, commit statistics and the watermark read. |
| `orchestration/delta_utils.py` | Staging writes and deletes, and the Delta retry wrapper. |
| `orchestration/staging_pipeline.py` | Runs the staging phases and builds a staging notebook's `main` function. |
| `orchestration/notebook_executor.py` | `NotebookExecutor`: runs notebooks through the adapter and parses their results. |
| `orchestration/validation.py` | Schema comparison, the one home for it. |
| `orchestration/dqd/` | The Declarative Query Definition's expression library: PySpark and a few standard-library modules only, reached from transformers through one seam ([layering](../../.claude/rules/layering.md) §7). |
| `orchestration/dqd/nodes.py` | The parsed expression and predicate nodes, and `DQDDefinitionError`. |
| `orchestration/dqd/parse.py` | `parse_expression` and `parse_predicate`: JSON to checked nodes, once. |
| `orchestration/dqd/functions.py` | Which `pyspark.sql.functions` a definition may call, the operators, and the arguments passed as raw values. |
| `orchestration/dqd/resolve.py` | `resolve_expression` and `resolve_predicate`: nodes to Spark columns over a source's columns. |
| `orchestration/transformers/` | The transformer package and its exports. |
| `orchestration/transformers/base.py` | `SourceTransformer`, `ProcessTransformer`, `AlwaysRunTransformer` and `LoadContext`. |
| `orchestration/transformers/state.py` | `PipelineState`, the state passed between steps. |
| `orchestration/transformers/protocols.py` | `HWMProvider`, `SCDMetricsProvider` and `SourceMetricsProvider`. |
| `orchestration/transformers/_constants.py` | The names steps share, routing columns and the target frame key, and `Constants`, the transformers' copy of the one in `models.py`. |
| `orchestration/transformers/_pipeline.py` | Re-exports the Delta and SCD functions steps may call. |
| `orchestration/transformers/_validation.py` | `assert_no_dups`, and nothing else. |
| `orchestration/transformers/_aliases.py` | The deprecated names of the steps F155 gave an `SCD` prefix, resolved with a warning; never in `__all__`. |
| `orchestration/transformers/_hashing.py` | The Data Vault hash diff (`hash_hex`, sha256 or md5 hex) and the seal of its settings on a table. |
| `orchestration/transformers/_scd_seal.py` | The seal of an SCD table's flag values, open-row sentinel and hash separator: compared before the merge and append-only writers write, set after. |
| `orchestration/transformers/_dqd.py` | Re-exports the DQD expression library, the one module that imports it. |
| `orchestration/transformers/staging.py` | `StagingTransformer`. |
| `orchestration/transformers/dqd.py` | `DQDTransformer`. |
| `orchestration/transformers/dv_hub.py` | `DVHubTargetTransformer`, `DVHubWriterTransformer`, the Data Vault load-column stamp, and the insert-once logic the link steps share. |
| `orchestration/transformers/dv_link.py` | `DVLinkTargetTransformer`, `DVLinkWriterTransformer`: the Data Vault link's steps. |
| `orchestration/transformers/dv_satellite.py` | The Data Vault satellite's steps: the two target steps that read each key's latest row, the change detector and the writer. |
| `orchestration/transformers/conditional_source.py` | `ConditionalSourceTransformer`. |
| `orchestration/transformers/target.py` | `SCDTargetTableTransformer` and `SCDIncrementalTargetTableTransformer`. |
| `orchestration/transformers/change_detector.py` | `SCDChangeDetectorTransformer` and `SCDGracefulChangeDetectorTransformer`. |
| `orchestration/transformers/staged_key_guard.py` | `StagedKeyGuardTransformer`. |
| `orchestration/transformers/candidate_guard.py` | `CandidateGuardTransformer`. |
| `orchestration/transformers/candidate_version_guard.py` | `SCDCandidateVersionGuardTransformer`. |
| `orchestration/transformers/native_scd.py` | `NativeSCDMergeTransformer` and `NativeSCDAppendOnlyTransformer`. |
| `orchestration/transformers/conditional.py` | `ConditionalTransformer`. |
| `orchestration/transformers/materialize.py` | `MaterializeTransformer` and `MaterializeFrameTransformer`. |
| `orchestration/transformers/debug.py` | `DebugTransformer`. |
| `orchestration/transformers/metrics_chart.py` | `MetricsChartTransformer`. |
| `orchestration/transformers/plan_report.py` | `PlanReportTransformer`. |
| `spark_load_orchestration.py` | A compatibility shim that re-exports `create_staging_main_function` for old staging notebooks. Re-exports only. |
| `spark_logging/` | `LogSession` and the `spark_logging` functions, which live in the package's `__init__.py`. |
| `spark_logging/persistence_adapter.py` | `LogPersistenceAdapter`, `LogRow`, and the platform-neutral persistence adapters. |
| `spark_logging/fabric_persistence.py` | `FabricPersistenceAdapter`. |
| `spark_logging/databricks_persistence.py` | `DatabricksPersistenceAdapter`. |
| `spark_logging/local_persistence.py` | `LocalFilePersistenceAdapter`. |

## Coverage

The unit suite carries an 85 % coverage gate. `pyproject.toml`'s `[tool.coverage.run] omit` list
leaves out the Fabric and Databricks adapters, `spark_logging/` and
`orchestration/load_pipeline.py`. That list is a gate file (CLAUDE.md §3).

## See also

- [Overview](overview.md): how the modules work together in a load.
- [Platform abstraction](platform-abstraction.md): why platform code lives only in `adapters/` and the persistence modules.
