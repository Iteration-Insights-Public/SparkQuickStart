# Guides

Task-oriented how-to, for someone using or extending the library.

| Guide | What it is |
|---|---|
| [try-it-locally.md](try-it-locally.md) | Try it locally in 15 minutes: two SCD2 loads of sample data on your own machine, with no cloud account |
| [run-your-first-load.md](run-your-first-load.md) | Run your first load: a staging notebook, a load context and `default_pipeline`, on Fabric, Databricks or locally |
| [write-a-load-context.md](write-a-load-context.md) | Write the load context for a new entity and check it before a run |
| [build-a-pipeline.md](build-a-pipeline.md) | Choose your own steps, add one conditionally, and pick the source at run time |
| [choose-a-load-style.md](choose-a-load-style.md) | Choose between merge and append-only loads, and when to hash in schema order |
| [write-a-custom-transformer.md](write-a-custom-transformer.md) | Write a step of your own, place it in a load, and report a watermark or counts |
| [run-incremental-loads.md](run-incremental-loads.md) | Stage only what changed since the last run, with a source watermark handed from run to run |
| [write-a-staging-notebook.md](write-a-staging-notebook.md) | Write the notebook that stages an entity's rows, and return its log to the load |
| [inspect-a-run.md](inspect-a-run.md) | Read a load's result, per-step timings, sample rows, query plans and log |
| [tune-performance.md](tune-performance.md) | Make a load faster: filter the target, compute expensive frames once, and turn off unused measurements |
| [test-without-a-cluster.md](test-without-a-cluster.md) | Test a load with pytest on your own machine or in CI, with local Spark, Delta and your staging code |
| [set-up-for-development.md](set-up-for-development.md) | Set up a checkout of this repository and run the tests, linter and type check before a commit |
| [load-a-table-with-a-dqd.md](load-a-table-with-a-dqd.md) | Bring one table to its target from a Declarative Query Definition, with no staging notebook |
| [load-a-data-vault-entity.md](load-a-data-vault-entity.md) | Load one entity into a Data Vault hub and satellite from a DQD, and read its current rows |
| [load-a-data-vault-link.md](load-a-data-vault-link.md) | Load a link between two Data Vault hubs, and a link satellite on it |
| [handle-early-exit-and-errors.md](handle-early-exit-and-errors.md) | Tell a load that wrote from one with nothing to write or one that failed, and end the notebook so its caller sees it |
| [cut-a-release.md](cut-a-release.md) | Publish a release: run the platform-filtered builds, check the release branches, and dispatch the release workflow per platform |
| [user-guide.md](user-guide.md) | The original user guide — being dissolved into task guides and reference pages |
| [orchestration-user-guide.md](orchestration-user-guide.md) | The original orchestration guide — being dissolved the same way |
