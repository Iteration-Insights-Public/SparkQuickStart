# Architecture

How the library is built, for someone changing it.

| Page | What it is |
|---|---|
| [overview.md](overview.md) | How one load moves through the library, from the notebook to the result |
| [pipeline-model.md](pipeline-model.md) | How a pipeline of transformers is built and run: state, step kinds, halting, services, wrappers |
| [metrics-and-watermarks.md](metrics-and-watermarks.md) | How a run measures itself, where the high watermark comes from, and what each measurement costs |
| [platform-abstraction.md](platform-abstraction.md) | How the library reaches Fabric, Databricks and a local machine only through adapters |
| [release-branches.md](release-branches.md) | How platform-specific releases are built from the one trunk, and the guards that keep the trunk whole |
| [repository-layout.md](repository-layout.md) | Where everything in the repository lives |
| [decisions/](decisions/README.md) | Dated design records: specs, plans, audits and decisions |

