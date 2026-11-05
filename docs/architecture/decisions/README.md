# Decisions — dated design records

Design documents, plans, audits and explorations, in date order, oldest first, by the date in the file name. Tracked,
because they are the record; `.claude/notes/` is deliberately gitignored scratch (see
`.gitignore:96-98`). Moved here from `docs/agentic-setup/` on 2026-09-27 (F53); the records
themselves still cite their old paths, as history.

| File | What it is |
|---|---|
| [2026-09-25-architecture-audit.md](2026-09-25-architecture-audit.md) | Full read of the built architecture. The evidence base for everything else. |
| [2026-09-25-agentic-workflow-design.md](2026-09-25-agentic-workflow-design.md) | The spec: thin `CLAUDE.md`, rules paired with tests, backlog, `/fix` loop. |
| [2026-09-25-agentic-workflow-plan.md](2026-09-25-agentic-workflow-plan.md) | The fifteen-task implementation plan for that spec. |
| [2026-09-27-documentation-architecture.md](2026-09-27-documentation-architecture.md) | Four documentation homes, doc skills, the model policy and enforcement; amends the 2026-09-25 design. |
| [2026-09-29-declarative-query-definition-design.md](2026-09-29-declarative-query-definition-design.md) | The Declarative Query Definition: a one-source, per-row source step on a new expression library, with a `hwm_context` section shared with staging loads. |
| [2026-10-01-key-identity-exploration.md](2026-10-01-key-identity-exploration.md) | Explored, not adopted: the business key and the row key as one identity, the consumer's join across source systems, and what a Data Vault satellite would need. |
| [2026-10-01-scd-version-ownership-exploration.md](2026-10-01-scd-version-ownership-exploration.md) | Explored, not adopted: moving the version out of the change detector, and `effective_from` instead of a version as the SCD2 row identity. |
| [2026-10-01-data-vault-design.md](2026-10-01-data-vault-design.md) | Data Vault targets, first increment: a hub and a satellite as new transformers and factories, chosen by `scd_type` in the existing `scd_context`; insert-only, hash key built upstream, hash diff by the library. |
| [2026-10-01-data-vault-link-design.md](2026-10-01-data-vault-link-design.md) | Data Vault links, accepted: two thin steps sharing the hub writer's logic, `link_hub_key_columns` in `scd_context`, insert-only on the link hash key, no load-time referential check. |
| [2026-10-05-extensions-context-design.md](2026-10-05-extensions-context-design.md) | `extensions_context` for the user's own data and `platform_context` for the paths the library resolves; `global_context` refuses unknown keys and `custom_params` goes. Accepted; the validation hook and a result channel deferred. |
| [2026-10-15-public-release-readiness.md](2026-10-15-public-release-readiness.md) | Going public: Iteration Insights holds the MIT copyright, the agent tooling stays public and scrubbed, the README says Public Preview / Beta, installs come from a GitHub Release wheel; the order of the work. |
| [2026-10-28-open-core-publication-model.md](2026-10-28-open-core-publication-model.md) | Open-core: private core (library + agentic KB) and private enterprise repos, a clean public export of the library with the agentic KB kept private; SemVer as the contract. Amends the 2026-10-15 decision. |

**The always-on instruction file is `CLAUDE.md` at the repo root, not here.** Claude Code
loads the root file for every session and a nested one only for work inside its own subtree,
so the live instructions cannot move into this directory.

Working state — what is open, in flight, or landed — lives in `.claude/backlog.md`.
