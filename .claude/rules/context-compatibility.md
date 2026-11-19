---
name: context-compatibility
description: What a breaking load-context change is, and the one change that is never made in place.
---

# Load-context compatibility

Enforced by: none — judgement.

No test, by choice: a changed default or meaning keeps its key, so no text check sees it.
`/review-pr` is the enforcement. Pipelines outside this repo write the load-context JSON and do
not change when the core is released.

1. **What breaks is defined once**, in the Compatibility table on
   `docs/reference/load-context.md`. A breaking change bumps the major; before 1.0, at least the
   minor.
2. **Never change the default or meaning of an existing field**, in any change after 0.4.0.
   Old JSON would parse and silently produce different output. Add a field under a new name,
   refuse the old one with a migration message (as `GlobalContext.parallel_execution_enabled`
   is), and bump as rule 1 says.
3. **Removing, renaming or moving a field leaves a refusing branch** naming the way on, in a
   strict or a lenient section alike: a lenient one would ignore the old key silently.
