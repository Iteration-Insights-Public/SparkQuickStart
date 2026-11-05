# Cut a release

At the end of this guide you have a GitHub Release per platform, `v<version>-fabric` and
`v<version>-databricks`, each with the wheel built from that platform's release branch. It is for a
maintainer of this repository with permission to run its workflows. How the release branches are
built, and why, is the architecture page [Release branches](../architecture/release-branches.md).

## Before you start

- The change you want to release is merged to `main`, and the test workflow passed on that push.
  The sync to `release/fabric` and `release/databricks` runs only after a passed run.
- The `version` in `pyproject.toml` on `main` is the version you are releasing. The release
  workflow takes the version as an input and does not commit it back.
- The GitHub CLI, `gh`, is signed in to this repository. Every step can also be run from the
  Actions tab.

## Steps

1. **Run the platform-filtered builds on `main`.** Nothing runs them automatically, and the
   release workflow runs no tests, so this is the only test of what each platform's wheel holds.

   ```bash
   gh workflow run platform-builds.yaml -f platform=both --ref main
   ```

   Wait for both jobs, `Verify platform-filtered build (fabric)` and
   `Verify platform-filtered build (databricks)`, to pass. Run it on `main`, never on a release
   branch, which is already filtered.

2. **Check that both release branches carry `main`'s tip.**

   ```bash
   git fetch origin
   git rev-parse origin/main
   git log -1 --format=%s origin/release/fabric
   git log -1 --format=%s origin/release/databricks
   ```

   Each subject reads `chore: sync release/<platform> from main@<sha>`, with the SHA `main` has.

3. **Publish each platform.** Dispatch the release workflow from that platform's branch, with the
   matching `platform` input.

   ```bash
   gh workflow run release.yaml --ref release/fabric -f platform=fabric -f version=<version>
   gh workflow run release.yaml --ref release/databricks -f platform=databricks -f version=<version>
   ```

   Add `-f release_notes="..."` to give the release a body.

## Check it worked

```bash
gh release view v<version>-fabric
gh release view v<version>-databricks
```

Each lists one asset, `spark_quick_start-<version>-py3-none-any.whl`. The install line in the
README's [Installation](../../README.md#installation) section downloads it from there.

## When it goes wrong

**`This workflow must be run from branch 'release/fabric', not 'main'.`** The release workflow was
dispatched from the wrong branch, which would publish an unfiltered wheel under a platform tag.
Dispatch it again with `--ref release/<platform>` and the matching `platform` input.

**A release branch's subject names an older SHA than `main`.** The sync has not run for `main`'s
tip: the test workflow on that push failed, was cancelled or has not finished. Fix or wait for the
run on `main`; the sync follows a passed one.

**The sync fails at `Verify main is the complete trunk` with `ERROR: this checkout is not the
complete platform-neutral trunk.`** A platform's files are missing from `main`, listed under the
error. Do not release: restore them on `main` first. The
[guards](../architecture/release-branches.md#the-guards) explain how this happens and what
catches it.

This guide was written from the workflow files; its commands were not run when it was written,
because they publish.

## See also

- [Release branches](../architecture/release-branches.md), what the branches are and the guards on them
- [Set up for development](set-up-for-development.md), the checks to run before a change reaches `main`
