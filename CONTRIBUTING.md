# Contributing to prytt

This guide is for everyone who changes prytt, people and coding agents alike.
Agents also follow [For coding agents](#for-coding-agents) at the end.

## Setup

prytt uses [mise](https://mise.jdx.dev) for tools, tasks and env vars, so a
task runs the same locally and in CI.

```bash
mise run setup:dev   # installs Python 3.13 + uv, syncs deps, warms Sail
mise run test        # unit + acceptance tests
mise run ci          # everything CI runs, in strict mode
```

`mise tasks` lists every task. `SPARK_REMOTE` must be unset when tests run;
mise unsets it for you, so run tests through `mise run` rather than bare
`pytest`.

## Workflow

1. Branch from an up-to-date `main` (see [Branches](#branches)).
2. Write a failing test first, then make it pass.
3. Update the docs in the same PR when user-visible behaviour changes
   (`docs/user/`, and `README.md` if the quick start changes). The
   [developer guide](docs/developer/guide.md#docs) says which page covers what.
4. Run `mise run ci` until it passes.
5. Open a PR with a [Conventional Commit title](#pr-titles-and-commits) and
   fill in the template.
6. Squash-merge once all three CI checks (Linux, macOS, Windows) are green.

Keep each PR to one concern. A refactor that a fix needs goes in its own PR
first.

## Branches

Name branches `<type>/<short-description>`, lowercase kebab-case:

```
feat/job-bookmarks
fix/dropnullfields-nested-struct
docs/iceberg-limitations
```

| Type       | Use for                                                  |
| ---------- | -------------------------------------------------------- |
| `feat`     | New behaviour users can see                              |
| `fix`      | A bug fix                                                |
| `docs`     | Docs only (`README.md`, `docs/`, this file, `CLAUDE.md`) |
| `test`     | Tests only, no behaviour change                          |
| `refactor` | Code change with no behaviour change                     |
| `perf`     | A speed or memory improvement                            |
| `build`    | Packaging, dependencies, `pyproject.toml`, `uv.lock`     |
| `ci`       | Workflows, `.mise/tasks`, repo settings                  |
| `chore`    | Anything else, including release version bumps          |

Release branches are the exception: `release/vX.Y.Z`, made by
`mise run release:prepare` (see [Releasing](#releasing)).

`main` is protected: changes land only through a PR with green checks.
Branches are deleted automatically after merge.

## PR titles and commits

`main` is squash-merged and the commit title is the PR title, so the PR
title is what history keeps. Write it as a
[Conventional Commit](https://www.conventionalcommits.org):

```
<type>(<optional scope>): <imperative summary, lowercase, no full stop>

feat(transforms): apply Filter locally
fix: return None from getResolvedOptions for a missing arg
feat(config)!: rename [tool.prytt] s3_root to s3.root
```

- The type matches the branch type.
- Scope is optional: a module or area such as `transforms`, `catalog`,
  `iceberg`, `hooks`, `config`.
- `!` after the type or scope marks a breaking change. Explain the break and
  the migration in the PR body.

Commits on the branch are squashed, so their messages are free-form. Keep
them readable for reviewers.

## Tests

- **Unit** (`tests/unit`, `mise run test:unit`): one module's behaviour, in
  process.
- **Acceptance** (`tests/acceptance`, `mise run test:acceptance`): runs Glue
  scripts as subprocesses so the real `prytt.pth` import hook is exercised.
  Use these for anything that touches start-up, hooks, or what a job script
  sees.
- Every bug fix comes with a regression test that fails without the fix.
- Tests that create an extra `SparkSession` stop it.
- A test that hangs is killed by `pytest-timeout`, with every thread's stack
  dumped at 60 s. Read that dump before retrying.

## Code

- Match the style of the code around you: naming, comment density, idiom.
- Comments explain why, not what.
- Code under `_bootstrap` / `_hooks` runs at every interpreter start: stdlib
  only, with the import limits listed in `CLAUDE.md`.
- When a Glue feature can't work locally, log it and pass data through
  rather than failing (unless strict mode), and add it to
  `docs/user/compatibility.md`.
- Pin GitHub Actions to a full commit SHA with a version comment.
  Dependabot keeps actions and `uv` dependencies current.

## Releasing

prytt follows [Semantic Versioning](https://semver.org). While it is 0.x, a
breaking change bumps the minor version, everything else the patch version.

To release, run `mise run release:prepare patch|minor|major` from a clean
tree. It branches `release/vX.Y.Z` from an up-to-date `main`, bumps `version`
in `pyproject.toml`, relocks `uv.lock`, updates the prytt version in
`README.md` and `docs/user/` (install URLs, wheel name, startup log line),
commits and pushes. Then open a `chore: release X.Y.Z` PR from that branch
and merge it. Before releasing, check that every `feat` and `fix` since the
last tag is in the docs; fix any gap in a `docs` PR first. In Claude Code,
`/release-patch`, `/release-minor` and `/release-major` do all three steps;
each writes release notes into the PR body: changes with PR links, breaking
changes with migration, and the docs check. The Release workflow runs
`mise run release:publish`, which tags `vX.Y.Z` and publishes a GitHub
Release with the wheel and sdist. If that version is already published it
does nothing, so merging unrelated changes never re-releases. If a release
run fails, fix the cause and use "Run workflow" on the Release workflow (on
`main`); a tag without a published release, or a leftover draft, is retried.

## For coding agents

Everything above applies. In addition:

- Read `CLAUDE.md` before changing code. Its gotchas each cost a debugging
  session to learn.
- Start every task on a new branch named by the convention above, created
  from an up-to-date `main`. Commit only to that branch.
- Work test-first: see the new test fail (red) for the expected reason
  before writing the fix.
- A task is done when `mise run ci` passes. Report a failure with its
  output, and say plainly which steps you skipped.
- Commit, push, open a PR, or bump the version only when asked. Open PRs as
  drafts unless told otherwise.
- Stay inside the task's scope. Note unrelated problems you find in your
  report instead of fixing them.
- When you learn something about prytt, Sail or pyspark-client that took more
  than one attempt to work out, add it to the gotchas in `CLAUDE.md` in the
  same PR.
