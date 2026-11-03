---
name: release-minor
description: Open a minor release PR for prytt (check docs, bump version and doc refs, relock, push, PR with release notes).
disable-model-invocation: true
---

1. Check the docs cover what ships, before anything is branched or pushed. Run `git fetch origin --tags`; the last release is `git describe --tags --abbrev=0 origin/main`; list `git log --oneline <tag>..origin/main`. For each `feat` or `fix` commit, read its PR with `gh pr view <n>` (squash titles end in `(#n)`) and confirm the user-visible change is described where the [developer guide](docs/developer/guide.md#docs) says it belongs: `docs/user/`, `docs/user/compatibility.md` for real vs stubbed, `README.md` if the quick start changed. If anything is missing or out of date, stop: list each gap with its PR and ask whether to fix it in a `docs/` PR first. Don't release with known gaps. Note any breaking change (`!` titles, behaviour or config changes, dropped support): from 1.0 on a minor can't ship one, so stop and say it needs `/release-major`.
2. Run `mise run release:prepare minor`. It branches `release/vX.Y.Z` from an up-to-date `main`, bumps `pyproject.toml`, relocks `uv.lock`, rewrites the prytt version in `README.md` and `docs/user/` (install URLs, wheel name, startup log line), commits and pushes. If it fails, stop and report its output; do not work around it. Then `git grep -n <old version> -- README.md docs/user` should find no prytt version left; report any hit.
3. Write the PR body, from the PRs read in step 1, for the person merging:
   - **Summary**: one or two sentences on what this release brings.
   - **Changes**: each `feat` and `fix`, one line, with its PR link; group `docs`, `ci`, `build` and the like on one line each.
   - **Breaking changes**: each break, what fails for an existing user, and the migration step (while prytt is 0.x a minor release may break). Say "None" plainly if there are none.
   - **Docs check**: the result of step 1, naming where each `feat`/`fix` is documented, and that the version refs now say X.Y.Z.
   - **Merge Danger**: merging tags `vX.Y.Z` and publishes the GitHub Release; a published release can't be unpublished cleanly.
   - End with the attribution line from your system prompt, if one is given.
4. Open the PR: `gh pr create --base main --title "chore: release X.Y.Z" --body-file <file>`, with the body in a temp file.
5. Report the PR URL, the new version, and the changes you listed.
