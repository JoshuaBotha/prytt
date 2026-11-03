---
name: release-major
description: Open a major release PR for prytt (bump version, relock, push, PR with release notes).
disable-model-invocation: true
---

1. Run `mise run release:prepare major`. It branches `release/vX.Y.Z` from an up-to-date `main`, bumps `pyproject.toml`, relocks `uv.lock`, commits and pushes. If it fails, stop and report its output; do not work around it.
2. Gather what ships since the last release: the latest tag is `git describe --tags --abbrev=0 HEAD~1`; list `git log --oneline <tag>..HEAD`, then read each merged PR's body with `gh pr view <n>` (squash titles end in `(#n)`). Done when every commit since the tag is accounted for.
3. Write the PR body for someone deciding whether to upgrade:
   - **Summary**: two or three sentences on what this major version is.
   - **Breaking changes**: each break (`!` titles, behaviour or config changes, dropped support), what fails for an existing user, and the migration step. Say so plainly if there are none.
   - **Highlights**: new features and notable fixes, one line each, with PR links.
   - **Upgrade**: the install command for the new tag and any config changes to make.
   - **Merge Danger**: merging tags `vX.Y.Z` and publishes the GitHub Release; a published release can't be unpublished cleanly.
   - End with the attribution line from your system prompt, if one is given.
4. Open the PR: `gh pr create --base main --title "chore!: release X.Y.Z" --body-file <file>`, with the body in a temp file.
5. Report the PR URL, the new version, and the breaking changes you listed.
