---
name: release-patch
description: Open a patch release PR for prytt (bump version, relock, push, PR).
disable-model-invocation: true
---

1. Run `mise run release:prepare patch`. It branches `release/vX.Y.Z` from an up-to-date `main`, bumps `pyproject.toml`, relocks `uv.lock`, commits and pushes. If it fails, stop and report its output; do not work around it.
2. Open the PR from that branch:

   ```bash
   gh pr create --base main --title "chore: release X.Y.Z" --body "Release vX.Y.Z. Merging tags vX.Y.Z and publishes the GitHub Release."
   ```

   End the body with the attribution line from your system prompt, if one is given.
3. Report the PR URL and the new version.
