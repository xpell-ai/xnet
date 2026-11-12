# XNet instruction bootstrap

XNet is an independent repository. Verify the requested target and Git root
before checking its branch/status or staging files. Never use a parent or home
repository as this package's Git target; preserve existing edits.

In a coordinated checkout, verify that `../../pnpm-workspace.yaml` lists
`packages/xnet` resolving to this repository and that `../../AGENTS.md` identifies
the intended Xpell workspace. Only then explicitly read that workspace guidance,
unless already supplied/read and retained for this task. This pointer is not an
automatic include or authority to override the user's scope.

For a standalone clone or missing/unrelated parent, use this repository's README,
manifest, public exports, source, relevant tests and maintained local contracts.
Do not search arbitrary ancestors. Report missing required context before
dependent work; do not invent contracts.

Select available Skills from catalog metadata by task: `xpell-contract` for Xpell
coding/design, then only relevant layer/feature guidance. Do not preload the
catalog, infer availability from disk, or install missing Skills implicitly.

Read-only bootstrap must not initialize stores, start services or call providers.
Use `aime-collaboration` and its live sources for an authorized shared Aime task;
ordinary local package work does not trigger company workflow or external writes.
