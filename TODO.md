# pert-gym TODO / Kanban mirror

_Last updated: 2026-06-22 by Kanban task `t_51be75dd`._

This file is a compact project-state mirror for agent handoffs. Keep detailed run logs in Kanban, `wiki/`, `docs/`, or dated artifacts.

## Repository / PR workflow status

- Canonical Git strategy: `pert-gym` is a standalone repo at `https://github.com/jkobject/pert-gym.git`, not a subdirectory of the broader OpenClaw workspace repo.
- Shared Mac checkout: `/Users/jkobject/.openclaw/workspace/work/pert-gym`. Use it for inspection, cache/data materialization, and emergency ops; do not let implementation edits accumulate there by default.
- Future implementation/model-code Kanban cards should use isolated worktrees under `/Users/jkobject/.openclaw/worktrees/pert-gym/<task-id>` and branches that include the task id.
- Code/docs/tests/config changes should be committed and opened as PRs before a worker marks code work done.
- Keep raw data, `data/source_cache/`, `data/temporal_pretraining_sources/`, Lamin caches, virtualenvs, `.omx/`, generated artifacts, and local model-ready `.h5ad` exports out of Git unless a task explicitly asks for a tiny fixture or manifest.

## Current OPS2 inventory snapshot

- The shared checkout Git metadata was restored on 2026-06-22 from `origin/main` without overwriting worker files.
- Broken prior metadata backup in shared checkout: `.git.broken-pre-ops2-20260622T172255`.
- After restoration, `git status` showed tracked worker changes across docs, data catalogues, package code, tools, notebook, and `uv.lock`, plus untracked raw/source-cache data. Those changes were intentionally not reverted or bulk-committed by OPS2.

## Temporal SCP browser-auth retry status

- T-SCPAUTH-Mac (`t_a469ca1d`) attempted the required OMX/logged-in Chrome retry for residual SCP rows (`SCP3301`, `SCP1467`, `SCP211`, `SCP3697`, `SCP282`, `SCP499`). No files were recovered: OMX reached the Chrome-extension skill but `node_repl/js` failed before browser access with `codex/sandbox-state-meta sandboxCwd must be an absolute file URI`.
- Evidence artifacts: `artifacts/schema_audit/scp_browser_auth_recovery_20260622.{md,json}` and `artifacts/schema_audit/scp_browser_auth_smoke_20260622.json`. Treat prior headless SCP HTTP 401 as still retryable after the OMX/Chrome bootstrap is fixed; do not mark these datasets terminal from this failed tooling run.
