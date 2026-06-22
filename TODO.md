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
