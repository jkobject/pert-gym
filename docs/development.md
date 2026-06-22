# Development

This repository is both a Python package and an active data-curation workspace. Keep code changes, documentation changes, data ingestion, and model-environment work clearly separated.

## Common package checks

```bash
make fmt
make lint
make type
make test
```

Focused checks for the current unified Collection query contract:

```bash
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
uv run python artifacts/schema_audit/validate_unified_collection_queries_20260621.py
uv run python artifacts/scripts/smoke_explore_unified_pertdata_collection.py
```

For docs-only edits:

```bash
uv run --extra dev mkdocs build
```

## Development boundaries

- Keep heavy model dependencies out of the base environment. Add model-family dependencies to `config/model_envs.toml` and test through `tools/model_env.py`; see [Isolated model environments](model_environments.md).
- Do not write Lamin artifacts from benchmark/model smoke code. Model code should read local manifests/data and write local artifacts only.
- Use `tools.lamin_context.connect_pertdata()` for project Lamin access. Do not depend on global Lamin CLI state.
- Treat old `/home/ubuntu/...` paths in logs as historical. The current Mac workspace path is documented in `CLAUDE.md`, but human docs should normally use repo-relative paths.
- Put agent-operational instructions in `CLAUDE.md`, not in rendered docs pages.

## Documentation workflow

Human-facing docs live in `docs/*.md`. The detailed project wiki lives under `wiki/pert-gym/`, and active status is summarized in `wiki/pert-gym/current-status.md`.

When updating docs:

1. Prefer repo-relative paths and links.
2. Use the exact count vocabulary from [Documentation index](index.md) and current status.
3. Link to generated audit artifacts only when they are useful evidence; avoid turning docs into a run log.
4. Run `uv run --extra dev mkdocs build` when feasible.

## Release flow

The scaffolded release command is:

```bash
make release
```

Only use it when the repository metadata, changelog expectations, and target remote are intentionally configured. The current repository is still an active research workspace, so routine docs/model/ingestion work should not assume a public release process.
