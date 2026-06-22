# API

This page is the generated package-reference entry point plus a short note about which APIs are stable today.

## Implemented package surface

The current importable package includes:

- `pert_gym.cli` — CLI parser and command handlers.
- `pert_gym.benchmarks` — benchmark loader smoke helpers and artifact writing.
- `pert_gym.evaluate` and `pert_gym.metrics` — lightweight evaluation protocol and metrics.
- `pert_gym.models` — baseline, classical, LPM, and CPA-style smoke model classes.

Generated reference:

::: pert_gym

## Schema/query contract status

The canonical data contract is documented in [Canonical schema contract](pert_gym_schema.md). Some examples there describe the intended notebook/API UX, including future helpers such as `load_collection_manifest` and `load_member_adata`. Those helpers are not currently exposed as a stable `pert_gym.collections` module.

For current Collection validation and exploration, use the repository tools/tests:

```bash
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
uv run python artifacts/schema_audit/validate_unified_collection_queries_20260621.py
uv run python artifacts/scripts/smoke_explore_unified_pertdata_collection.py
```

Project-specific Lamin access should go through `tools.lamin_context.connect_pertdata()` rather than global Lamin CLI state.
