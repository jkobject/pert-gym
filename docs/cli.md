# CLI

The installed console script is `pert-gym`:

```bash
pert-gym --version
pert-gym --help
```

## Commands

### `info`

Print project/package metadata:

```bash
pert-gym info
```

### `check`

Run a quick repository health check for required project files:

```bash
pert-gym check
```

This is intentionally lightweight; it is not a replacement for the pytest/audit validation commands.

### `run`

Run the current default pipeline placeholder with a YAML config path:

```bash
pert-gym run --config config/base.yml
```

Today this command validates/report-selects a config path and warns when it is missing. Treat it as scaffolding for future pipeline wiring, not as a full ingestion or benchmark runner.

### `benchmark-loader-smoke`

Run the canonical model benchmark loader smoke with the mean-control baseline:

```bash
pert-gym benchmark-loader-smoke \
  --manifest artifacts/schema_audit/model_ready_subset_20260621.json \
  --artifact-dir artifacts/model_benchmarks
```

The loader uses the reviewed model-ready v0 manifest when available and falls back to deterministic synthetic data where needed. It writes local benchmark smoke artifacts and should not write Lamin artifacts.

## Non-CLI workflows

Many important project operations are intentionally script/test driven rather than exposed as top-level CLI commands:

```bash
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
uv run python artifacts/schema_audit/validate_unified_collection_queries_20260621.py
uv run python artifacts/scripts/smoke_explore_unified_pertdata_collection.py
uv run python tools/model_env.py list
```

See [Usage](usage.md) and [Isolated model environments](model_environments.md).
