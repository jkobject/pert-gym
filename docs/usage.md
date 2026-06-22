# Usage

Use `pert-gym` in three layers:

1. the package CLI for project health checks and benchmark-loader smokes;
2. the schema/query tools for read-only canonical Collection exploration;
3. isolated model environments for optional model families.

## CLI basics

```bash
pert-gym info
pert-gym check
pert-gym run --config config/base.yml
pert-gym benchmark-loader-smoke \
  --manifest artifacts/schema_audit/model_ready_subset_20260621.json \
  --artifact-dir artifacts/model_benchmarks
```

`run` is currently a lightweight pipeline placeholder that reports the selected config. The more meaningful commands today are `check` and `benchmark-loader-smoke`.

See [CLI](cli.md) for the full current command surface.

## Query the canonical Collection

The schema contract is documented in [Canonical schema contract](pert_gym_schema.md). The current implemented query helpers live under `tools/`, not a stable public `pert_gym.collections` module yet.

Useful read-only checks:

```bash
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
uv run python artifacts/schema_audit/validate_unified_collection_queries_20260621.py
uv run python artifacts/scripts/smoke_explore_unified_pertdata_collection.py
```

The canonical pattern is:

```text
obs.parquet  ->  X.h5ad  ->  var.parquet
```

Load metadata first, then load matrix payloads only when the workflow really needs them. Large PRISM/VIPerturb members should usually stay metadata-only or backed/chunked.

## Run model smokes

Lightweight baseline checks can run from the base/dev environment. Heavier model families use isolated envs:

```bash
uv run python tools/model_env.py list
uv run python tools/model_env.py create classical --dry-run
uv run python tools/model_env.py smoke classical --dry-run
```

When ready to create a real optional env:

```bash
uv run python tools/model_env.py create cpa
uv run python tools/model_env.py smoke cpa
```

Model envs should read model-ready data or deterministic synthetic fallbacks. They must not curate data or write Lamin artifacts. See [Isolated model environments](model_environments.md), [CPA baseline smoke](cpa_baseline.md), and [LPM baseline smoke](lpm_baseline.md).

## Where outputs go

- Audit reports and validation artifacts: `artifacts/schema_audit/`.
- Benchmark smoke outputs: `artifacts/model_benchmarks/` or `artifacts/model-runs/<model>/`.
- Temporary raw data/cache: repo-local cache directories; durable curated data belongs in LaminDB or project GCS staging, not Git.

For active source/blocker status, prefer `wiki/pert-gym/current-status.md` over old artifact logs.
