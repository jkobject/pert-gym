# pert-gym

`pert-gym` is a working benchmark and data-curation project for perturbation and temporal-response prediction. The practical question is simple: given a biological system at baseline, can we predict how it moves after a genetic, chemical, temporal, or environmental intervention?

The repository is still under active harmonization. Treat it as a research workspace with a Python package, data-ingestion tools, model baseline experiments, and a local project wiki — not as a polished public PyPI package yet.

## Why it exists

Perturbation modelling only works if the data contract is boring and reliable. `pert-gym` is building that contract around the `laminlabs/pertdata` LaminDB instance so that scRNA-seq perturbation screens, drug-response screens, temporal atlases, and other response datasets can be queried through a common interface.

The current target is a unified collection of dataset triplets:

```text
<dataset_prefix>/obs.parquet   # sample/cell metadata
<dataset_prefix>/X.h5ad        # matrix payload
<dataset_prefix>/var.parquet   # feature/gene metadata
```

`obs -> X -> var` links are stored in Lamin artifact feature links. This keeps metadata queries cheap while allowing matrix loading only when a workflow really needs it.

## Architecture at a glance

- [`src/pert_gym/`](src/pert_gym/) — package code, CLI, model and metric scaffolding.
- [`tools/`](tools/) — ingestion, audit, Lamin connection, query, cache, and benchmark helpers.
- [`data/`](data/) — local dataset catalogue and scratch/raw-data conventions. Durable curated data should live in LaminDB, not in Git.
- [`docs/`](docs/) — mkdocs documentation, schema notes, and model-environment notes.
- [`wiki/pert-gym/`](wiki/pert-gym/) — current project state, harmonization decisions, audit vocabulary, and branch policy.
- [`notebooks/`](notebooks/) — exploratory and validation notebooks.
- `artifacts/` — generated audit reports, manifests, smoke results, and benchmark outputs; this directory is produced locally and is not required to exist in a fresh checkout.

Lamin access should go through `tools.lamin_context.connect_pertdata()`, which explicitly targets `laminlabs/pertdata` on branch `jkobject`. Do not rely on the global Lamin CLI state for this project.

## Current milestone status

The latest source of truth is [`wiki/pert-gym/current-status.md`](wiki/pert-gym/current-status.md).

As of the current status page:

- The canonical query surface is `pert-gym/canonical/20260621`.
- It contains 1,056 canonical `obs.parquet` collection members with validated `obs -> X -> var` triplet links.
- These members represent 120 logical dataset/family rows in the unified manifest.
- A separate `pert-gym/model-ready/20260621` collection exists as a tiny reviewed v0 loader-smoke subset; it is not the same thing as the full canonical query surface.

Be careful with counts: historical values like “110 datasets” or “720/721 triplets” were audit-subset or prefix counts, not the database size. Use the vocabulary in [`wiki/pert-gym/current-status.md`](wiki/pert-gym/current-status.md) when reporting state.

## Minimal setup

```bash
uv venv .venv --python 3.11
source .venv/bin/activate
make install
```

Useful checks while working on package code:

```bash
make lint
make type
make test
```

For the current unified collection query contract, the focused test is:

```bash
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
```

For documentation-only edits, a lighter sanity check is usually enough:

```bash
uv run --extra dev mkdocs build
```

## Where to go next

- [`wiki/pert-gym/current-status.md`](wiki/pert-gym/current-status.md) — latest milestone status, validated counts, and remaining post-P3/model-ready work.
- [`wiki/pert-gym/index.md`](wiki/pert-gym/index.md) — project wiki index for schema, audit, modality, and deduplication decisions.
- [`CLAUDE.md`](CLAUDE.md) — agent-facing operating guide and project-specific pitfalls.
- [`data/README.md`](data/README.md) — dataset catalogue, triplet format notes, and missing-dataset notes.
- [`docs/`](docs/) — user/developer documentation and schema references.
- Current project-state mirror — use [`wiki/pert-gym/current-status.md`](wiki/pert-gym/current-status.md) for detailed validation evidence and remaining post-P3/model-ready work; `TODO.md` may be present in workflow-restoration branches.

## Development notes

This repository contains active data-ingestion machinery. Avoid downloading or caching huge files blindly. Large raw assets should be treated as temporary cache, staged through the project’s GCS/Lamin workflows when needed, and removed after verification.

For new ingestion or audit work, read [`CLAUDE.md`](CLAUDE.md) first. For quick orientation as a human, start with this README and then open [`wiki/pert-gym/current-status.md`](wiki/pert-gym/current-status.md).
