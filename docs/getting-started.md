# Getting started

`pert-gym` is an active research repository, not a polished PyPI package. The default workflow is local development with `uv`, a repo-local virtualenv, and explicit LaminDB connection helpers.

## Prerequisites

- Python 3.10+; Python 3.11 is the normal project environment.
- [`uv`](https://docs.astral.sh/uv/) installed.
- Access to the repository checkout.
- For Lamin-backed data queries: access to `laminlabs/pertdata`, branch `jkobject`.

## Install the base development environment

From the repository root:

```bash
uv venv .venv --python 3.11
source .venv/bin/activate
make install
```

The base environment carries LaminDB, ingestion/audit tools, and lightweight package tests. Heavy perturbation-model stacks are intentionally isolated; see [Isolated model environments](model_environments.md).

## Quick checks

```bash
pert-gym info
pert-gym check
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
```

For documentation-only work:

```bash
uv run --extra dev mkdocs build
```

## First files to read

1. `README.md` — project overview and current milestone summary.
2. [`index.md`](index.md) — docs map and count vocabulary.
3. [`pert_gym_schema.md`](pert_gym_schema.md) — canonical schema and Collection contract.
4. `wiki/pert-gym/current-status.md` — latest validated counts and remaining work.
5. `data/README.md` — dataset catalogue and source notes.

If you are running an ingestion or agent task, also read `CLAUDE.md` first. It contains operational guardrails such as the required Lamin connection pattern, branch safety rules, and cache handling.

## Lamin connection rule

Do not rely on global Lamin CLI state. Project code should connect through:

```python
from tools.lamin_context import connect_pertdata

ln = connect_pertdata()
```

The intended instance/branch is `laminlabs/pertdata` on branch `jkobject`.
