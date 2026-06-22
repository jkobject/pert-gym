# Project structure

`pert-gym` is organized as a research repository with package code, data-curation tools, generated artifacts, and a local project wiki.

```text
.
├── README.md                    # human project overview
├── TODO.md                      # compact Kanban/project-state mirror
├── CLAUDE.md                    # agent-facing operating guide
├── pyproject.toml               # package metadata and optional dependency groups
├── mkdocs.yml                   # rendered docs configuration
├── config/                      # pipeline configs and model environment specs
├── data/                        # dataset catalogue, local source notes, raw/cache conventions
├── docs/                        # human-facing rendered documentation
├── notebooks/                   # exploratory and validation notebooks
├── src/pert_gym/                # package code, CLI, models, metrics, evaluation
├── tests/                       # package and query-contract tests
├── tools/                       # Lamin, query, ingestion, cache, audit, and model-env helpers
├── artifacts/                   # generated audits, manifests, logs, and model smoke outputs
└── wiki/pert-gym/               # durable project knowledge and current status
```

## Durable vs generated

Durable source-of-truth files:

- `README.md`, `TODO.md`, `CLAUDE.md`.
- `docs/*.md` for rendered documentation.
- `wiki/pert-gym/*.md` for detailed project state and policy.
- `src/`, `tests/`, `tools/`, `config/`.
- `data/README.md` and curated catalogues/manifests.

Generated or run-specific outputs:

- `artifacts/schema_audit/` reports and JSON/TSV manifests.
- `artifacts/logs/` command logs.
- `artifacts/model_benchmarks/` and `artifacts/model-runs/` outputs.
- repo-local cache directories such as `.lamin-cache/`, `data/gcs_cache/`, and optional `.venv-models/` environments.

Large raw assets should not become Git documentation. Stage or curate them through the project Lamin/GCS workflow and document only the durable identifiers, status, and validation evidence.

## Documentation surfaces

- Use `README.md` for quick human orientation.
- Use [`index.md`](index.md) for the docs-site map.
- Use `wiki/pert-gym/current-status.md` for latest counts and validation status.
- Use `CLAUDE.md` for agent-specific execution rules.
