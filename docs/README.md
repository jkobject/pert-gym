# Documentation index

`docs/` is the single home for durable pert-gym knowledge. The root files have deliberately narrow roles:

- [`../README.md`](../README.md) — public GitHub façade: purpose, installation, first use, contribution, and navigation.
- [`../AGENTS.md`](../AGENTS.md) — short agent boot file and non-negotiable safety rules.
- [`../TODO.md`](../TODO.md) — current project state, active work, blockers, and next steps.
- [`README.md`](README.md) — this durable documentation map.

The former parallel documentation tree was audited and integrated here so policy, runbooks, scientific provenance, and project knowledge no longer compete with a second documentation root. [`index.md`](index.md) remains the MkDocs landing page.

## Start by goal

### Install, use, or contribute

- [Getting started](getting-started.md) — environment setup and quick checks.
- [Usage](usage.md) — read-only query and benchmark examples.
- [CLI](cli.md) and [API](api.md) — implemented interfaces.
- [Configuration](configuration.md) — configuration and artifact locations.
- [Development](development.md) — quality gates and contribution workflow.
- [Project structure](structure.md) — durable, generated, and local-only paths.

### Understand the data and scientific contract

- [Canonical schema and Collection contract](pert_gym_schema.md) — binding obs/X/var, modality, provenance, and harmonization contract.
- [Dataset catalogue](../data/README.md) — source-level scientific provenance and dataset notes.
- [Dataset modality policy](project/dataset-modalities.md) — modality-specific decisions.
- [Deduplication policy](project/deduplication-policy.md) — pre-ingestion duplicate/subduplicate gates.
- [Lamin audit and branch model](project/lamin-audit-and-branch-model.md) — branch, counting, and integrity vocabulary.

### Operate or resume the project

- [Current status](project/current-status.md) — latest validated status/count snapshot; use [`../TODO.md`](../TODO.md) for active work.
- [Agent runbook](project/agent-runbook.md) — detailed GCP/GCS/Lamin placement, safety, and validation recipes.
- [Harmonization roadmap](project/harmonization-roadmap.md) — durable phased strategy.
- [Model and benchmark roadmap](project/model-roadmap.md) — model-ready criteria and interpretation limits.
- [Model environments](model_environments.md) and baseline notes ([CPA](cpa_baseline.md), [LPM](lpm_baseline.md), [trVAE replacement](trvae_replacement.md)).
- [Archive](archive/) — dated historical context only; never treat it as current instructions.

## Documentation audit decisions

| Previous surface | Decision | Canonical destination / rationale |
|---|---|---|
| Root `README.md` | **KEEP + EDIT** | Public project façade; operational detail moved behind links. |
| Root `AGENTS.md` | **KEEP + EDIT** | Single short boot file; detailed recipes live in `project/agent-runbook.md`. |
| Root `TODO.md` | **KEEP + EDIT** | Current state only; durable policy and historical evidence live below `docs/`. |
| `docs/index.md` | **KEEP** | Rendered MkDocs home; links back to this fuller repository index. |
| Former schema-contract page | **MERGE / DELETE** | Redundant summary absorbed by binding `pert_gym_schema.md`. |
| Former status, audit, modality, dedup, and roadmap pages | **MERGE** | Unique durable content retained under `project/`. |
| Former agent-context page | **MERGE + RENAME** | Preserved as `project/agent-runbook.md`; separates boot rules from procedures. |
| Dated historical pages | **ARCHIVE** | Preserved under `archive/` with provenance intact. |
| Former documentation index | **MERGE / DELETE** | Replaced by this index; no parallel knowledge root remains. |

## Maintenance rules

1. Put public orientation in the root `README.md`, not status logs.
2. Keep `AGENTS.md` short; put commands and operational recipes in the agent runbook.
3. Keep `TODO.md` current and compact; move durable decisions into the appropriate `docs/` page.
4. Preserve source accessions, citations, hashes, artifact paths, and dated evidence when consolidating scientific documentation.
5. Keep generated reports and run logs under `artifacts/`; link them instead of copying their contents into docs.
6. Update links when renaming pages and run the repository Markdown link check plus `mkdocs build` before review.
