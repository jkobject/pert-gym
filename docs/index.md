# pert-gym documentation

`pert-gym` is a research workspace for perturbation and temporal-response prediction. It combines a Python package, LaminDB curation tools, schema audits, model baseline smokes, and a project wiki around one practical question: can we predict how a biological system moves after an intervention?

Start here if you want the human-facing map. Agent operating rules and safety constraints live in `CLAUDE.md`; this documentation is for users and developers reading the project as a repository or rendered docs site.

## Current status in one paragraph

The current canonical query surface is the dated Lamin Collection `pert-gym/canonical/20260621` on `laminlabs/pertdata`, branch `jkobject`. It contains 1,056 canonical `obs.parquet` collection members with validated `obs -> X -> var` links and represents 120 logical dataset/family rows. The `pert-gym/model-ready/20260621` Collection is separate and currently contains one tiny reviewed loader-smoke member; do not treat the whole canonical query surface as model-ready training data. For latest counts and caveats, read `wiki/pert-gym/current-status.md`.

## Public / human-facing docs

These pages should be safe to render as user/developer documentation:

- [Getting started](getting-started.md) — install the project and run quick checks.
- [Usage](usage.md) — query data, run benchmark loader smokes, and use models without mutating Lamin.
- [CLI](cli.md) — actual `pert-gym` command surface.
- [Configuration](configuration.md) — config files, model environment specs, and artifact paths.
- [Project structure](structure.md) — repository layout and which directories are durable vs scratch.
- [Canonical schema contract](pert_gym_schema.md) — triplet schema, Collection contract, harmonization levels, and query UX target.
- [Isolated model environments](model_environments.md) — dependency isolation for classical/LPM/CPA/chemCPA/legacy-trVAE/replacement/scPRAM work.
- [trVAE replacement](trvae_replacement.md) — maintained conditional VAE analogue replacing the blocked TensorFlow 1.x trVAE route.
- [LPM baseline smoke](lpm_baseline.md) and [CPA baseline smoke](cpa_baseline.md) — model-specific smoke notes and limitations.
- [API reference](api.md) — generated package reference plus notes on implemented vs target APIs.
- [Development](development.md) — code quality, docs, and safe validation commands.

## Internal / status references

The following files are not a replacement for this docs site, but they are the right place for active project state:

- `README.md` — repository overview and minimal setup.
- `TODO.md` — compact Kanban/project-state mirror.
- `wiki/pert-gym/index.md` — detailed wiki map for schema, audit vocabulary, modality policy, and deduplication policy.
- `data/README.md` — dataset catalogue and source notes.
- `CLAUDE.md` — agent-facing operating guide; keep agent instructions there, not in rendered docs.

## Count vocabulary

Use precise terms:

- latest visible Lamin artifacts: all latest artifact records visible on branch `jkobject` at the relevant audit time;
- canonical collection members: `obs.parquet` artifacts in the current canonical Collection;
- logical datasets / families: grouped biological/source dataset rows;
- triplet prefixes / chunks: represented obs/X/var prefixes;
- model-ready members: reviewed loader-smoked subset only.

Historical `110`/`111` and `720`/`721` values were audit/logical/prefix counts, not database size. Prefer the vocabulary and numbers in `wiki/pert-gym/current-status.md`.
