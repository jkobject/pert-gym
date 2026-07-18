# pert-gym agent boot

This is the single required repository boot file. Do not ask workers to read a second root boot file.

## Project gist

`pert-gym` is a benchmarking and data-curation project for perturbation and temporal-response prediction. It builds a reliable data contract around `laminlabs/pertdata` so scRNA-seq perturbation screens, drug-response screens, temporal atlases, spatial transcriptomics, imaging phenotypes, and related response datasets can be queried and benchmarked through explicit Collection and payload contracts.

## Source hierarchy and context diet

- `README.md` — public project façade: installation, usage, architecture, contribution.
- `TODO.md` — current operational state, active work, blockers, and next steps.
- `docs/project/current-status.md` — latest compact status/count snapshot.
- `docs/project/agent-runbook.md` — detailed Lamin/GCS safety, routing, and validation commands.
- `docs/pert_gym_schema.md` — binding schema and unified Collection contract.
- `docs/README.md` — durable documentation index.
- `data/README.md` — dataset catalogue and scientific source notes.

Do not browse the repository broadly by default. Read this file, the Kanban card context packet, `TODO.md`, and only the task-specific docs named by the card or runbook. If required context is still missing, block with the exact gap instead of mining historical logs. `wiki/` is obsolete; do not recreate it.

Kanban cards must state the biological dataset/source, allowed writes, exact Collection/payload target, duplicate checks, validation commands, relevant files/docs, expected evidence, and what not to read.

## Durable data and execution boundary

- `laminlabs/pertdata` is the durable system of record. Use `tools.lamin_context.connect_pertdata()` on branch `jkobject`; never use the global Lamin CLI.
- Project-owned GCS is temporary staging, not a durable dependency or deletion target by default.
- Do not place large payloads or GCS caches on the Mac. Run payload, GCS, and broad Lamin work only on the EU VM (`pert-gym-worker-eu`, `europe-west1-b`). Do not silently fall back to Mac-local bulk reads.
- `gs://scperturb` is in `EUROPE-WEST1` with Requester Pays; direct access must use billing project `jkobject-1549353370965`.
- The target for large matrices is adaptive sparse-Zarr with shared-var support. Retain legacy triplets until their accepted replacement has parity/readback evidence and a rollback identity.
- Never write Lamin `main`; never touch TxGNN/Jouvence resources.

## Publication and reproducibility

- One logical dataset has one writer. Publication is append-only and crash-recoverable; do not bypass the journal/recovery stages.
- Per-dataset processing-decision notebooks are the reconstruction layer. They distinguish accepted facts, current state, and pending work and record source identity/checksums, code lineage, validation, and temporary GCS dependencies.
- No deletion is allowed without accepted artifact UIDs/Collections, source-to-Lamin parity, remote readback, an executable dataset notebook, and an immutable upstream checksum or retained Lamin raw source.
- Final GCS exit is gated by a reviewed machine-readable `GCS_DECOMMISSION_READY` manifest, never prose alone.
- Missing applicable metadata remains `unknown`; genuinely inapplicable metadata is `not_applicable`. Never invent values and never convert local missingness into a global publication block.

## Collection and payload contract

Canonical expression data is resolved through explicit links:

```text
obs -> X -> var
```

Most members use same-prefix triplets:

```text
<dataset_prefix>/obs.parquet
<dataset_prefix>/X.h5ad
<dataset_prefix>/var.parquet
```

Reviewed exact-hash chunk families may link to a dataset-level shared var alias. Always follow Lamin feature links; never infer linked keys by string replacement.

Keep count vocabulary explicit: biological datasets/publications, logical families, physical members, Collection members, triplet prefixes/chunks, and model-ready members are different denominators.

## Git and review workflow

The standalone repository is `https://github.com/jkobject/pert-gym.git`. The shared checkout at `/Users/jkobject/.openclaw/workspace/work/pert-gym` is for read-only inspection, cache/materialized data, and emergency operations. Implementation and model-code work uses isolated worktrees under `/Users/jkobject/.openclaw/worktrees/pert-gym/<task-id>` and a reviewable PR.

Do not pile implementation edits into the shared checkout. Commit only code, docs, tests, config, and reviewable manifests—not raw data, caches, generated sites/logs, or local model-ready exports.

A single independent verifier normally combines behavioral/live tests and semantic/provenance review. Split tester and reviewer only when evidence, expertise, permissions, or environments genuinely differ. `goal_mode` is selective and does not replace independent review.

## Role routing

- CTO/orchestrator: `TODO.md`, then current-status/schema only as required.
- Ingestion/Lamin worker: card packet, `docs/project/agent-runbook.md`, source-specific `data/README.md`, and schema contract.
- Model/benchmark worker: card packet plus the named model-environment/baseline docs.
- Docs-only worker: target docs only; no code/data writes.
