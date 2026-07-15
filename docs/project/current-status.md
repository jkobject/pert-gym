# pert-gym current status

_Last verified: 2026-07-15 17:24 CEST. Exact task status lives on Kanban board `pert-gym`; `TODO.md` is the operational source of truth._

## Executive snapshot

- Existing canonical Collection: **120 logical families**, **1,056 physical members**, **142,572,358 observations**.
- Publication workload manifest: **153 executable components + 60 external exclusions**. The 153 are not 153 new biological datasets.
- Accepted workload ledger: **2/153 components**.
- HEK293T: complete, independently tested and approved.
- Airway temporal-v4 row 99: one fresh immutable retry is running after its category-safe Parquet correction was approved and merged.
- Existing-dataset metadata audit: base-public 60-component report exists; **9/26** biological/publication reviews are complete and the other 17 are running in two replacement lanes.
- 60-addition audit exists but needs semantic-placeholder normalization.
- VAR Ensembl/species report: not yet produced for 120/120; replacement task is ready.
- Final 120-row OBS+VAR synthesis: dependency-waiting.
- PR #77 and #79 merged; PR #78 is being corrected on the same branch before independent review/merge.
- **23 PRs remain open**; a reviewer is classifying all of them against exact-head Kanban evidence.
- Genuine human blocker: RxRx3 portal/Auth0 + Recursion EULA only.

## Production

### Accepted HEK293T

- shape: 4,534,299 × 38,606;
- 32 contiguous manifest records;
- 29,136,391,388 nnz;
- one shared var and zero per-block vars;
- source/readback mismatch: 0;
- independent tester: PASS;
- independent reviewer: APPROVE;
- ledger delta: 1/153 → 2/153.

### Running airway component

Card: `t_d0a2115a`.

- source: CELLxGENE temporal-v4 row 99, human airway epithelium regeneration;
- shape: 10,224 × 35,552;
- first candidate stopped before manifest because `hash.ID` and `cluster_l1` changed dtype from integer categorical to `int64` after Parquet roundtrip;
- failed candidate has no credit and must not be resumed;
- corrected helper/writer PR #79 passed exact-head independent review and merged to `main` as `ad1cd8c22516993f5a8403837f25fec58ab4abf3`;
- fresh retry requires ledger precondition 2/153, fresh immutable revision, sole EU writer lease, manifest-last and zero promotion/Collection mutation/cleanup.

No ledger increment occurs until complete producer evidence, tester PASS and reviewer APPROVE.

## Existing 120-family semantic audit

### Orthogonal verdicts

- `OBS_COMPLETED=true|false|blocked`
- `VAR_ENSEMBL_SPECIES_COMPLETED=true|false|blocked|not_applicable`

Storage/Zarr/chunking/X concerns never determine `OBS_COMPLETED`.

OBS exclusions: `perturbation_target`, `perturbation_target_id`, `timepoint_unit`, `model_ready`, `loader_projectable`, `harmonization_level`, `duplicate_status`, `guide_id`.

Applicable sequence fields: `guide_sequence` and `molecule_sequence`.

### Canonical active cards

| Card | State at snapshot | Outcome |
|---|---|---|
| `t_7254400c` | running | Correct existing PR #78 contract/scorer |
| `t_a0427365` | running | Biological OBS reviews 1–9/26 |
| `t_9f15f45b` | done | Biological OBS reviews 10–18/26 |
| `t_993d234c` | running | Biological OBS reviews 19–26/26 |
| `t_5870e629` | dependency waiting | Consolidate exactly 26 reviews |
| `t_629bb1d0` | ready | Fix semantic-placeholder handling for 60 additions |
| `t_c2b5fc6e` | ready | Audit VAR Ensembl/species for exactly 120 families |
| `t_9f7052fd` | dependency waiting | Final exact 120-row OBS+VAR synthesis |

Superseded cards are historical only and are not progress or current dependencies.

## PR state

### Merged

- #77 → `8030e9f3be46266f7b268af75567ae7b250f89f1`
- #79 → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`

### Critical open PR

- #78 current old head is CI-green/mergeable at GitHub but violates the corrected OBS field contract. `t_7254400c` is updating the same PR in its clean worktree. Merge only after the corrected exact head receives independent approval.

### Backlog

Card `t_c56390c3` is classifying every open PR. A PR may be merged only if exact-head independent acceptance exists and no later finding contradicts it. CI-green alone is insufficient. Stale/superseded PRs should be closed with their canonical successor named.

## Final convergence chain

1. `t_12667244`: publication/reconciliation macro-gate;
2. `t_fc3d4794`: complete versioned logical Collection;
3. `t_17ec66d9`: exhaustive denominator/shared-var/Zarr/loader/Collection test;
4. `t_61847c4c`: terminal Definition-of-Done acceptance;
5. `t_3df00bdb`: compact final project gate;
6. `t_e8f9c88c`: separate RxRx3 human access/EULA gate.

## Safety

- Lamin instance `laminlabs/pertdata`, branch `jkobject` only.
- Never write Lamin `main`.
- Heavy data operations run on `pert-gym-worker-eu`, not the Mac.
- One heavy writer; read-only audits and local PR work may run in parallel.
- Failed/no-manifest revisions are immutable evidence, not resumable accepted product.
- Shared checkout is intentionally not reset or broadly cleaned; implementation changes use isolated worktrees.
