# pert-gym TODO / active source of truth

_Last updated: 2026-07-15 17:24 CEST. This file is the current operational state; Kanban `pert-gym` holds exact live status and dependencies._

## Product objective / Definition of Done

The project is not done when tooling, pilots, audits, PRs, or individual writers finish. It is done when:

1. every correctly downloadable target dataset is materialized on `laminlabs/pertdata`, branch `jkobject`;
2. accepted datasets are represented in a complete versioned logical Collection;
3. exact source/readback parity, provenance, branch isolation, shared-var and storage contracts pass independent tests and review;
4. `OBS_COMPLETED` and `VAR_ENSEMBL_SPECIES_COMPLETED` are reported as separate biological/semantic verdicts;
5. genuine external exclusions and human-only blockers are explicit.

## Count vocabulary — do not conflate these units

- Existing canonical surface `pert-gym/canonical/20260621`: **1,056 physical members**, **120 logical families**, **142,572,358 observations**.
- Publication workload manifest: **213 records = 153 executable components + 60 external exclusions**.
- The 153 records are workload components to reconcile/publish, **not 153 new biological datasets on top of the existing 120**.
- Accepted publication ledger at this snapshot: **2/153 components**. This is an operational workload ledger, not a count of distinct biological datasets.
- The 60 `base_public` components correspond to **26 biological/publication-level review units**.

## Live production lane

### Accepted

- HCT116: accepted publication component before HEK293T.
- HEK293T: independently tested and approved; **4,534,299 × 38,606**, 32 contiguous records, 29,136,391,388 nnz, one shared var, mismatch 0. Ledger advanced from 1/153 to **2/153**.

### Running now — airway temporal component

Card `t_d0a2115a` is the sole product writer lane for CELLxGENE temporal-v4 row 99:

- human airway epithelium regeneration;
- 10,224 observations × 35,552 variables;
- first attempt failed closed before manifest because two categorical integer `obs` columns round-tripped through Parquet as `int64`;
- failed revision is immutable, no-credit, and must never be reused;
- category-safe parity correction PR #79 was independently approved and merged to `main` as `ad1cd8c22516993f5a8403837f25fec58ab4abf3`;
- one fresh immutable retry is running, with ledger precondition exactly 2/153, one EU writer lease, manifest-last, no promotion/Collection mutation/cleanup.

A running card or payload does not increment the ledger. Credit requires a complete manifest, independent tester PASS, independent reviewer APPROVE, and administrative producer completion.

## `OBS_COMPLETED` / existing 120-family audit

### Binding contract

`OBS_COMPLETED` concerns `obs` metadata, semantics, identity, provenance, controls and applicable calculations. It does **not** depend on Zarr, chunk sizes/counts, X storage/integrity, duplicated var payloads, or source-X identity.

Excluded from the OBS verdict:

- `perturbation_target`
- `perturbation_target_id`
- `timepoint_unit`
- `model_ready`
- `loader_projectable`
- `harmonization_level`
- `duplicate_status`
- `guide_id`

Required when biologically applicable:

- `guide_sequence`
- `molecule_sequence`

`VAR_ENSEMBL_SPECIES_COMPLETED` is a separate adjacent verdict.

### Active canonical graph

1. `t_7254400c` — **running**: correct the existing PR #78 on its clean worktree; no second PR.
2. `t_2b2e279d` — **done**: first read-only audit of all 60 base-public components / 110,398,202 observations.
3. Publication-level reviews of those 60 components:
   - `t_a0427365` — **running**: biological datasets 1–9/26;
   - `t_9f15f45b` — **done**: biological datasets 10–18/26, nine reports produced;
   - `t_993d234c` — **running**: biological datasets 19–26/26;
   - `t_5870e629` — dependency-waiting synthesis of exactly 26 reviews and exhaustive 60-component coverage.
4. `t_629bb1d0` — ready: correct the 60-addition audit so placeholders such as `unknown`, blank, `na`, `none`, `unreported`, etc. cannot count as semantically present/recoverable.
5. `t_c2b5fc6e` — ready: produce the missing read-only 120/120 `VAR_ENSEMBL_SPECIES_COMPLETED` report.
6. `t_9f7052fd` — dependency-waiting canonical final review: exactly 120 logical rows with separate OBS and VAR verdicts.

The older synthesis cards `t_1241410f` and `t_15ac600e`, old 26-review synthesis `t_f4c0ac3a`, crashed reviewer lanes and original tester recovery lanes are superseded and must not be treated as canonical work.

### Current artifact status

- Base-public component audit exists: `artifacts/schema_audit/obs_completed_base_public_60_20260715.{json,tsv,md}`.
- Biological crosswalk exists: `artifacts/schema_audit/obs_completed_reviews/base_public/base_public_26_review_crosswalk_20260715.{json,tsv,md}`.
- Nine reviews for positions 10–18 exist under `artifacts/schema_audit/obs_completed_reviews/base_public/by_dataset/`.
- Initial 60-addition JSON/TSV/MD exists but is not final until semantic-placeholder normalization is rerun.
- No accepted 120/120 VAR Ensembl/species report exists yet.
- No final combined 120-row OBS+VAR report exists yet.
- All audit lanes are read-only; they must not alter live Lamin/GCS payloads.

## PR hygiene

Merged on 2026-07-15:

- PR #77, measured sparse-block gate → `8030e9f3be46266f7b268af75567ae7b250f89f1`.
- PR #79, category-safe Parquet frame parity → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`.

Open and on the critical path:

- PR #78: CI-green but **not mergeable by policy yet** because its current head violates the corrected OBS contract. `t_7254400c` is repairing the same PR; it requires one independent exact-head review before merge.

Repository backlog:

- **23 PRs remain open** at the snapshot.
- `t_c56390c3` is running a complete read-only classification into: merge-now-approved, needs-independent-review, needs-fix/rebase, stale/superseded-close, or unknown-needs-owner.
- Green CI and GitHub mergeability alone do not authorize merge. Merge requires exact-head independent acceptance and no later contradictory finding.
- Do not broadly clean or reset the shared checkout: it contains extensive pre-existing tracked/untracked work. Use isolated worktrees and targeted changes.

## Remaining final project graph

The durable convergence dependency graph is:

- publication macro-gate `t_12667244` → Collection build `t_fc3d4794` → exhaustive Collection/shared-var/Zarr/loader test `t_17ec66d9`;
- the separate human-only RxRx3 access/EULA gate `t_e8f9c88c` runs in parallel with that path;
- test `t_17ec66d9` and RxRx3 gate `t_e8f9c88c` converge into terminal acceptance `t_61847c4c`;
- compact final gate `t_3df00bdb` follows terminal acceptance and retains all six direct canonical parents: denominator `t_04b761eb`, loader contract `t_0cff18c2`, publication `t_12667244`, Collection `t_fc3d4794`, test `t_17ec66d9`, and terminal acceptance `t_61847c4c`.

RxRx3 is the only known genuine human gate. It must not serialize other accessible dataset work before convergence at terminal acceptance.

## Operating rules

- One heavy product writer at a time; read-only audits and local PR work may run in parallel.
- Large GCS/Lamin operations run on `pert-gym-worker-eu` in `europe-west1-b`, never on the Mac.
- Use `tools.lamin_context.connect_pertdata()` and branch `jkobject`; never write Lamin `main`.
- No product credit for ready cards, running agents, partial payloads, failed revisions, or producer-only claims.
- Merge reviewed PRs promptly; never merge a merely green/mergeable PR without exact-head acceptance.
- Update this file and `docs/project/current-status.md` whenever accepted ledger, active writer, canonical audit graph, or PR critical path changes.
