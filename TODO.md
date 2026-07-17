# pert-gym TODO / active source of truth

_Last updated: 2026-07-17. This file is the current operational state; Kanban `pert-gym` holds exact live status and dependencies._

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
- The live accepted-component count is intentionally not duplicated here; Kanban and the immutable outcome ledger are authoritative. This operational count is not a count of distinct biological datasets.
- The 60 `base_public` components correspond to **26 biological/publication-level review units**.

## Publication execution

HCT116 and HEK293T have durable accepted evidence from earlier waves. Later publication waves continue through the bounded topology below; exact live component counts, writers, retries and terminal verdicts change too quickly for this file and must be read from the `pert-gym` Kanban board and immutable outcome ledger.

A running card or payload never earns credit. Credit requires a complete manifest, one independent verifier covering exact behavioral/readback and semantic/provenance checks, and administrative producer completion. Failed revisions remain immutable no-credit evidence and are never resumed as accepted product.

## Dataset-level OBS / VAR audit

The biological reporting unit is the **real dataset/publication**, not a logical family or physical artifact. The current conserved crosswalk contains **70 `real_dataset_id` rows = 26 base-public + 44 additions**, backed by 120 logical families, 1,056 physical members and 142,572,358 observations. Families and members remain provenance columns only.

The latest locally generated read-only pre-remediation snapshot is **not tracked by Git** because `artifacts/` is ignored. Its durable evidence record is Kanban task `t_a5bf1b1b`, which stores the conservation results, exact SHA-256 digests and artifact paths. Local files, when present, are:

- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.{json,tsv,md}`;
- `artifacts/schema_audit/final_real_dataset_missingness_review_20260717.tsv`

Verification procedure: read durable task record `t_a5bf1b1b` on board `pert-gym`, then compare any local copies with `shasum -a 256`. Recorded digests are JSON `60530cc3…`, TSV `de267a96…`, Markdown `3d906840…`, and flat TSV `f2d6a07b…`. Regeneration source is currently outside the tracked repository, so these files are evidence products rather than repository APIs.

Deterministic conservation checks pass 70/70 real datasets, 120/120 families, 1,056/1,056 members and 142,572,358/142,572,358 observations. Current VAR verdicts are **21 `true`, 45 `false`, 4 `not_applicable`**. Current OBS strict verdict is 70 `false`; missingness is non-blocking by default and uses only `unknown` or `not_applicable`. The flat review contains 1,550 `unknown` and 1,031 `not_applicable` field rows; each `unknown` keeps available source/search evidence without claiming completed recovery.

VAR remediation is controlled by `real_dataset_id`. The corrected JIT chain starts with dataset-level baseline `t_a8e5b268` and controller `t_5ec24c1a`; the superseded 120-family remediation graph remains inert. Existing L01–L04 work may contribute physical evidence but not define report granularity. Each outcome normally has one independent verifier combining tests/readback with semantic/provenance review.

`OBS_COMPLETED` and `VAR_ENSEMBL_SPECIES_COMPLETED` remain orthogonal. Response-axis `not_applicable` for PRISM/GDSC/Sanger must not be conflated with the status of a separately joined baseline-expression reference. All audit/report lanes are read-only.

## PR hygiene

Merged on the critical path:

- PR #77, measured sparse-block gate → `8030e9f3be46266f7b268af75567ae7b250f89f1`.
- PR #79, category-safe Parquet frame parity → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`.
- PR #78, corrected `OBS_COMPLETED` contract/scorer → `b6c931e16abb325c5206aaa2fe10a2c4c1544164`, after independent exact-head approval.

Repository backlog:

- **22 PRs remain open** at the 2026-07-17 reconciliation snapshot.
- Green CI and GitHub mergeability alone do not authorize merge. Merge requires exact-head independent acceptance and no later contradictory finding.
- Do not broadly clean or reset the shared checkout: it contains extensive pre-existing tracked/untracked work. Use isolated worktrees and targeted changes.

## Remaining final project graph

The durable six-stage convergence path remains:

1. publication macro-gate `t_12667244` — complete/reconcile executable components;
2. Collection build `t_fc3d4794`;
3. exhaustive Collection/shared-var/Zarr/loader test `t_17ec66d9`;
4. terminal acceptance `t_61847c4c`;
5. compact final gate `t_3df00bdb`;
6. separate human-only RxRx3 access/EULA gate `t_e8f9c88c`.

RxRx3 is the only known genuine human gate. It must not serialize other accessible dataset work.

### Publication-wave topology delta

- Lane-3 macro `t_12667244` now has **5 direct indispensable parents**, replacing 76 historical leaf parents. Its 153 executable workload records are assigned exactly once across **13 bounded waves** (12 × 12 + 1 × 9), followed by two bundle gates; every macro, wave and bundle fan-in is at most 12.
- Immutable final gate `t_3df00bdb` is unchanged at exactly **six canonical parents**.
- The migration creates no product credit: four already-accepted component outcomes retain their independent review and ledger evidence. Existing canonical producer lanes retain their accepted evidence. New outcomes normally use one producer → independent-verifier chain; separate tester and reviewer cards are reserved for genuinely different evidence or environments.
- Missing metadata or quality findings remain dataset-local reports, never exclusions or sibling dependencies. RxRx3 remains the separate genuine human gate.
- Deterministic evidence: `artifacts/orchestration/kanban_graph_compaction_t_36a3533e_manifest.json` and `artifacts/orchestration/kanban_graph_compaction_t_36a3533e_health.json`. Pre-migration backup: `/Users/jkobject/.hermes/backups/kanban-20260716T174202-pre-t_36a3533e.db`.

## Operating rules

- One heavy product writer at a time; read-only audits and local PR work may run in parallel.
- Large GCS/Lamin operations run on `pert-gym-worker-eu` in `europe-west1-b`, never on the Mac.
- Use `tools.lamin_context.connect_pertdata()` and branch `jkobject`; never write Lamin `main`.
- No product credit for ready cards, running agents, partial payloads, failed revisions, or producer-only claims.
- Merge reviewed PRs promptly; never merge a merely green/mergeable PR without exact-head acceptance.
- Update this file and `docs/project/current-status.md` whenever accepted ledger, active writer, canonical audit graph, or PR critical path changes.
