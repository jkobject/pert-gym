# pert-gym current status

_Last verified: 2026-07-17. Exact task status lives on Kanban board `pert-gym`; `TODO.md` is the operational source of truth._

## Executive snapshot

- Canonical surface: **70 biological/publication-level datasets**, represented by **120 logical families**, **1,056 physical members**, and **142,572,358 observations**.
- Publication workload: **153 executable components + 60 external exclusions**. Components are operational publication units, not additional biological datasets.
- Base-public surface: **26 real datasets**, represented by 60 components and 110,398,202 observations.
- Latest locally generated pre-remediation dataset-level OBS+VAR evidence: complete and deterministically validated; files are ignored by Git and the durable record is Kanban task `t_a5bf1b1b`.
- Current VAR baseline: **21 `true`, 45 `false`, 4 `not_applicable`** across exactly 70 `real_dataset_id` rows.
- Current strict OBS verdict: 70 `false`; missing applicable metadata is documented as `unknown` and does not globally block publication by default.
- PR #78 is merged at `b6c931e16abb325c5206aaa2fe10a2c4c1544164` after exact-head approval.
- **22 PRs remain open** at this snapshot.
- Genuine human blocker: RxRx3 portal/Auth0 + Recursion EULA only.

## Dataset-level OBS and VAR report

Latest local generated files (**ignored by Git, not part of a clean clone**):

- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.json`
- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.tsv`
- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.md`
- `artifacts/schema_audit/final_real_dataset_missingness_review_20260717.tsv`

Verification procedure: read durable task record `t_a5bf1b1b` on board `pert-gym`, then compare any local copies with `shasum -a 256`. Recorded digests are JSON `60530cc3…`, TSV `de267a96…`, Markdown `3d906840…`, and flat TSV `f2d6a07b…`. Regeneration source is currently outside the tracked repository, so these files are evidence products rather than repository APIs.

Validated conservation:

| Unit | Verified |
|---|---:|
| Real biological datasets/publications | 70/70 |
| Logical families | 120/120 |
| Physical members | 1,056/1,056 |
| Observations | 142,572,358/142,572,358 |
| Base-public real datasets | 26/26 |
| Base-public components | 60/60 |
| Base-public observations | 110,398,202/110,398,202 |

The flat missingness report contains 1,550 `unknown` and 1,031 `not_applicable` rows. Status vocabulary is exactly `unknown|not_applicable`; source/search evidence is retained separately and no unresolved candidate is represented as recovered.

`OBS_COMPLETED` and `VAR_ENSEMBL_SPECIES_COMPLETED` are orthogonal. Storage/Zarr/chunking/X concerns never determine `OBS_COMPLETED`. Response-axis `not_applicable` for PRISM/GDSC/Sanger is distinct from any separately joined baseline-expression reference.

## VAR/Ensembl remediation

The remediation owner unit is `real_dataset_id`, never artifact or logical family. The corrected just-in-time chain is:

1. dataset-level 70-row baseline contract `t_a8e5b268`;
2. continuation controller `t_5ec24c1a`;
3. bounded correction lanes by real dataset;
4. one independent verifier per outcome, combining exact tests/readback with semantic/provenance review;
5. final dataset-level JSON/TSV/Markdown certification.

The superseded 120-family remediation graph is held inert. Existing L01–L04 physical work can contribute evidence but does not define report granularity. `goal_mode` is selective; deterministic verifier cards normally remain single-shot.

## PR state

Merged critical contracts include:

- #77 → `8030e9f3be46266f7b268af75567ae7b250f89f1`
- #79 → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`
- #78 → `b6c931e16abb325c5206aaa2fe10a2c4c1544164`

A PR may merge only with exact-head independent acceptance and no later contradictory finding. CI-green alone is insufficient.

## Final convergence chain

1. `t_12667244`: publication/reconciliation macro-gate;
2. `t_fc3d4794`: complete versioned logical Collection;
3. `t_17ec66d9`: exhaustive denominator/shared-var/Zarr/loader/Collection test;
4. `t_61847c4c`: terminal Definition-of-Done acceptance;
5. `t_3df00bdb`: compact final project gate;
6. `t_e8f9c88c`: separate RxRx3 human access/EULA gate.

### Bounded publication-wave topology

- Lane-3 macro `t_12667244` has 5 direct indispensable parents, replacing 76 historical leaf parents.
- The 153 executable workload records occur exactly once across 13 waves (12 × 12 + 1 × 9), followed by two bundle gates; macro, wave, and bundle fan-in is at most 12.
- Immutable final gate `t_3df00bdb` remains at exactly six canonical parents.
- Missing metadata and quality findings remain dataset-local reports, not exclusions or sibling dependencies.
- New outcomes normally use one producer → independent-verifier chain. Split tester/reviewer gates only for genuinely distinct evidence or environments.

## Safety

- Lamin instance `laminlabs/pertdata`, branch `jkobject` only; never write Lamin `main`.
- Heavy operations run on `pert-gym-worker-eu`, not the Mac.
- One heavy writer; read-only audits and local PR work may run in parallel.
- Failed/no-manifest revisions are immutable evidence, not resumable accepted product.
- Implementation changes use isolated worktrees; do not broadly reset the shared checkout.
