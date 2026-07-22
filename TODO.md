# pert-gym TODO / active source of truth

_Last verified: 2026-07-22 21:08 CEST from the `pert-gym` Kanban board, accepted product-delta ledger, immutable reviewer handoffs, and the bounded GSE150062 retry packet. Pending writers are never counted before independent acceptance._

## Product objective / Definition of Done

The project is complete only when every correctly downloadable target dataset is durably represented on `laminlabs/pertdata/jkobject`, all genuinely new families are in a complete append-only Collection, required recompactions have accepted parity/readback and rollback identities, OBS and VAR outcomes are independently certified across the 70 real datasets, exclusions are explicit, and the immutable final gate accepts the whole contract.

## Live product counters

| Metric | Accepted | Remaining | Evidence / note |
|---|---:|---:|---|
| Publication components | **29/153** | 124 | Temporal-v4 row 79 / SCP1467 was the latest accepted component delta |
| New families registered on `jkobject` | **10/22** | 12 | E-MTAB-9304; exact accepted readback `Artifact/rt5eRz8opcJXtybp0000` |
| New families accepted in versioned Collection | **10/22** | 12 | exact accepted readback `Collection/WBFxVN9Alr8zFt9T0000`, mismatch/drift 0 |
| Existing recompactions accepted | **9/32** | 23 | GSE216673 reviewer `t_dfcb1549`; exact immutable readback `gs://scperturb/pert-gym/staging/pert-gym/logical/prism_collection/GSE216673/revisions/t_414e4129_retry2_20260720T082230Z/manifest.json#1784537088866291` |
| Real datasets with ≥1 independently accepted OBS recovery | **10/70** | 60 | accepted history includes `geo/GSE132080` reviewer `t_3bb03773`, OBS `lhR6Ny3n8QcVeItH0003`, plus the current-main GSE197452 outcome; GSE150062 itself remains pending independent acceptance and receives no anticipatory credit |
| Base-public recoverable-existing component×field candidates independently validated | **33/640** | 607 | frozen non-writable baseline; candidate-universe SHA-256 `371b2b78c755c5cfdf8fa82d7826e5b2fdbfe48b67ca11e6d2d77ec7b6ff60c2` |
| Accepted OBS component×field assignments outside the base-public candidate universe | **6** | — | DepMap/CCLE 26Q1; intentionally no `/640` denominator |
| VAR dataset remediations independently accepted | **8/70** | 62 | latest accepted delta: `scperturb/datlinger17` reviewer `t_2c228f48`, VAR `AYnivbGN3JCRzkN70001`; GSE150062 remains pending independent acceptance |
| External exclusions dispositioned | **60/60** | 0 | complete |

The canonical surface remains **120 logical families / 1,056 physical members / 142,572,358 observations**. `accepted_components`, new-family registration, Collection membership, recompaction, OBS and VAR are separate units and must never be added together.

### Count vocabulary and audit provenance

- The publication workload contains **213 records = 153 executable components + 60 external exclusions**. Components are reconciliation/publication work units, not 153 new biological datasets on top of the existing 120 families.
- The biological OBS/VAR reporting unit is the **real dataset/publication**. The conserved crosswalk contains **70 `real_dataset_id` rows = 26 base-public + 44 additions**; logical families and physical members remain provenance dimensions.
- The 60 base-public components map to 26 biological/publication-level review units. Their frozen recoverable-existing component×field universe is the separate 640-denominator metric shown above; additions outside that universe never increment `/640`.
- The durable pre-remediation evidence record is Kanban task `t_a5bf1b1b`. Local evidence copies, when present, are `artifacts/schema_audit/final_real_dataset_obs_var_20260717.{json,tsv,md}` and `artifacts/schema_audit/final_real_dataset_missingness_review_20260717.tsv`; recorded digests begin JSON `60530cc3…`, TSV `de267a96…`, Markdown `3d906840…`, and flat TSV `f2d6a07b…`.
- `OBS_COMPLETED` and `VAR_ENSEMBL_SPECIES_COMPLETED` are orthogonal. Missing applicable metadata remains `unknown`; genuinely inapplicable metadata is `not_applicable`. Response-axis `not_applicable` does not determine the status of a separately joined baseline-expression reference.

## Active and next workstreams

| Workstream | Current state | Owner / next gate | Completion condition |
|---|---|---|---|
| New-family registration | **ACCEPTED — 10/22** | E-MTAB-9304 exact Artifact `rt5eRz8opcJXtybp0000`; successor Collection `WBFxVN9Alr8zFt9T0000` is accepted at 10/22 | Continue one exact generation-pinned family at a time |
| Datlinger17 OBS recovery | **ACCEPTED — 4th OBS dataset** | reviewer `t_cab55b02` PASS, 38/38 fresh EU checks | Exact OBS `sitiyL4128YBC8BS0003`: 5,905-row parity, six accepted fields, `perturbation_type` and `x_semantics` honestly absent; counters **4/70** and **25/640** |
| Chang22 OBS recovery | **ACCEPTED — 5th OBS dataset** | reviewer `t_4c7c7ae2` accepted immutable revision 2: manifest `d2d41e1eadd91b48861f3de48a2e3311df58c92f1ea16aecd5801a540e9c04cf`, exact OBS `ue1GWkOr29VoRN5R0002` | 42,277-row parity and all eight Phase-A fields accepted; counters **5/70** and **33/640** |
| Existing recompactions | **ONGOING — 9/32 accepted** | GSE216673 reviewer `t_dfcb1549` PASS; next capacity-safe JIT recompaction from the accepted exact inventory | Fresh source/readback parity, rollback identity, shared-var/storage proof, independent reviewer |
| OBS continuation | **ONGOING — 10/70 datasets, 33/640 base-public component×fields, and 6 outside-universe component×fields accepted** | `geo/GSE132080` reviewer `t_3bb03773` authorized OBS **8→9/70**; current-main accepted history also includes GSE197452. GSE150062 remains pending independent acceptance and receives no anticipatory credit; GSE213921 is frozen after rejection 3/3 and must not be auto-rerun | Other additions may advance the dataset counter after review, but never the frozen base-public `/640` metric |
| VAR remediation | **ONGOING — 8/70 accepted by the strict ledger** | `geo/GSE132080` reviewer `t_3bb03773` authorized **6→7/70**, then `scperturb/datlinger17` reviewer `t_2c228f48` authorized **7→8/70**; GSE150062 remains pending independent acceptance and receives no anticipatory credit | Continue one stable real-dataset identity at a time; conserve 70 datasets / 120 families / 1,056 members and keep residual false/unknown/not-applicable verdicts honest |
| Code/PR reliability | **SUBSTANTIAL CLEANUP COMPLETE; merge policy remains exact-SHA reviewer gated** | accepted/fused fixes include PRs #43, #45, #66, #71 and #75; remaining open PRs require their existing exact gates | No stale producer may be repromoted after a superseding continuation is accepted |
| Final convergence | **DOWNSTREAM** | `t_12667244` → `t_fc3d4794` → `t_17ec66d9` → `t_61847c4c` → immutable final gate `t_3df00bdb` | Complete versioned Collection, exhaustive loader/shared-var/Zarr validation, terminal acceptance |
| RxRx3 | **HUMAN BLOCKER ONLY** | `t_e8f9c88c` | Auth0/portal access and Recursion EULA resolved, or reviewer-accepted exclusion |

## Accepted work since the previous dashboard

### New families

The frozen 22-family batch advanced from **2→8 registered and 2→8 in Collection**. Accepted additions now include C. elegans, Odd001154, Odd001155, Odd001111, Odd001099, temporal-v4 row 98, temporal-v4 row 109 / GSE269572, and temporal-v4 row 133 / SCP1973. The current successor Collection is `4KzIQvzliuWg8R0k0000` with **1,014 unique OBS members**, exact predecessor union, zero removed members and zero unrelated/storage drift.

GSE269572 originally failed closed because a full 64-character external SHA-256 was incorrectly assigned to Lamin's 22-character native `Artifact.hash`. The corrected writer leaves native hash fields Lamin-managed and stores generation-bound SHA-256 provenance in the description. Administrative closure `t_7ac012e8` repaired the delayed registration ledger **6→7/22** without replaying the Collection delta. SCP1973 then passed its own generation-pinned review and append-only Collection gate, advancing both exact counters **7→8/22**.

### Recompaction

`prism_collection/GSE216673` became the ninth accepted recompaction: **55,156 rows**, 36,601 vars and 76,929,891 CSR nonzeros across the exact 25-Artifact OBS/X/shared-VAR allowlist. Reviewer `t_dfcb1549` independently rehashed all 25 sealed sources and 164 generation-pinned outputs, reproduced exhaustive matrix/OBS/shared-VAR parity through the committed public loader, and confirmed exact no-op replay from immutable readback `gs://scperturb/pert-gym/staging/pert-gym/logical/prism_collection/GSE216673/revisions/t_414e4129_retry2_20260720T082230Z/manifest.json#1784537088866291`; all rejected revisions remain preserved, with zero new-family, Collection, accepted-component, OBS or VAR credit.

`prism_collection/GSE220974` became the eighth accepted recompaction: **24,661 rows**, 23,760 vars and 98,324,326 CSR nonzeros across the exact five selected legacy triplets. Reviewer `t_c34a72aa` independently rehashed all 16 sealed source/rollback Artifacts and all 3,011 generation-pinned outputs, reproduced exhaustive matrix/OBS/shared-VAR parity through the ordinary loader, and confirmed exact no-op replay from manifest generation `1784513650947728`; `schema_fingerprint` remains honestly `unknown`, with zero new-family, Collection, accepted-component, OBS or VAR credit.

`prism_collection/GSE161824` became the seventh accepted recompaction: **176,040 rows**, 939 vars and 72,175,182 CSR nonzeros across the exact 36 selected legacy triplets. Reviewer `t_b8cf6151` rehashed all 108 selected sources and all 2,215 generation-pinned outputs, reproduced exhaustive matrix/OBS/shared-VAR parity, and confirmed exact no-op replay from immutable manifest generation `1784505766163121`; external-source completeness remains `unknown`, with zero new-family, Collection or accepted-component credit. GSE214844 remains the independently accepted sixth recompaction.

### OBS

The strict accepted OBS ledger is **10/70 real datasets**: the previously documented eight-dataset snapshot (DRUG-seq, Ginkgo/VCPI, LINCS phase 2, Datlinger17, Chang22, depmap_ccle/26q1, `scperturb/adamson16`, and `SchiebingerLander2019`) plus independently accepted `geo/GSE132080` and GSE197452. Reviewer `t_3bb03773` authorized the exact **8→9/70** delta against OBS `lhR6Ny3n8QcVeItH0003`, X `NEbod0p6ws0H5wug0000`, and zero-write receipt `6426b06e1fc5a688015912642f8cbf78d37f788b6c4adf3a13567b750c5d6e05`. Datlinger17 revision `0003` remains accepted on exact 5,905-row parity and six fields, while `perturbation_type` and `x_semantics` remain explicitly absent. GSE197452 retains exact 20,811×33,694 source/X parity, OBS `6UsaktwOJjkXPM3L0003`, additions successor `ZTXfvA5YDoaqrd750000`, and replay no-op evidence. The frozen base-public assignment counter remains **33/640**, plus six accepted DepMap assignments outside that universe. GSE150062 remains pending independent acceptance and contributes no counter delta in this PR.

The **70** denominator counts `real_dataset_id` values, not families, physical members or evidence packets. Artifact integrity/lineage acceptance is tracked separately from metadata missingness quality: a dataset can pass immutable identity and row-parity checks while unsupported or absent fields remain honestly missing.

### VAR

The strict accepted VAR ledger is **8/70** at the GSE150062 retry gate. After the previously documented **6/70** snapshot, `geo/GSE132080` reviewer `t_3bb03773` authorized **6→7/70** for VAR `GJ1HqkBSHfDD1o4m0002`, then `scperturb/datlinger17` reviewer `t_2c228f48` authorized **7→8/70** for VAR `AYnivbGN3JCRzkN70001`. GSE150062 remains pending independent acceptance and contributes no counter delta in this PR. Its candidate boundary is explicit rather than fabricated: 44,025 source-backed ENSG features, 16,401 source-native custom `LH` features for which exact ENSG assignment is `not_applicable`, and 71 unresolved applicable features preserved as `unknown`; all 60,497 retain source identity, human species, and X-axis parity.

### PR and execution reliability

Independent gates merged or closed several reliability PRs after exact-SHA tests. PR #71 exposed an orchestration defect: a superseding recovery card was linked as parent of the stale original producer, so completion repromoted the old card and `active_pr` refused it repeatedly. The stale producer is now terminal. The CTO supervisor now distinguishes nominal `ready` inventory from actually dispatchable work, wakes once if recovery coverage disappears while the source remains open, never wakes for that loss after terminal source closure, and no longer rearms an unchanged successful event from wall-clock cooldown alone. Explicit failed-launch rearm and new event generations remain supported; the prompt forbids reversed supersession links.

Critical-path contract PRs retained from the reconciled history:

- PR #77, measured sparse-block gate → `8030e9f3be46266f7b268af75567ae7b250f89f1`;
- PR #79, category-safe Parquet frame parity → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`;
- PR #78, corrected `OBS_COMPLETED` scorer contract → `b6c931e16abb325c5206aaa2fe10a2c4c1544164`, after independent exact-head approval.

Green CI and GitHub mergeability alone do not authorize merge. Merge requires independent acceptance of the exact head and no later contradictory finding.

## Remaining final project graph

The durable convergence path remains publication macro-gate `t_12667244` → Collection build `t_fc3d4794` → exhaustive Collection/shared-var/Zarr/loader test `t_17ec66d9` → terminal acceptance `t_61847c4c` → compact final gate `t_3df00bdb`. RxRx3 access/EULA gate `t_e8f9c88c` remains the separate human-only lane and must not serialize accessible dataset work.

Lane 3 has five direct indispensable parents instead of 76 historical leaf parents. Its 153 executable records are assigned exactly once across 13 bounded waves (12 × 12 + 1 × 9), followed by two bundle gates; the immutable final gate retains exactly six canonical parents. Deterministic topology evidence is recorded in `artifacts/orchestration/kanban_graph_compaction_t_36a3533e_{manifest,health}.json`.

## Current operational risks

- Fresh independent terminal probe after GSE161824 acceptance: `pert-gym-worker-eu` has exactly one 500-GB boot disk, **295,551,266,816 bytes free**, 32,784,683,008 bytes MemAvailable, idle/unassigned labels, and no active payload writer, tmux session, or lease. The previously attached extra disk is absent; no cleanup/destruction was performed in this run.
- SCP1973 had a pre-mutation harness false positive because the process-conflict probe matched its own SSH/shell ancestor. It failed closed with zero writes, then restarted with corrected self-ancestor exclusion and posted fresh preflight/writing/checkpointing heartbeats.
- Some historical macro/controller cards remain blocked or in triage. Their status is not product truth; only accepted product deltas and independent reviewer evidence change counters.
- RxRx3 remains the only genuine human access blocker and must not block other lanes.

## Operating rules

- One heavy product writer at a time; read-only audits, metadata-only work and local PR work may run in parallel when their contracts prove zero payload materialization.
- Heavy payload/GCS/Lamin work runs only on `pert-gym-worker-eu` in `europe-west1-b`, never on the Mac.
- Connect only through `tools.lamin_context.connect_pertdata()` to `laminlabs/pertdata`, branch `jkobject`; never write `main`.
- Publication and recompaction are append-only. Preserve old generations and rollback identities; no deletion without explicit accepted gates.
- A producer, dry-run, heartbeat, green test, PR, staging object or VM process is not product progress. Credit requires `product_delta` with exact before/after/denominator/unit, mismatch 0 and durable live readback.
- A superseding continuation must not be linked as parent of the stale source card. After accepted review, the stale source becomes terminal `superseded`.
- Every accepted product delta must reconcile this file and `docs/project/current-status.md`; pending writers are labelled pending rather than anticipated.
- `wiki/` is obsolete. `AGENTS.md` is the single boot file; durable detail belongs under `docs/`.
