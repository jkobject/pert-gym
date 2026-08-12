# pert-gym TODO / active source of truth

_Last reconciled: 2026-08-12 from the strict accepted product ledger, `accepted_10_dataset_review_snapshot.json`, and the review-pending GSE207360 same-snapshot completion evidence. Pending writers are never counted before independent acceptance; scoped validation is not full dataset completion._

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
| Inventory rows with scoped OBS+VAR+structure+cleaning+publication acceptance | **19/92 review-pending snapshot** | 73 | accepted 18 plus merged PR #117 GSE207360 evidence; this PR still requires independent acceptance before the counter is authoritative |
| Scientific datasets satisfying the stronger full project DoD | **0/92** | 92 | fail-closed: no row has all processing-notebook, canonical publication, staging-decommission receipt, same-snapshot docs, and merged exact-head inventory-PR evidence |
| External exclusions dispositioned | **60/60** | 0 | complete |

The canonical surface remains **120 logical families / 1,056 physical members / 142,572,358 observations**. `accepted_components`, new-family registration, Collection membership, recompaction, OBS and VAR are separate units and must never be added together.

### Count vocabulary and audit provenance

- The publication workload contains **213 records = 153 executable components + 60 external exclusions**. Components are reconciliation/publication work units, not 153 new biological datasets on top of the existing 120 families.
- The biological OBS/VAR reporting unit is the **real dataset/publication**. The conserved crosswalk contains **70 `real_dataset_id` rows = 26 base-public + 44 additions**; logical families and physical members remain provenance dimensions.
- The 60 base-public components map to 26 biological/publication-level review units. Their frozen recoverable-existing component×field universe is the separate 640-denominator metric shown above; additions outside that universe never increment `/640`.
- The deterministic review inventory has **92 rows = 70 strict-ledger dataset identities + 22 genuinely-new family identities**. The accepted-10 wave maps exactly to ten of the 22 rows, not to ten additional strict-70 rows. Its exact-ID overlap with the prior eight scoped-complete rows and with the frozen strict-70 IDs is zero. Until an accepted alias/crosswalk reconciliation binds those identities into the strict ledger, the wave receives no `/70` or `/640` credit.
- Deterministic regeneration starts from the exact pre-reconciliation input `data/pert_gym_dataset_review_inventory_baseline_20260729.csv` (SHA-256 `6f79e32f7d829904debcacfe700ce3cd7b42a71428ba5044fe4be0ee1405842d`) and applies the immutable PR #135 integration manifest plus the accepted-10 review snapshot; the generated inventory is never used as its own source. `data/accepted_10_evidence_digests.json` records the 24 exact producer-head evidence digests and is itself code-pinned at SHA-256 `42926969b40e717e44b7474d7ae75677db61b5931e216406d19cf6b3128dbd69`, so verification remains fail-closed in shallow CI checkouts.
- In the separate 404-row storage inventory, `in_lamindb` is reserved for the **23 canonical cleaned** Lamin publications and is identical to `in_canonical_lamindb`. The 176 working/historical catalog rows remain queryable through explicit catalog-status and branch/evidence columns but receive no canonical publication credit.
- `scoped_scientific_validation_accepted` is an explicitly heterogeneous review counter: eight strict-ledger rows passed the earlier OBS+VAR+structure+cleaning+publication conjunction, while ten new-family rows passed code-owned predicates against immutable accepted receipts. Nine of those ten also have complete structured key→UID obs→X→var evidence; GSE196799 remains fail-closed because its accepted link rows are not joined to an immutable current OBS key→UID identity. The wave's strict-70 booleans remain false, so this counter cannot be reused as `/70` credit. It is not `entirely_validated`: the stronger 2026-08-11 full DoD also requires independently accepted scientific bindings, an executable processing-decision notebook, canonical `data/cleaned/<dataset>/` publication, accepted staging-decommission receipt, same-snapshot inventory/docs acceptance, and an independently reviewed merged exact-head inventory PR.
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
| Accepted-10 inventory reconciliation | **SCOPED 8→18/92; full DoD 0/92** | snapshot `accepted-10-review-reconciliation@2026-08-11`; exact accepted heads and reviewer lineage are bound in the deterministic input | Independent review must accept this same inventory/TODO/docs snapshot; unresolved strict-70 aliases and per-row DoD gaps remain explicit |
| Code/PR reliability | **SUBSTANTIAL CLEANUP COMPLETE; merge policy remains exact-SHA reviewer gated** | accepted/fused fixes include PRs #43, #45, #66, #71 and #75; remaining open PRs require their existing exact gates | No stale producer may be repromoted after a superseding continuation is accepted |
| Final convergence | **DOWNSTREAM** | `t_12667244` → `t_fc3d4794` → `t_17ec66d9` → `t_61847c4c` → immutable final gate `t_3df00bdb` | Complete versioned Collection, exhaustive loader/shared-var/Zarr validation, terminal acceptance |
| RxRx3 | **HUMAN BLOCKER ONLY** | `t_e8f9c88c` | Auth0/portal access and Recursion EULA resolved, or reviewer-accepted exclusion |

## Accepted work since the previous dashboard

### GSE207360 review-pending full-DoD continuation

Merged PR #117 and its accepted reviewer establish the source-exhaustive OBS/VAR,
processing notebook, and append-only OBS publication for `geo/GSE207360`. A fresh
EU verify-only readback at exact head `f033b0ad4e6bbaf802bb3342a96c644769fa8003`
reconfirmed OBS `KSAkP0NJF5P5g1mJ0004` → X `4IOEQEw4ylx0Zx4c0000` → VAR
`U8OeHI58YG9Y9Nsb0002`, 12,487 rows split into 10,984 human and 1,503 mouse,
source SHA-256 `b54a754f…`, and registry counts 28,600 Artifacts / 55 Collections
before and after with zero writes or deletions. The canonical receipt digest is
`7f9dfcd4dee95405e8eb5b41845e37db7d20853bdd599444fd54c1e05946d94f`.

Full completion remains fail-closed. No Collection contains the current OBS UID,
no canonical `gs://scperturb/data/cleaned/GSE207360/` payload exists, and the
legacy staging object is already absent. Therefore no deletion was performed and
no `GCS_DECOMMISSION_READY` is asserted. The review-pending inventory row is not
`entirely_validated`; residual gates are canonical cleaned publication, current
Collection membership, independently accepted decommission disposition, accepted
same-snapshot docs, and merged exact-head inventory PR.

### Accepted ten-dataset scoped wave and full-DoD boundary

PR #135 integrated the exact independently reviewed dataset-scoped blobs for GSE228110, C. elegans embryogenesis, E-MTAB-9304, GSE138002/ODD001099, GSE130238/ODD001111, GSE194214/ODD001154, GSE196799/ODD001155, GSE107185, SCP1973/GSE226373, and GSE269572. The canonical review snapshot binds each row to its immutable accepted head, producer, reviewer, aliases, scientific modality, axes/endpoints, annotation level, source evidence, and physical-member/observation denominator.

Those ten identities are exactly the ten already registered genuinely-new-family rows. They overlap neither the prior eight scoped-complete inventory rows nor the frozen strict-70 exact IDs, so the honest scoped inventory delta is **8→18/92**, while registration and Collection remain **10/22** and strict OBS/VAR remain **10/70** and **8/70**. No arithmetic `8+10` is applied to `/70`.

Under the binding stronger project DoD, **0/92** rows are currently complete. All ten wave rows retain explicit scientific-binding, executable-notebook, staging-decommission, accepted same-snapshot docs, and merged inventory-PR gaps; four have an immutable committed notebook, but none has execution/replay evidence in this snapshot. Four payloads retain canonical-layout evidence gaps: E-MTAB-9304, GSE107185, and SCP1973/GSE226373 live under `pert-gym/logical/...`, while GSE196799 lacks an immutable current OBS key→UID binding despite its explicit accepted link rows. This is a fail-closed reconciliation, not a rollback of their earlier scoped acceptance and not authorization to delete staging.

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
- A dataset is complete only when every stronger full-DoD gate is accepted together. Artifact/triplet presence, scoped metadata acceptance, registration, or Collection membership alone must never set `entirely_validated=true`.
- `wiki/` is obsolete. `AGENTS.md` is the single boot file; durable detail belongs under `docs/`.
