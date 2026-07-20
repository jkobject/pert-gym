# pert-gym TODO / active source of truth

_Last verified: 2026-07-20 04:54 CEST from the `pert-gym` Kanban board, accepted product-delta ledger, and immutable reviewer handoffs. Pending writers are never counted before independent acceptance._

## Product objective / Definition of Done

The project is complete only when every correctly downloadable target dataset is durably represented on `laminlabs/pertdata/jkobject`, all genuinely new families are in a complete append-only Collection, required recompactions have accepted parity/readback and rollback identities, OBS and VAR outcomes are independently certified across the 70 real datasets, exclusions are explicit, and the immutable final gate accepts the whole contract.

## Live product counters

| Metric | Accepted | Remaining | Evidence / note |
|---|---:|---:|---|
| Publication components | **29/153** | 124 | Temporal-v4 row 79 / SCP1467 was the latest accepted component delta |
| New families registered on `jkobject` | **10/22** | 12 | E-MTAB-9304; exact accepted readback `Artifact/rt5eRz8opcJXtybp0000` |
| New families accepted in versioned Collection | **10/22** | 12 | exact accepted readback `Collection/WBFxVN9Alr8zFt9T0000`, mismatch/drift 0 |
| Existing recompactions accepted | **8/32** | 24 | GSE220974 reviewer `t_c34a72aa`; exact 24,661-row CSR/OBS/shared-VAR parity, immutable manifest generation `1784513650947728` |
| Real datasets with ≥1 independently accepted OBS recovery | **6/70** | 64 | latest: depmap_ccle/26q1 OBS `kCNSxyUJoJJKRSgE0004` |
| Base-public recoverable-existing component×field candidates independently validated | **33/640** | 607 | frozen non-writable baseline; candidate-universe SHA-256 `371b2b78c755c5cfdf8fa82d7826e5b2fdbfe48b67ca11e6d2d77ec7b6ff60c2` |
| Accepted OBS component×field assignments outside the base-public candidate universe | **6** | — | DepMap/CCLE 26Q1; intentionally no `/640` denominator |
| VAR dataset remediations independently accepted | **4/70** | 66 | strict live ledger; run 4073 (`t_051858ce`) rejected `unit_mismatch`, run 4094 (`t_f4d2948f`) rejected `before_does_not_match_current`; both pending reconciliation |
| External exclusions dispositioned | **60/60** | 0 | complete |

The canonical surface remains **120 logical families / 1,056 physical members / 142,572,358 observations**. `accepted_components`, new-family registration, Collection membership, recompaction, OBS and VAR are separate units and must never be added together.

## Active and next workstreams

| Workstream | Current state | Owner / next gate | Completion condition |
|---|---|---|---|
| New-family registration | **ACCEPTED — 10/22** | E-MTAB-9304 exact Artifact `rt5eRz8opcJXtybp0000`; successor Collection `WBFxVN9Alr8zFt9T0000` is accepted at 10/22 | Continue one exact generation-pinned family at a time |
| Datlinger17 OBS recovery | **ACCEPTED — 4th OBS dataset** | reviewer `t_cab55b02` PASS, 38/38 fresh EU checks | Exact OBS `sitiyL4128YBC8BS0003`: 5,905-row parity, six accepted fields, `perturbation_type` and `x_semantics` honestly absent; counters **4/70** and **25/640** |
| Chang22 OBS recovery | **ACCEPTED — 5th OBS dataset** | reviewer `t_4c7c7ae2` accepted immutable revision 2: manifest `d2d41e1eadd91b48861f3de48a2e3311df58c92f1ea16aecd5801a540e9c04cf`, exact OBS `ue1GWkOr29VoRN5R0002` | 42,277-row parity and all eight Phase-A fields accepted; counters **5/70** and **33/640** |
| Existing recompactions | **ONGOING — 8/32 accepted** | GSE220974 reviewer `t_c34a72aa` PASS; next capacity-safe JIT recompaction from the accepted exact inventory | Fresh source/readback parity, rollback identity, shared-var/storage proof, independent reviewer |
| OBS continuation | **ONGOING — 6/70 datasets, 33/640 base-public component×fields, and 6 outside-universe component×fields accepted** | depmap_ccle/26q1 reviewer `t_8d1e3b8c` PASS | GSE213921 and other additions may advance the dataset counter after review, but never the frozen base-public `/640` metric |
| VAR remediation | **ONGOING — 4/70 accepted by the strict ledger** | GSE207360/run 4073 and LINCS phase1/run 4094 have accepted product evidence but their top-level claims remain rejected (`unit_mismatch`; `before_does_not_match_current`) pending conforming exact-once reconciliation | Continue one stable real-dataset identity at a time; conserve 70 datasets / 120 families / 1,056 members and keep residual false/unknown verdicts honest |
| Code/PR reliability | **SUBSTANTIAL CLEANUP COMPLETE; merge policy remains exact-SHA reviewer gated** | accepted/fused fixes include PRs #43, #45, #66, #71 and #75; remaining open PRs require their existing exact gates | No stale producer may be repromoted after a superseding continuation is accepted |
| Final convergence | **DOWNSTREAM** | `t_12667244` → `t_fc3d4794` → `t_17ec66d9` → `t_61847c4c` → immutable final gate `t_3df00bdb` | Complete versioned Collection, exhaustive loader/shared-var/Zarr validation, terminal acceptance |
| RxRx3 | **HUMAN BLOCKER ONLY** | `t_e8f9c88c` | Auth0/portal access and Recursion EULA resolved, or reviewer-accepted exclusion |

## Accepted work since the previous dashboard

### New families

The frozen 22-family batch advanced from **2→8 registered and 2→8 in Collection**. Accepted additions now include C. elegans, Odd001154, Odd001155, Odd001111, Odd001099, temporal-v4 row 98, temporal-v4 row 109 / GSE269572, and temporal-v4 row 133 / SCP1973. The current successor Collection is `4KzIQvzliuWg8R0k0000` with **1,014 unique OBS members**, exact predecessor union, zero removed members and zero unrelated/storage drift.

GSE269572 originally failed closed because a full 64-character external SHA-256 was incorrectly assigned to Lamin's 22-character native `Artifact.hash`. The corrected writer leaves native hash fields Lamin-managed and stores generation-bound SHA-256 provenance in the description. Administrative closure `t_7ac012e8` repaired the delayed registration ledger **6→7/22** without replaying the Collection delta. SCP1973 then passed its own generation-pinned review and append-only Collection gate, advancing both exact counters **7→8/22**.

### Recompaction

`prism_collection/GSE220974` became the eighth accepted recompaction: **24,661 rows**, 23,760 vars and 98,324,326 CSR nonzeros across the exact five selected legacy triplets. Reviewer `t_c34a72aa` independently rehashed all 16 sealed source/rollback Artifacts and all 3,011 generation-pinned outputs, reproduced exhaustive matrix/OBS/shared-VAR parity through the ordinary loader, and confirmed exact no-op replay from manifest generation `1784513650947728`; `schema_fingerprint` remains honestly `unknown`, with zero new-family, Collection, accepted-component, OBS or VAR credit.

`prism_collection/GSE161824` became the seventh accepted recompaction: **176,040 rows**, 939 vars and 72,175,182 CSR nonzeros across the exact 36 selected legacy triplets. Reviewer `t_b8cf6151` rehashed all 108 selected sources and all 2,215 generation-pinned outputs, reproduced exhaustive matrix/OBS/shared-VAR parity, and confirmed exact no-op replay from immutable manifest generation `1784505766163121`; external-source completeness remains `unknown`, with zero new-family, Collection or accepted-component credit. GSE214844 remains the independently accepted sixth recompaction.

### OBS

Accepted OBS baseline is now DRUG-seq, Ginkgo/VCPI, LINCS phase 2, Datlinger17 and Chang22: **5/70 real datasets and 33/640 Phase-A assignments**. Chang22 immutable revision 2 passed with manifest `d2d41e1eadd91b48861f3de48a2e3311df58c92f1ea16aecd5801a540e9c04cf`, exact OBS `ue1GWkOr29VoRN5R0002`, 42,277-row parity and all eight proposed fields accepted. Datlinger17 revision `0003` remains accepted on exact 5,905-row parity and six fields, while `perturbation_type` and `x_semantics` remain explicitly absent.

The **70** denominator counts `real_dataset_id` values, not families, physical members or evidence packets. Artifact integrity/lineage acceptance is tracked separately from metadata missingness quality: a dataset can pass immutable identity and row-parity checks while unsupported or absent fields remain honestly missing.

### VAR

`geo/GSE207360` and LINCS phase1 Level2 retain independently reviewed product evidence (`U8OeHI58YG9Y9Nsb0002` and `3HNxm817WoemWsl10002`, respectively), but neither currently advances the strict top-level counter. Run 4073 (`t_051858ce`) is rejected as `unit_mismatch`; run 4094 (`t_f4d2948f`) is rejected as `before_does_not_match_current`. Both outcomes remain pending conforming exact-once ledger reconciliation, so the canonical VAR counter stays **4/70** and no prose-only 4→5→6 credit is claimed.

### PR and execution reliability

Independent gates merged or closed several reliability PRs after exact-SHA tests. PR #71 exposed an orchestration defect: a superseding recovery card was linked as parent of the stale original producer, so completion repromoted the old card and `active_pr` refused it repeatedly. The stale producer is now terminal. The CTO supervisor now distinguishes nominal `ready` inventory from actually dispatchable work, wakes once if recovery coverage disappears while the source remains open, never wakes for that loss after terminal source closure, and no longer rearms an unchanged successful event from wall-clock cooldown alone. Explicit failed-launch rearm and new event generations remain supported; the prompt forbids reversed supersession links.

## Current operational risks

- Fresh independent terminal probe after GSE161824 acceptance: `pert-gym-worker-eu` has exactly one 500-GB boot disk, **295,551,266,816 bytes free**, 32,784,683,008 bytes MemAvailable, idle/unassigned labels, and no active payload writer, tmux session, or lease. The previously attached extra disk is absent; no cleanup/destruction was performed in this run.
- SCP1973 had a pre-mutation harness false positive because the process-conflict probe matched its own SSH/shell ancestor. It failed closed with zero writes, then restarted with corrected self-ancestor exclusion and posted fresh preflight/writing/checkpointing heartbeats.
- Some historical macro/controller cards remain blocked or in triage. Their status is not product truth; only accepted product deltas and independent reviewer evidence change counters.
- RxRx3 remains the only genuine human access blocker and must not block other lanes.

## Operating rules

- Heavy payload/GCS/Lamin work runs only on `pert-gym-worker-eu` in `europe-west1-b`, never on the Mac.
- Connect only through `tools.lamin_context.connect_pertdata()` to `laminlabs/pertdata`, branch `jkobject`; never write `main`.
- Publication and recompaction are append-only. Preserve old generations and rollback identities; no deletion without explicit accepted gates.
- A producer, dry-run, heartbeat, green test, PR, staging object or VM process is not product progress. Credit requires `product_delta` with exact before/after/denominator/unit, mismatch 0 and durable live readback.
- A superseding continuation must not be linked as parent of the stale source card. After accepted review, the stale source becomes terminal `superseded`.
- Every accepted product delta must reconcile this file and `docs/project/current-status.md`; pending writers are labelled pending rather than anticipated.
- `wiki/` is obsolete. `AGENTS.md` is the single boot file; durable detail belongs under `docs/`.
