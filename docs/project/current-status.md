# pert-gym current status

_Last verified: 2026-07-20 04:54 CEST. `TODO.md` is the operational dashboard; Kanban `pert-gym` carries exact lifecycle/evidence; accepted product-delta ledger and immutable reviewer handoffs are authoritative for counters._

## Executive snapshot

- Canonical surface: **120 logical families / 1,056 members / 142,572,358 observations**.
- Publication workload: **29/153 accepted components**.
- Frozen new-family batch: **10/22 registered on `jkobject` and 10/22 independently accepted in a versioned Collection** after E-MTAB-9304; exact registration readback `Artifact/rt5eRz8opcJXtybp0000` and Collection readback `Collection/WBFxVN9Alr8zFt9T0000`.
- Existing recompactions: **8/32 accepted**; latest accepted outcome is GSE220974 with exact 24,661-row CSR/OBS/shared-VAR parity, immutable manifest generation `1784513650947728`, and `schema_fingerprint` explicitly `unknown`.
- OBS recovery: **6/70 real datasets**, **33/640 base-public recoverable-existing component×field candidates**, and **6 accepted component×field assignments outside that base-public universe**. The `/640` metric is a frozen non-writable baseline bound to candidate-universe SHA-256 `371b2b78c755c5cfdf8fa82d7826e5b2fdbfe48b67ca11e6d2d77ec7b6ff60c2`; depmap_ccle/26q1 is the outside-universe six and has no `/640` denominator.
- VAR remediation: **4/70 independently accepted by the strict ledger**. Run 4073 (`t_051858ce`) is rejected as `unit_mismatch` and run 4094 (`t_f4d2948f`) as `before_does_not_match_current`; both product outcomes remain pending conforming exact-once reconciliation.
- External exclusions: **60/60 dispositioned**.
- Genuine human blocker: RxRx3 Auth0/portal and Recursion EULA only.

## Live execution

| Lane | State | Exact next outcome |
|---|---|---|
| New-family registration | E-MTAB-9304 accepted at exact counters **10/22**; Artifact `rt5eRz8opcJXtybp0000`, Collection `WBFxVN9Alr8zFt9T0000` | continue the next exact generation-pinned family from that accepted Collection |
| Datlinger17 OBS | accepted by `t_cab55b02`, 38/38 checks, zero reviewer writes | counters advanced **3→4/70** and **19→25/640**; continue the next bounded OBS dataset |
| Recompaction | GSE220974 accepted by `t_c34a72aa`; **8/32 accepted** | one next capacity-safe JIT producer from the accepted inventory |
| OBS | depmap_ccle/26q1 accepted by `t_8d1e3b8c`: exact OBS `kCNSxyUJoJJKRSgE0004`, 1,719 rows, six accepted fields outside the frozen base-public candidate universe | counters are **6/70** real datasets, **33/640** base-public component×fields, and **6** outside-universe component×fields without a denominator |
| VAR | GSE207360 and LINCS phase1 retain reviewed product evidence, but runs 4073 and 4094 fail the strict top-level delta contract and remain pending reconciliation | counter is **4/70**; continue one stable real-dataset identity at a time |
| Final convergence | downstream | publication macro → complete Collection → exhaustive test → terminal acceptance → immutable final gate |

## Problems found and fixes

1. **Stale PR #71 producer starved the queue.** A recovery card had been linked as parent of the stale original producer. When the recovery completed, the kernel correctly promoted its child — unfortunately the child was the obsolete producer. `active_pr` then refused it every minute. The old producer is now terminal. The supervisor excludes repeatedly guarded `ready` cards from executable continuity, detects recovery coverage that disappears while its source remains open, suppresses that wake after terminal source closure, and never rearms an unchanged successful event solely because a cooldown elapsed. Explicit failed-launch rearm and authoritative new event generations remain intact; the CTO prompt forbids reversed supersession links.
2. **GSE269572 hash contract initially failed closed.** A 64-character external SHA-256 was assigned to Lamin's 22-character native `Artifact.hash`. Zero Artifacts or links were created in that failed attempt. The corrected writer leaves native hashes Lamin-managed and retains the full generation-bound SHA-256 in provenance. Independent review passed; administrative ledger closure repaired registration **6→7/22** without product mutation.
3. **Datlinger17's first revision contained unsupported semantics.** Independent review rejected `control_availability=True` and vague `perturbation_type='genetic'`. Revision `0003` uses `dataset_control_available`, removes the unsupported perturbation type and leaves `x_semantics` absent. The final independent gate passed 38/38 fresh EU checks with exact 5,905-row parity, advancing **3→4/70 OBS** and **19→25/640 Phase-A assignments**.
4. **Chang22 revision 2 is accepted.** Reviewer `t_4c7c7ae2` verified manifest `d2d41e1eadd91b48861f3de48a2e3311df58c92f1ea16aecd5801a540e9c04cf`, exact OBS `ue1GWkOr29VoRN5R0002`, all 42,277 rows and all eight Phase-A fields. The accepted ledger is now **5/70 OBS** and **33/640 assignments**.
5. **Dashboards were stale.** They still reported 2/22 families, 3 recompactions and 2/70 OBS. Both status files have been rebuilt from the live ledger/reviewer evidence. The CTO prompt now requires reconciliation after every accepted `product_delta` and forbids anticipating active-writer deltas.
6. **SCP1973 preflight had a process-probe false positive.** The probe matched its own SSH/shell ancestor and failed closed before mutation. The worker corrected self-ancestor exclusion, restarted with fresh leases/preflight and posted real writing/checkpointing heartbeats.
7. **VM capacity reverified.** Fresh independent terminal probe after GSE161824 acceptance found exactly one 500-GB boot disk, 295,551,266,816 bytes free, 32,784,683,008 bytes MemAvailable, idle/unassigned labels, and no active payload writer, tmux session, or lease. The previously attached extra disk is absent; this run performed no cleanup or destruction.
8. **GSE207360 is an authoritative mixed-species axis, pending ledger reconciliation.** Reviewer `t_051858ce` accepted append-only VAR `U8OeHI58YG9Y9Nsb0002` after exact 60,736-row live readback, but run 4073's top-level claim is rejected as `unit_mismatch`; it does not currently advance 4/70.
9. **LINCS phase1 product evidence is accepted, pending ledger reconciliation.** Reviewer `t_953d8c3e` accepted VAR `3HNxm817WoemWsl10002` and X `QlcIPRMMk667dGwS0000`, but reconciliation run 4094 (`t_f4d2948f`) is rejected as `before_does_not_match_current`; it does not currently advance 4/70.
10. **GSE220974 recompaction is accepted as the eighth outcome.** Reviewer `t_c34a72aa` independently rehashed all 16 sealed sources and 3,011 generation-pinned outputs, reproduced exhaustive 24,661-row matrix/OBS/feature parity through the ordinary loader, verified exact Lamin/run/shared-VAR identity and exact no-op replay, and accepted `existing_recompactions_accepted` **7→8/32** with mismatch 0. `schema_fingerprint` remains honestly `unknown`.
11. **GSE161824 recompaction is accepted.** Reviewer `t_b8cf6151` independently selected the exact active VAR identity, rehashed all 108 selected sources and 2,215 generation-pinned outputs, reproduced exhaustive 176,040-row parity, and confirmed `EXACT_NOOP` replay with mismatch 0. The strict recompaction counter is now **7/32**.

## Accounting and safety

- `accepted_components`, registration, Collection membership, recompaction, OBS and VAR are distinct counters.
- The **70** OBS/VAR denominator counts `real_dataset_id` values, not families, physical members or evidence packets. Immutable integrity/lineage acceptance is separate from metadata missingness quality; absent or unsupported fields remain explicit rather than being inferred from parity.
- No producer, test, heartbeat, PR, dry-run or VM process changes a product counter without independent acceptance and mismatch-0 live readback.
- All GCS/Lamin/payload work runs on `pert-gym-worker-eu` in `europe-west1-b`; never on the Mac.
- Use `tools.lamin_context.connect_pertdata()` on `laminlabs/pertdata/jkobject`; never write Lamin `main`.
- Publication is append-only; preserve predecessor Collections and rollback identities; no deletion without explicit accepted gates.
- `wiki/` is obsolete; `AGENTS.md` is the sole boot file and durable detail belongs under `docs/`.
