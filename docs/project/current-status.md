# pert-gym current status

_Last verified: 2026-07-20 04:54 CEST. `TODO.md` is the operational dashboard;
Kanban `pert-gym` carries exact lifecycle/evidence; accepted product-delta
ledger and immutable reviewer handoffs are authoritative for counters._

## Executive snapshot

- Canonical surface: **120 logical families / 1,056 members / 142,572,358
  observations**.
- Publication workload: **29/153 accepted components**. **+ 60 external
  exclusions**. The 153 are not 153 new biological datasets.
- Frozen new-family batch: **10/22 registered on `jkobject` and 10/22
  independently accepted in a versioned Collection** after E-MTAB-9304; exact
  registration readback `Artifact/rt5eRz8opcJXtybp0000` and Collection readback
  `Collection/WBFxVN9Alr8zFt9T0000`.
- Existing recompactions: **8/32 accepted**; latest accepted outcome is
  GSE220974 with exact 24,661-row CSR/OBS/shared-VAR parity, immutable manifest
  generation `1784513650947728`, and `schema_fingerprint` explicitly `unknown`.
- OBS recovery: **6/70 real datasets**, **33/640 base-public
  recoverable-existing component×field candidates**, and **6 accepted
  component×field assignments outside that base-public universe**. The `/640`
  metric is a frozen non-writable baseline bound to candidate-universe SHA-256
  `371b2b78c755c5cfdf8fa82d7826e5b2fdbfe48b67ca11e6d2d77ec7b6ff60c2`;
  depmap_ccle/26q1 is the outside-universe six and has no `/640` denominator.
- VAR remediation: **4/70 independently accepted by the strict ledger**. Run
  4073 (`t_051858ce`) is rejected as `unit_mismatch` and run 4094 (`t_f4d2948f`)
  as `before_does_not_match_current`; both product outcomes remain pending
  conforming exact-once reconciliation.
- External exclusions: **60/60 dispositioned**.
- Genuine human blocker: RxRx3 Auth0/portal and Recursion EULA only.
- Base-public surface: **26 real datasets**, represented by 60 components and
  110,398,202 observations.
- Latest locally generated pre-remediation dataset-level OBS+VAR evidence:
  complete and deterministically validated; files are ignored by Git and the
  durable record is Kanban task `t_a5bf1b1b`.
- Current VAR baseline: **21 `true`, 45 `false`, 4 `not_applicable`** across
  exactly 70 `real_dataset_id` rows.
- **22 PRs remain open** at this snapshot.

## Dataset-level OBS and VAR report

| Lane                    | State                                                                                                                                                          | Exact next outcome                                                                                                                              |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| New-family registration | E-MTAB-9304 accepted at exact counters **10/22**; Artifact `rt5eRz8opcJXtybp0000`, Collection `WBFxVN9Alr8zFt9T0000`                                           | continue the next exact generation-pinned family from that accepted Collection                                                                  |
| Datlinger17 OBS         | accepted by `t_cab55b02`, 38/38 checks, zero reviewer writes                                                                                                   | counters advanced **3→4/70** and **19→25/640**; continue the next bounded OBS dataset                                                           |
| Recompaction            | GSE220974 accepted by `t_c34a72aa`; **8/32 accepted**                                                                                                          | one next capacity-safe JIT producer from the accepted inventory                                                                                 |
| OBS                     | depmap_ccle/26q1 accepted by `t_8d1e3b8c`: exact OBS `kCNSxyUJoJJKRSgE0004`, 1,719 rows, six accepted fields outside the frozen base-public candidate universe | counters are **6/70** real datasets, **33/640** base-public component×fields, and **6** outside-universe component×fields without a denominator |
| VAR                     | GSE207360 and LINCS phase1 retain reviewed product evidence, but runs 4073 and 4094 fail the strict top-level delta contract and remain pending reconciliation | counter is **4/70**; continue one stable real-dataset identity at a time                                                                        |
| Final convergence       | downstream                                                                                                                                                     | publication macro → complete Collection → exhaustive test → terminal acceptance → immutable final gate                                          |

## Problems found and fixes

### Running airway component

Card: `t_d0a2115a`.

- source: CELLxGENE temporal-v4 row 99, human airway epithelium regeneration;
- shape: 10,224 × 35,552;
- first candidate stopped before manifest because `hash.ID` and `cluster_l1`
  changed dtype from integer categorical to `int64` after Parquet roundtrip;
- failed candidate has no credit and must not be resumed;
- corrected helper/writer PR #79 passed exact-head independent review and merged
  to `main` as `ad1cd8c22516993f5a8403837f25fec58ab4abf3`;
- fresh retry requires ledger precondition 2/153, fresh immutable revision, sole
  EU writer lease, manifest-last and zero promotion/Collection mutation/cleanup.

No ledger increment occurs until complete producer evidence, tester PASS and
reviewer APPROVE.

## Existing 120-family semantic audit

### Orthogonal verdicts

- `OBS_COMPLETED=true|false|blocked`
- `VAR_ENSEMBL_SPECIES_COMPLETED=true|false|blocked|not_applicable`

Storage/Zarr/chunking/X concerns never determine `OBS_COMPLETED`.

OBS exclusions: `perturbation_target`, `perturbation_target_id`,
`timepoint_unit`, `model_ready`, `loader_projectable`, `harmonization_level`,
`duplicate_status`, `guide_id`.

Applicable sequence fields: `guide_sequence` and `molecule_sequence`.

### Canonical active cards

| Card         | State at snapshot  | Outcome                                            |
| ------------ | ------------------ | -------------------------------------------------- |
| `t_7254400c` | running            | Correct existing PR #78 contract/scorer            |
| `t_a0427365` | running            | Biological OBS reviews 1–9/26                      |
| `t_9f15f45b` | done               | Biological OBS reviews 10–18/26                    |
| `t_993d234c` | running            | Biological OBS reviews 19–26/26                    |
| `t_5870e629` | dependency waiting | Consolidate exactly 26 reviews                     |
| `t_629bb1d0` | ready              | Fix semantic-placeholder handling for 60 additions |
| `t_c2b5fc6e` | ready              | Audit VAR Ensembl/species for exactly 120 families |
| `t_9f7052fd` | dependency waiting | Final exact 120-row OBS+VAR synthesis              |

Superseded cards are historical only and are not progress or current
dependencies.

## production

Latest local generated files (**ignored by Git, not part of a clean clone**):

- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.json`
- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.tsv`
- `artifacts/schema_audit/final_real_dataset_obs_var_20260717.md`
- `artifacts/schema_audit/final_real_dataset_missingness_review_20260717.tsv`

Verification procedure: read durable task record `t_a5bf1b1b` on board
`pert-gym`, then compare any local copies with `shasum -a 256`. Recorded digests
are JSON `60530cc3…`, TSV `de267a96…`, Markdown `3d906840…`, and flat TSV
`f2d6a07b…`. Regeneration source is currently outside the tracked repository, so
these files are evidence products rather than repository APIs.

Validated conservation:

| Unit                                  |                Verified |
| ------------------------------------- | ----------------------: |
| Real biological datasets/publications |                   70/70 |
| Logical families                      |                 120/120 |
| Physical members                      |             1,056/1,056 |
| Observations                          | 142,572,358/142,572,358 |
| Base-public real datasets             |                   26/26 |
| Base-public components                |                   60/60 |
| Base-public observations              | 110,398,202/110,398,202 |

The flat missingness report contains 1,550 `unknown` and 1,031 `not_applicable`
rows. Status vocabulary is exactly `unknown|not_applicable`; source/search
evidence is retained separately and no unresolved candidate is represented as
recovered.

`OBS_COMPLETED` and `VAR_ENSEMBL_SPECIES_COMPLETED` are orthogonal.
Storage/Zarr/chunking/X concerns never determine `OBS_COMPLETED`. Response-axis
`not_applicable` for PRISM/GDSC/Sanger is distinct from any separately joined
baseline-expression reference.

## VAR/Ensembl remediation

The remediation owner unit is `real_dataset_id`, never artifact or logical
family. The corrected just-in-time chain is:

1. dataset-level 70-row baseline contract `t_a8e5b268`;
2. continuation controller `t_5ec24c1a`;
3. bounded correction lanes by real dataset;
4. one independent verifier per outcome, combining exact tests/readback with
   semantic/provenance review;
5. final dataset-level JSON/TSV/Markdown certification.

The superseded 120-family remediation graph is held inert. Existing L01–L04
physical work can contribute evidence but does not define report granularity.
`goal_mode` is selective; deterministic verifier cards normally remain
single-shot.

1. **Stale PR #71 producer starved the queue.** A recovery card had been linked
   as parent of the stale original producer. When the recovery completed, the
   kernel correctly promoted its child — unfortunately the child was the
   obsolete producer. `active_pr` then refused it every minute. The old producer
   is now terminal. The supervisor excludes repeatedly guarded `ready` cards
   from executable continuity, detects recovery coverage that disappears while
   its source remains open, suppresses that wake after terminal source closure,
   and never rearms an unchanged successful event solely because a cooldown
   elapsed. Explicit failed-launch rearm and authoritative new event generations
   remain intact; the CTO prompt forbids reversed supersession links.
2. **GSE269572 hash contract initially failed closed.** A 64-character external
   SHA-256 was assigned to Lamin's 22-character native `Artifact.hash`. Zero
   Artifacts or links were created in that failed attempt. The corrected writer
   leaves native hashes Lamin-managed and retains the full generation-bound
   SHA-256 in provenance. Independent review passed; administrative ledger
   closure repaired registration **6→7/22** without product mutation.
3. **Datlinger17's first revision contained unsupported semantics.** Independent
   review rejected `control_availability=True` and vague
   `perturbation_type='genetic'`. Revision `0003` uses
   `dataset_control_available`, removes the unsupported perturbation type and
   leaves `x_semantics` absent. The final independent gate passed 38/38 fresh EU
   checks with exact 5,905-row parity, advancing **3→4/70 OBS** and **19→25/640
   Phase-A assignments**.
4. **Chang22 revision 2 is accepted.** Reviewer `t_4c7c7ae2` verified manifest
   `d2d41e1eadd91b48861f3de48a2e3311df58c92f1ea16aecd5801a540e9c04cf`, exact OBS
   `ue1GWkOr29VoRN5R0002`, all 42,277 rows and all eight Phase-A fields. The
   accepted ledger is now **5/70 OBS** and **33/640 assignments**.
5. **Dashboards were stale.** They still reported 2/22 families, 3 recompactions
   and 2/70 OBS. Both status files have been rebuilt from the live
   ledger/reviewer evidence. The CTO prompt now requires reconciliation after
   every accepted `product_delta` and forbids anticipating active-writer deltas.
6. **SCP1973 preflight had a process-probe false positive.** The probe matched
   its own SSH/shell ancestor and failed closed before mutation. The worker
   corrected self-ancestor exclusion, restarted with fresh leases/preflight and
   posted real writing/checkpointing heartbeats.
7. **VM capacity reverified.** Fresh independent terminal probe after GSE161824
   acceptance found exactly one 500-GB boot disk, 295,551,266,816 bytes free,
   32,784,683,008 bytes MemAvailable, idle/unassigned labels, and no active
   payload writer, tmux session, or lease. The previously attached extra disk is
   absent; this run performed no cleanup or destruction.
8. **GSE207360 is an authoritative mixed-species axis, pending ledger
   reconciliation.** Reviewer `t_051858ce` accepted append-only VAR
   `U8OeHI58YG9Y9Nsb0002` after exact 60,736-row live readback, but run 4073's
   top-level claim is rejected as `unit_mismatch`; it does not currently advance
   4/70.
9. **LINCS phase1 product evidence is accepted, pending ledger reconciliation.**
   Reviewer `t_953d8c3e` accepted VAR `3HNxm817WoemWsl10002` and X
   `QlcIPRMMk667dGwS0000`, but reconciliation run 4094 (`t_f4d2948f`) is
   rejected as `before_does_not_match_current`; it does not currently advance
   4/70.
10. **GSE220974 recompaction is accepted as the eighth outcome.** Reviewer
    `t_c34a72aa` independently rehashed all 16 sealed sources and 3,011
    generation-pinned outputs, reproduced exhaustive 24,661-row
    matrix/OBS/feature parity through the ordinary loader, verified exact
    Lamin/run/shared-VAR identity and exact no-op replay, and accepted
    `existing_recompactions_accepted` **7→8/32** with mismatch 0.
    `schema_fingerprint` remains honestly `unknown`.
11. **GSE161824 recompaction is accepted.** Reviewer `t_b8cf6151` independently
    selected the exact active VAR identity, rehashed all 108 selected sources
    and 2,215 generation-pinned outputs, reproduced exhaustive 176,040-row
    parity, and confirmed `EXACT_NOOP` replay with mismatch 0. The strict
    recompaction counter is now **7/32**.

## Accounting and safety

- `accepted_components`, registration, Collection membership, recompaction, OBS
  and VAR are distinct counters.
- The **70** OBS/VAR denominator counts `real_dataset_id` values, not families,
  physical members or evidence packets. Immutable integrity/lineage acceptance
  is separate from metadata missingness quality; absent or unsupported fields
  remain explicit rather than being inferred from parity.
- No producer, test, heartbeat, PR, dry-run or VM process changes a product
  counter without independent acceptance and mismatch-0 live readback.
- All GCS/Lamin/payload work runs on `pert-gym-worker-eu` in `europe-west1-b`;
  never on the Mac.
- Use `tools.lamin_context.connect_pertdata()` on `laminlabs/pertdata/jkobject`;
  never write Lamin `main`.
- Publication is append-only; preserve predecessor Collections and rollback
  identities; no deletion without explicit accepted gates.
- `wiki/` is obsolete; `AGENTS.md` is the sole boot file and durable detail
  belongs under `docs/`. ||||||| 1a12a6f
- #77 → `8030e9f3be46266f7b268af75567ae7b250f89f1`
- #79 → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`

### Critical open PR

- #78 current old head is CI-green/mergeable at GitHub but violates the
  corrected OBS field contract. `t_7254400c` is updating the same PR in its
  clean worktree. Merge only after the corrected exact head receives independent
  approval.

### Backlog

Card `t_c56390c3` is classifying every open PR. A PR may be merged only if
exact-head independent acceptance exists and no later finding contradicts it.
CI-green alone is insufficient. Stale/superseded PRs should be closed with their
canonical successor named.

- #77 → `8030e9f3be46266f7b268af75567ae7b250f89f1`
- #79 → `ad1cd8c22516993f5a8403837f25fec58ab4abf3`
- #78 → `b6c931e16abb325c5206aaa2fe10a2c4c1544164`

A PR may merge only with exact-head independent acceptance and no later
contradictory finding. CI-green alone is insufficient.

## Final convergence chain

1. `t_12667244`: publication/reconciliation macro-gate;
2. `t_fc3d4794`: complete versioned logical Collection;
3. `t_17ec66d9`: exhaustive denominator/shared-var/Zarr/loader/Collection test;
4. `t_61847c4c`: terminal Definition-of-Done acceptance;
5. `t_3df00bdb`: compact final project gate;
6. `t_e8f9c88c`: separate RxRx3 human access/EULA gate.

- Publication/reconciliation macro-gate `t_12667244` → complete versioned
  logical Collection `t_fc3d4794` → exhaustive
  denominator/shared-var/Zarr/loader/Collection test `t_17ec66d9`.
- Separate RxRx3 human access/EULA gate `t_e8f9c88c` runs in parallel with that
  path.
- Test `t_17ec66d9` and RxRx3 gate `t_e8f9c88c` converge into terminal
  Definition-of-Done acceptance `t_61847c4c`.
- Compact final project gate `t_3df00bdb` follows terminal acceptance and
  retains all six direct canonical parents: denominator `t_04b761eb`, loader
  contract `t_0cff18c2`, publication `t_12667244`, Collection `t_fc3d4794`, test
  `t_17ec66d9`, and terminal acceptance `t_61847c4c`.

### Bounded publication-wave topology

- Lane-3 macro `t_12667244` has 5 direct indispensable parents, replacing 76
  historical leaf parents.
- The 153 executable workload records occur exactly once across 13 waves (12 ×
  12 + 1 × 9), followed by two bundle gates; macro, wave, and bundle fan-in is
  at most 12.
- Immutable final gate `t_3df00bdb` remains at exactly six canonical parents.
- Missing metadata and quality findings remain dataset-local reports, not
  exclusions or sibling dependencies.
- New outcomes normally use one producer → independent-verifier chain. Split
  tester/reviewer gates only for genuinely distinct evidence or environments.

## Safety

- Lamin instance `laminlabs/pertdata`, branch `jkobject` only; never write Lamin
  `main`.
- Heavy operations run on `pert-gym-worker-eu`, not the Mac.
- One heavy writer; read-only audits and local PR work may run in parallel.
- Failed/no-manifest revisions are immutable evidence, not resumable accepted
  product.
- Shared checkout is intentionally not reset or broadly cleaned; implementation
  changes use isolated worktrees.
