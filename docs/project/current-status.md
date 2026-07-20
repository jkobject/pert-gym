# pert-gym current status

_Last verified: 2026-07-20 12:36 CEST from `TODO.md`, the strict accepted
product-delta snapshot, and the immutable reviewer handoffs named below. Pending
work is not counted. `TODO.md` remains the operational dashboard._

## Executive snapshot

| Metric | Accepted | Remaining | Strict evidence / note |
|---|---:|---:|---|
| Publication components | **29/153** | 124 | latest accepted component delta remains Temporal-v4 row 79 / SCP1467 |
| New families registered on `jkobject` | **10/22** | 12 | `Artifact/rt5eRz8opcJXtybp0000` |
| New families in the versioned Collection | **10/22** | 12 | `Collection/WBFxVN9Alr8zFt9T0000`; mismatch/drift 0 |
| Existing recompactions | **9/32** | 23 | GSE216673 reviewer `t_dfcb1549`; exact generation-pinned manifest below |
| Real datasets with accepted OBS recovery | **6/70** | 64 | latest: depmap_ccle/26q1 reviewer `t_8d1e3b8c` |
| Frozen base-public OBS component×field candidates | **33/640** | 607 | SHA-256 `371b2b78c755c5cfdf8fa82d7826e5b2fdbfe48b67ca11e6d2d77ec7b6ff60c2` |
| Accepted OBS assignments outside that universe | **6** | — | depmap_ccle/26q1; intentionally no `/640` denominator |
| VAR dataset remediations | **4/70** | 66 | strict ledger; GSE207360 and LINCS evidence awaits conforming exact-once reconciliation |
| External exclusions | **60/60** | 0 | complete |

The canonical surface remains **120 logical families / 1,056 physical members /
142,572,358 observations**. Publication components, family registration,
Collection membership, recompaction, OBS, and VAR are separate units and must
not be added together.

## Accepted evidence and pending boundaries

### GSE216673 recompaction

GSE216673 is the ninth accepted recompaction. Reviewer `t_dfcb1549` independently
accepted the exact 25-Artifact source allowlist and immutable retry2 output after
public-loader readback, exhaustive matrix/OBS/shared-VAR parity, and exact no-op
replay. The accepted generation-pinned manifest is:

`gs://scperturb/pert-gym/staging/pert-gym/logical/prism_collection/GSE216673/revisions/t_414e4129_retry2_20260720T082230Z/manifest.json#1784537088866291`

This outcome advances only `existing_recompactions_accepted` **8→9/32**.

### OBS

The strict OBS ledger is **6/70 datasets**, plus **33/640** assignments in the
frozen base-public candidate universe and **6** depmap_ccle/26q1 assignments
outside it. The outside-universe assignments do not change `/640`.

GSE213921 is **frozen after rejection 3/3**. It must not be auto-rerun. A future
attempt requires explicit operator authorization and a new bounded contract; it
does not currently advance any counter.

### VAR

The strict VAR ledger remains **4/70**. GSE207360 product evidence is rejected at
the top-level ledger as `unit_mismatch`; LINCS phase1 evidence is rejected as
`before_does_not_match_current`. Both remain pending conforming exact-once
reconciliation, so neither is anticipated as accepted progress.

## Count vocabulary

- The publication workload is **213 records = 153 executable components + 60
  external exclusions**. Components are not new biological datasets.
- OBS and VAR use exactly **70 real datasets/publications = 26 base-public + 44
  additions**.
- The 60 base-public components map to 26 biological review units. Their frozen
  recoverable-existing OBS candidate universe has denominator 640; additions
  outside that universe never increment `/640`.
- Missing applicable metadata remains `unknown`; genuinely inapplicable metadata
  is `not_applicable`.

## Final convergence and blockers

The durable convergence path is publication macro-gate `t_12667244` → complete
Collection `t_fc3d4794` → exhaustive Collection/shared-var/Zarr/loader test
`t_17ec66d9` → terminal acceptance `t_61847c4c` → immutable final gate
`t_3df00bdb`.

RxRx3 (`t_e8f9c88c`) remains the separate human-only Auth0/portal and Recursion
EULA blocker; it must not serialize accessible dataset work.

## Operating rules

- One heavy product writer at a time. Heavy payload, GCS, and broad Lamin work
  runs only on `pert-gym-worker-eu` in `europe-west1-b`, never on the Mac.
- Connect only through `tools.lamin_context.connect_pertdata()` to
  `laminlabs/pertdata`, branch `jkobject`; never write `main`.
- Publication and recompaction are append-only. Preserve old generations and
  rollback identities; no deletion without explicit accepted gates.
- Producers, tests, heartbeats, PRs, staging objects, and VM processes are not
  product progress. Credit requires independently accepted mismatch-0 live
  readback.
- `wiki/` is obsolete. `AGENTS.md` is the single boot file; durable detail lives
  under `docs/`.