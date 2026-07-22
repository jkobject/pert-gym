# pert-gym current status

_Last verified: 2026-07-23 03:43 CEST from `TODO.md`, the strict accepted
product-delta snapshot, and the immutable reviewer handoffs named below. Pending
work is not counted. `TODO.md` remains the operational dashboard._

## Executive snapshot

| Metric | Accepted | Remaining | Strict evidence / note |
|---|---:|---:|---|
| Publication components | **29/153** | 124 | latest accepted component delta remains Temporal-v4 row 79 / SCP1467 |
| New families registered on `jkobject` | **10/22** | 12 | `Artifact/rt5eRz8opcJXtybp0000` |
| New families in the versioned Collection | **10/22** | 12 | `Collection/WBFxVN9Alr8zFt9T0000`; mismatch/drift 0 |
| Existing recompactions | **9/32** | 23 | GSE216673 reviewer `t_dfcb1549`; exact generation-pinned manifest below |
| Real datasets with accepted OBS recovery | **10/70** | 60 | accepted history includes `geo/GSE132080` reviewer `t_3bb03773`, OBS `lhR6Ny3n8QcVeItH0003`, plus current-main GSE197452; GSE150062 remains pending independent acceptance and receives no anticipatory credit |
| Frozen base-public OBS component×field candidates | **33/640** | 607 | SHA-256 `371b2b78c755c5cfdf8fa82d7826e5b2fdbfe48b67ca11e6d2d77ec7b6ff60c2` |
| Accepted OBS assignments outside that universe | **6** | — | depmap_ccle/26q1; intentionally no `/640` denominator |
| VAR dataset remediations | **8/70** | 62 | latest accepted delta: `scperturb/datlinger17` reviewer `t_2c228f48`, VAR `AYnivbGN3JCRzkN70001`; GSE150062 remains pending independent acceptance |
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

The strict OBS ledger is **10/70 datasets**, plus **33/640** assignments in the
frozen base-public candidate universe and **6** depmap_ccle/26q1 assignments
outside it. The outside-universe assignments do not change `/640`. The prior
named snapshot reached eight datasets through `SchiebingerLander2019`; the
authoritative product ledger owns the two additional accepted identities,
including the accepted GSE197452 and Datlinger17 outcomes. GSE150062
remains pending independent acceptance and contributes no counter delta in this
PR.

`GSE197452` is accepted on current main:
source-exhaustive readback proves exact 20,811×33,694 Illumina raw-count parity,
OBS `6UsaktwOJjkXPM3L0003` contains 20,784 exact guide-sequence joins, immutable
additions successor `ZTXfvA5YDoaqrd750000` has 1,018 unique-key members, and
verify replay wrote nothing. This accepted outcome is included in the strict OBS
counter above.

GSE213921 is **frozen after rejection 3/3**. It must not be auto-rerun. A future
attempt requires explicit operator authorization and a new bounded contract; it
does not currently advance any counter.

### VAR

The strict VAR ledger is **8/70** at the GSE150062 retry gate. After the prior
**6/70** snapshot, `geo/GSE132080` reviewer `t_3bb03773` authorized **6→7/70**
for exact VAR `GJ1HqkBSHfDD1o4m0002`; `scperturb/datlinger17` reviewer
`t_2c228f48` then authorized **7→8/70** for exact VAR
`AYnivbGN3JCRzkN70001`. GSE150062 remains pending independent acceptance and
contributes no counter delta in this PR. Its candidate boundary is explicit:
44,025 source-backed ENSG features, 16,401 source-native custom `LH` features
whose exact ENSG assignment is `not_applicable`, and 71 unresolved applicable
features retained as `unknown`; all 60,497 retain source identity, human species,
and X-axis parity.

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
