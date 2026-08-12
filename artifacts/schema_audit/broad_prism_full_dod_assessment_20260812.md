# Broad PRISM full-DoD assessment — 2026-08-12

**Dataset:** `broad_prism_repurposing`

**Task:** `t_cf959e37`
**Status:** **BLOCKED / NO WRITE**

The exact machine-readable assessment is
[`broad_prism_full_dod_assessment_20260812.json`](broad_prism_full_dod_assessment_20260812.json).

## Evidence gathered

PR #116 was rebased onto exact `main` commit
`1a143ae4915d885c32de6a686cca6a4f0409a8f0`. The live read-only assessment used
PR head `a7690785169e001b0c223d00e2d1fc7f3c80e41e` after exact-head CI passed.
It ran only on `pert-gym-worker-eu` through
`tools/launch_pert_gym_heavy.py --task t_cf959e37 --verify-only`; the launcher
released the lease and returned the VM to `TERMINATED`. No Lamin write was
attempted or observed.

Live `jkobject` resolves the legacy triplet as:

| Role | UID | key | rows / n_obs | Collection membership |
|---|---|---|---:|---|
| OBS | `eKrJkcFDb9TEDbte0003` | `broad_prism_repurposing/obs.parquet` | 22,316,860 | none |
| X | `ah3Fl8EUHErIckk80000` | `broad_prism_repurposing/X.h5ad` | 22,316,860 | n/a |
| var | `T3YpaJB1Rt51Ef4U0000` | `broad_prism_repurposing/var.parquet` | 0 | n/a |

The empty non-gene var axis remains `not_applicable`; it must not be presented
as gene-level VAR.

The immutable source-row contract binds exactly 4,463,372 rows in
`Repurposing_Public_24Q2_LFC.csv` (SHA-256 `824149f9b9f3821eb520b385a5976e1a9977d86b21caf5d22171763800a40523`).
The 22,316,860-row live OBS is a
legacy structural expansion with synthetic-control and per-field rows. The
project has not accepted whether that expansion or the direct source-row table
is the canonical response-table unit. That choice changes OBS identity, X shape,
and model-ready semantics, so it is a publication blocker rather than a local
formatting detail.

## Fail-closed decision

The 13-gate full dataset DoD is not equivalent. In particular:

- exact source license evidence is still unknown;
- no accepted canonical row-unit contract exists;
- no same-snapshot bounded `jkobject` versus `main` equivalence receipt exists;
- keys are not under `data/cleaned/broad_prism_repurposing/`;
- the OBS has no Collection membership;
- no canonical target exists for clean-process loader/model-ready verification;
- no accepted plan/write/verify publication receipt exists;
- no reviewed `GCS_DECOMMISSION_READY` manifest or merged inventory/docs PR exists.

The candidate code also asserted `dose_unit=micromolar` and row-level
`disease=cancer` without exact source-row bindings. PR #116 now keeps both
applicable values `unknown` with `missing` state. That correction is code-only;
it does not authorize a live publication.

## Smallest safe next delta

1. Accept one exact row-unit contract: preserve the 22,316,860-row legacy
   expansion or reconstruct the 4,463,372-row sealed LFC source table.
2. Bind source license and per-field evidence. Keep `dose_unit` and row-level
   disease `unknown` unless exact evidence is added.
3. Implement and capture a bounded same-snapshot `jkobject`/`main` equivalence
   check.
4. Build a coherent `data/cleaned/broad_prism_repurposing/{obs.parquet,X.h5ad,var.parquet}`
   triplet with exact OBS/X denominator parity.
5. After plan hash authorization, publish append-only, create/update the intended
   versioned Collection, and verify from a clean public-loader/model-ready query.
6. Execute the processing notebook and retain immutable plan/write/verify
   receipts. Only then assess staging decommission and inventory/docs merge.

No source/staging deletion is authorized by this assessment.
