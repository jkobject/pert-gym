# Temporal-v4 row 132 — SCP1846 / GSE202155

Task: `t_03c886aa`

## Frozen scope

- Record: `temporal_v4_132_overlapping_transcriptional_programs_promote_survival_and_axonal_regeneration_of`
- Logical key: `pert-gym/logical/temporal/overlapping_transcriptional_programs_promote_survival_and_axonal_regeneration_of`
- Frozen catalogue denominator: 129,441 observations.
- Frozen manifest SHA-256: `ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4`.
- Wave graph SHA-256: `59c18752f65257270b980353811da5bf554d5ac2b6c11c550a63849664ce9c98`.

The SCP1846 publication maps to GEO SuperSeries GSE202155. Its twelve GSE201254 processed 10x count matrices total exactly 129,441 cells and share one ordered 23,308-gene axis. The related GSE202154 Smart-seq2 subseries has 411 cells and 46,078 genes; it was intentionally not merged because it is a distinct assay and would violate the frozen SCP1846 denominator.

## Build

Executed only on `pert-gym-worker-eu` (`europe-west1-b`). All twelve GEO members were downloaded and checked against the sizes and SHA-256 identities frozen in `build_component.py`. No Lamin or Collection writes occurred.

Result:

- shape: 129,441 × 23,308
- nnz: 528,649,855
- raw count sum: 2,017,575,951
- revision: `temporal-v4-132-wave13-5f7d704794fcd538`
- immutable manifest: `gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/overlapping_transcriptional_programs_promote_survival_and_axonal_regeneration_of/revisions/temporal-v4-132-wave13-5f7d704794fcd538/manifest.json#1784284536286438`
- manifest SHA-256: `dc33b1a1e1d24e96f3ce8efce8d052d1b01c2e7102d335a0ebf866ebc63e92a6`
- writer verdict: PASS
- independent generation-qualified verifier verdict: PASS

The independent verifier reread all nine physical members by immutable generation, checked every object SHA-256/size, verified the manifest was written last, and checked H5AD sparse encoding, shape, nnz, count sum, and ordered obs/var axis parity.

## Missingness and ledger

No source cell was filtered. Age, sex, cell type, and source quality annotation remain explicit nulls. Counts, detected genes, mitochondrial fraction, and ribosomal fraction were computed without rejection.

The row-specific accepted-component credit was 0 before and after publication; the producer did not self-accept. The global accepted-components helper is currently fail-closed because task `t_98a0a434` run 3771 has a malformed administrative replay binding after the last contiguous 4/153 owner. This limitation is preserved in the immutable ledger evidence. Independent administrative repair/review is required before any accepted-product credit can be awarded.

## Verification commands

- `uv run python -m py_compile build_component.py verify_component.py`
- `uv run ruff check --ignore E701,E702 build_component.py verify_component.py`
- Remote writer invocation is captured in `writer.log`.
- Remote independent verifier invocation is captured in `verifier.log`.
