# PRISM P5F cont7 remaining inventory — 2026-06-23

- source: current status artifacts + live GCS check for `GSE254100`
- completed datasets preserved: `11`
- newly completed in this run: `GSE254100` (`5` chunks, `21,667 × 22,583`)
- chunk-size rationale: Source is 1.0 GiB with backed CSRDataset float64 layout and only 21,667 rows; smoke chunk at 5,000 rows peaked below 700 MB RSS, so 5,000-row chunks reduced metadata overhead without full-loading the matrix.
- remaining smoke-first candidates: `19`
- staged payload issues: `1`
- missing/source rows: `5`
- user-excluded rows: `1`

## Next candidates by staged object size

| dataset | GiB | bytes | uri |
|---|---:|---:|---|
| GSE250558 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE250558.h5ad` |
| GSE247599 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247599.h5ad` |
| GSE283614 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE283614.h5ad` |
| GSE280767 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad` |
| GSE269596 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE269596.h5ad` |
| GSE281048_TGFB_Perturb_seq | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_TGFB_Perturb_seq.h5ad` |
| GSE251715 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE251715.h5ad` |
| GSE241683_pilot | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE241683_pilot.h5ad` |
| GSE273271 | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE273271.h5ad` |
| GSE281048_IFNG_Perturb_seq | 0.00 | 0 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_IFNG_Perturb_seq.h5ad` |

## Preserved blockers / exclusions

- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt; do not retry until re-staged.
- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.
