# PRISM P5F recovery status — updated 2026-06-23 after GSE254100

- staging prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- Lamin target: `laminlabs/pertdata` branch `jkobject`
- completed verified P5F datasets: `11`
- remaining smoke-first staged candidates: `19`
- staged payload issues: `1`
- missing/source rows: `5`
- user-excluded rows: `1`

## Newly completed in this update

- `GSE254100`: verified `5/5` same-prefix chunks at chunk size `5000`, shape `21,667 × 22,583`, controls `4,269`.
- verification artifacts: `artifacts/schema_audit/prism_GSE254100_chunked_verification.json` and `.md`.
- run note: staged object byte-verified at `1,078,390,433` bytes; backed layout `CSRDataset float64`; smoke chunk succeeded below 700 MB RSS; full run completed without full-loading the matrix.

## Completed verified P5F datasets

GSE236057, GSE236519, GSE241683_carT, GSE243244, GSE246714, GSE247274, GSE252589, GSE254100, GSE255832, GSE263524, GSE267982

## Next remaining candidates by staged object size

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

- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt (GCS size 531,628,032 bytes; HDF5 stored EOF 2,308,911,126 bytes). Do not retry until re-staged/recovered.
- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.
