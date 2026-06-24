# PRISM P5F recovery status — updated 2026-06-23 after GSE269596 cell_line repair

- staging prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- Lamin target: `laminlabs/pertdata` branch `jkobject`
- completed verified P5F datasets: `16`
- remaining smoke-first staged candidates: `14`
- staged payload issues: `1`
- missing/source rows: `5`
- user-excluded rows: `1`

## Newly completed in this update

- `GSE269596`: verified `75/75` same-prefix chunks at chunk size `1000`, shape `74,312 × 36,601`, controls `7,394`.
- source `cellline` is preserved in standardized `cell_line`: `74,312/74,312` rows non-unknown, values `HEK293T`, `K562`.
- verification artifacts: `artifacts/schema_audit/prism_GSE269596_source_probe_20260623.json`, `artifacts/schema_audit/prism_GSE269596_cell_line_obs_repair_20260623.json`, and `artifacts/schema_audit/prism_GSE269596_chunked_verification.json`.
- run note: staged object byte-verified at `2,505,068,264` bytes; backed layout `CSRDataset float32`; repair revised obs parquet artifacts only and relinked existing X artifacts, without full-loading or rewriting X.

## Completed verified P5F datasets

GSE236057, GSE236519, GSE241683_carT, GSE243244, GSE246714, GSE247274, GSE247599, GSE250558, GSE252589, GSE254100, GSE255832, GSE263524, GSE267982, GSE269596, GSE280767, GSE283614

## Next remaining candidates by staged object size

| dataset | GiB | bytes | uri |
|---|---:|---:|---|
| GSE281048_TGFB_Perturb_seq | 2.58 | 2767634550 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_TGFB_Perturb_seq.h5ad` |
| GSE251715 | 2.63 | 2823124959 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE251715.h5ad` |
| GSE241683_pilot | 2.69 | 2890310231 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE241683_pilot.h5ad` |
| GSE273271 | 2.83 | 3033728368 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE273271.h5ad` |
| GSE281048_IFNG_Perturb_seq | 2.84 | 3047289494 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_IFNG_Perturb_seq.h5ad` |
| GSE270828 | 3.72 | 3998858504 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE270828.h5ad` |

## Preserved blockers / exclusions

- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt (GCS size 531,628,032 bytes; HDF5 stored EOF 2,308,911,126 bytes). Do not retry until re-staged/recovered.
- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.
