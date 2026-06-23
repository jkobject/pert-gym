# PRISM P5F cont6 remaining inventory — 2026-06-23

- source: current status artifacts + live `gsutil ls`
- completed datasets preserved: `10`
- newly completed in this run: `GSE236519` (`20` chunks, `98,883 × 34,435`)
- remaining smoke-first candidates: `20`
- staged payload issues: `1`
- missing/source rows: `5`
- user-excluded rows: `1`

## Next candidates by staged object size

| dataset | GiB | bytes | uri |
|---|---:|---:|---|
| GSE254100 | 1.00 | 1078390433 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE254100.h5ad` |
| GSE250558 | 1.25 | 1339211295 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE250558.h5ad` |
| GSE247599 | 1.34 | 1433706825 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247599.h5ad` |
| GSE283614 | 1.41 | 1514297474 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE283614.h5ad` |
| GSE280767 | 1.87 | 2010340238 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad` |
| GSE269596 | 2.33 | 2505068264 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE269596.h5ad` |
| GSE281048_TGFB_Perturb_seq | 2.58 | 2767634550 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_TGFB_Perturb_seq.h5ad` |
| GSE251715 | 2.63 | 2823124959 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE251715.h5ad` |
| GSE241683_pilot | 2.69 | 2890310231 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE241683_pilot.h5ad` |
| GSE273271 | 2.83 | 3033728368 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE273271.h5ad` |

## Preserved blockers / exclusions

- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt; do not retry until re-staged.
- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.
