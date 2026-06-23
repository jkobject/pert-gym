# PRISM P5F cont8 remaining inventory — 2026-06-23

- source: current status artifacts + live GCS/backing check for `GSE250558`
- completed datasets preserved: `12`
- newly completed in this run: `GSE250558` (`60` chunks, `59,837 × 36,713`)
- chunk-size rationale: Source is 1.25 GiB with backed CSRDataset int32 layout, 59,837 rows, duplicate obs names, and 36,713 genes; kept conservative 1,000-row chunks after a smoke chunk to bound per-chunk materialization and preserve the P5F recovery pattern.
- remaining smoke-first candidates: `18`
- staged payload issues: `1`
- missing/source rows: `5`
- user-excluded rows: `1`

## Next candidates by staged object size

| dataset | GiB | bytes | uri |
|---|---:|---:|---|
| GSE247599 | 1.34 | 1433706825 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247599.h5ad` |
| GSE283614 | 1.41 | 1514297474 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE283614.h5ad` |
| GSE280767 | 1.87 | 2010340238 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad` |
| GSE269596 | 2.33 | 2505068264 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE269596.h5ad` |
| GSE281048_TGFB_Perturb_seq | 2.58 | 2767634550 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_TGFB_Perturb_seq.h5ad` |
| GSE251715 | 2.63 | 2823124959 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE251715.h5ad` |
| GSE241683_pilot | 2.69 | 2890310231 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE241683_pilot.h5ad` |
| GSE273271 | 2.83 | 3033728368 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE273271.h5ad` |
| GSE281048_IFNG_Perturb_seq | 2.84 | 3047289494 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_IFNG_Perturb_seq.h5ad` |
| GSE270828 | 3.72 | 3998858504 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE270828.h5ad` |

## Preserved blockers / exclusions

- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt; do not retry until re-staged.
- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.
