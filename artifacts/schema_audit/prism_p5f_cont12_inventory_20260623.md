# PRISM P5F cont12 inventory — 2026-06-23

Recomputed from the current GCS staging listing and visible Lamin triplet keys on branch `jkobject`; no Lamin writes and no X matrix loads.

- GCS h5ad objects: 33
- GCS h5ad bytes: 99139295837
- Completed datasets: 16
- Accessible smoke-first candidates: 14
- Staged payload issues: 1
- Missing/source-blocked: 5
- User-excluded: 1
- Newly completed this run: `GSE269596`
- Next candidate: `GSE281048_TGFB_Perturb_seq`

`GSE269596` verification: 75/75 chunks, 74,312 rows, 36,601 vars, 7,394 controls, obs->X->var links and X payloads OK, required obs fields present, and standardized `cell_line` is not all `unknown` (`74,312/74,312` rows; values `HEK293T`, `K562`). The metadata repair revised obs parquet artifacts only and relinked existing X artifacts; it did not full-load or rewrite X.

## Remaining candidates by current GCS size

| dataset | bytes | uri |
| --- | ---: | --- |
| GSE281048_TGFB_Perturb_seq | 2767634550 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_TGFB_Perturb_seq.h5ad` |
| GSE251715 | 2823124959 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE251715.h5ad` |
| GSE241683_pilot | 2890310231 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE241683_pilot.h5ad` |
| GSE273271 | 3033728368 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE273271.h5ad` |
| GSE281048_IFNG_Perturb_seq | 3047289494 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_IFNG_Perturb_seq.h5ad` |
| GSE270828 | 3998858504 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE270828.h5ad` |
| GSE278572 | 4056291107 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE278572.h5ad` |
| GSE281048_IFNB_Perturb_seq | 4465857408 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_IFNB_Perturb_seq.h5ad` |
| GSE261025 | 4607920664 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE261025.h5ad` |
| GSE281048_TNFA_Perturb_seq | 4867288765 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_TNFA_Perturb_seq.h5ad` |
| GSE281048_INS_Perturb_seq | 5810523276 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE281048_INS_Perturb_seq.h5ad` |
| GSE235325 | 6212884683 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE235325.h5ad` |
| GSE241683_cropseq | 6664749343 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE241683_cropseq.h5ad` |
| GSE210681 | 26102404344 | `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE210681.h5ad` |
