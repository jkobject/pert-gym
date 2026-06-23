# PRISM P5F Google Drive recovery status — 2026-06-22

- updated UTC: `2026-06-23T08:28:47.271643+00:00`
- staging prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- duplicate gate: `artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.json`
- duplicate hash artifact: `artifacts/schema_audit/prism_p5f_duplicate_hashes_20260622.tsv`
- completed now: `8` datasets
- accessible candidates remaining estimate: `22`
- staged payload issues: `1`
- duplicate-named pairs still needing hash compare: `0`
- still missing source rows: `5`
- user-excluded rows: `1`

## Completed and verified

| dataset | obs | vars | chunks | prefix | verification |
|---|---:|---:|---:|---|---|
| GSE255832 | 27912 | 32565 | 28 | `prism_collection/GSE255832` | `artifacts/schema_audit/prism_GSE255832_chunked_verification.json` |
| GSE263524 | 42289 | 32285 | 43 | `prism_collection/GSE263524` | `artifacts/schema_audit/prism_GSE263524_chunked_verification.json` |
| GSE236057 | 15866 | 28351 | 16 | `prism_collection/GSE236057` | `artifacts/schema_audit/prism_GSE236057_chunked_verification.json` |
| GSE267982 | 45808 | 32285 | 46 | `prism_collection/GSE267982` | `artifacts/schema_audit/prism_GSE267982_chunked_verification.json` |
| GSE247274 | 69907 | 22977 | 70 | `prism_collection/GSE247274` | `artifacts/schema_audit/prism_GSE247274_chunked_verification.json` |
| GSE241683_carT | 55213 | 36601 | 56 | `prism_collection/GSE241683_carT` | `artifacts/schema_audit/prism_GSE241683_carT_chunked_verification.json` |
| GSE252589 | 23297 | 68886 | 24 | `prism_collection/GSE252589` | `artifacts/schema_audit/prism_GSE252589_chunked_verification.json` |
| GSE243244 | 48587 | 32286 | 49 | `prism_collection/GSE243244` | `artifacts/schema_audit/prism_GSE243244_chunked_verification.json` |

## Staged payload issues

- `GSE274751`: staged_h5ad_truncated; `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE274751.h5ad` is 531,628,032 bytes but HDF5 stored EOF is 2,308,911,126 bytes. Next action: Re-stage/recover source before ingestion; do not retry current staged object.

## Duplicate resolution

- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247274.h5ad  cca43320462febf0da93458e6e97136759a01466f26cfc02680c5b84c244db84`
- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247274 (1).h5ad  cca43320462febf0da93458e6e97136759a01466f26cfc02680c5b84c244db84`
- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE267982.h5ad  b50b86d164c496208c35acedf7466ef52d2d6b1898a52295cbda212c074cec1a`
- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE267982 (1).h5ad  b50b86d164c496208c35acedf7466ef52d2d6b1898a52295cbda212c074cec1a`

## Remaining blockers / exclusions

- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Duplicate-resolved but not yet ingested: none. `GSE247274` canonical object is now ingested; do not ingest the redundant `(1)` copy.

## Notes

- GSE90063_human-004 remains excluded by user decision.
- GSE247274 and GSE267982 duplicate-named staged pairs were resolved with streaming SHA-256; both pairs are byte-identical.
- GSE247598, GSE261157, GSE272093, GSE272457, and GSE282731 remain missing from the staged recovery prefix.
- The first GSE255832 smoke chunk was written before ln.track() was patched into the chunker; subsequent P5F writes use tracked transform cqKr10EUOIPg0000.
- GSE263524 and GSE267982 both required idempotent resume after the 600s foreground cap; final verification passed for all chunks.
- GSE247274 canonical object was ingested after duplicate resolution; the redundant `(1)` staged object remains un-ingested and should not be used.
- GSE241683_carT was smoke-first ingested from the canonical staged object and verified as 56 same-prefix chunks; the full run hit the 600s foreground cap at chunk_0045 and was resumed with --overwrite from chunk_0045 to repair the partial obs+X/no-var chunk before final verification.
- GSE252589 was smoke-first ingested from the canonical staged object and verified as 24 same-prefix chunks (23,297 obs / 68,886 vars).
- GSE243244 was smoke-first ingested from the canonical staged object and verified as 49 same-prefix chunks (48,587 obs / 32,286 vars); the full run hit the 600s cap at chunk_0032 and was resumed with --overwrite from chunk_0032.
- GSE274751 staged object is corrupt/truncated: GCS size 531,628,032 bytes but HDF5 stored EOF is 2,308,911,126 bytes; re-stage before retry.
