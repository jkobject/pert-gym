# PRISM P5F Google Drive recovery status — 2026-06-22

- staging prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- duplicate gate: `artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.json`
- duplicate hash artifact: `artifacts/schema_audit/prism_p5f_duplicate_hashes_20260622.tsv`
- completed now: `4` datasets
- accessible candidates remaining estimate: `27` (includes duplicate-resolved GSE247274)
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

## Duplicate resolution

- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247274.h5ad  cca43320462febf0da93458e6e97136759a01466f26cfc02680c5b84c244db84`
- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247274 (1).h5ad  cca43320462febf0da93458e6e97136759a01466f26cfc02680c5b84c244db84`
- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE267982.h5ad  b50b86d164c496208c35acedf7466ef52d2d6b1898a52295cbda212c074cec1a`
- `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE267982 (1).h5ad  b50b86d164c496208c35acedf7466ef52d2d6b1898a52295cbda212c074cec1a`

## Remaining blockers / exclusions

- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.
- User-excluded: `GSE90063_human-004`.
- Duplicate-resolved but not yet ingested: `GSE247274` (canonical and `(1)` objects byte-identical; ingest canonical only).

## Notes

- GSE90063_human-004 remains excluded by user decision.
- GSE247274 and GSE267982 duplicate-named staged pairs were resolved with streaming SHA-256; both pairs are byte-identical.
- GSE267982 canonical object was ingested after duplicate resolution; GSE247274 remains an accessible duplicate-resolved candidate, not a hash blocker.
- GSE247598, GSE261157, GSE272093, GSE272457, and GSE282731 remain missing from the staged recovery prefix.
- The first GSE255832 smoke chunk was written before ln.track() was patched into the chunker; subsequent P5F writes use tracked transform cqKr10EUOIPg0000.
- GSE263524 and GSE267982 both required idempotent resume after the 600s foreground cap; final verification passed for all chunks.
