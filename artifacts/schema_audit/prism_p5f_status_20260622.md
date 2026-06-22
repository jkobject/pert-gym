# PRISM P5F Google Drive recovery status — 2026-06-22

- staging prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- duplicate gate: `artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.json`
- completed now: `2` datasets
- remaining smoke-first candidates estimate: `27`
- duplicate-named staged pairs blocked: `2`
- still missing source rows: `5`
- user-excluded rows: `1`

## Completed and verified

| dataset | obs | vars | chunks | prefix | verification |
|---|---:|---:|---:|---|---|
| GSE255832 | 27912 | 32565 | 28 | `prism_collection/GSE255832` | `artifacts/schema_audit/prism_GSE255832_chunked_verification.json` |
| GSE263524 | 42289 | 32285 | 43 | `prism_collection/GSE263524` | `artifacts/schema_audit/prism_GSE263524_chunked_verification.json` |

## Notes

- GSE90063_human-004 remains excluded by user decision.
- GSE247274 and GSE267982 remain blocked as duplicate-named staged object pairs until compared/cleaned.
- GSE247598, GSE261157, GSE272093, GSE272457, and GSE282731 remain missing from the staged recovery prefix.
- The first GSE255832 smoke chunk was written before ln.track() was patched into the chunker; subsequent P5F writes use the tracked transform cqKr10EUOIPg0000.
- GSE263524 was interrupted by the 600s foreground cap at chunk_0037; resume used --overwrite from chunk_0037 and final verification passed 43/43.
