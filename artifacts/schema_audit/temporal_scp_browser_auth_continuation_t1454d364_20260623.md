# Temporal SCP browser-auth continuation — 2026-06-23

Task: `t_1454d364`

Scope: continuation of the residual authenticated Broad Single Cell Portal recovery after the earlier `t_a469ca1d` attempt was blocked by the active PR #9 guard. This status records the browser-auth results already obtained via logged-in Chrome/computer-use and re-verifies the staged GCS payloads from this continuation run.

## Recovered and byte-verified on GCS

| SCP | file | role | GCS URI | bytes |
| --- | --- | --- | --- | ---: |
| `SCP1467` | `Expression_Heart_only.tsv` | expression | `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1467/Expression_Heart_only.tsv` | 100,298,692 |
| `SCP1467` | `Heart_counts.tsv` | expression/counts | `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1467/Heart_counts.tsv` | 51,951,316 |
| `SCP1467` | `Heartmetadata.tsv` | metadata | `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1467/Heartmetadata.tsv` | 508,836 |
| `SCP499` | `EB.matrix.txt.gz` | expression | `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499/EB.matrix.txt.gz` | 40,372,658 |

`gsutil stat` was run during this continuation and all four remote byte sizes matched the browser-auth recovery artifact. Local browser-downloaded copies had already been deleted after upload verification.

## Still blocked or deferred

- `SCP3301` / matched `GSE315712`: the SCP file table exposes raw and processed `WTintegrated` matrix families of identical reported size. The recommended next contract is the processed matrix family (`WTintegrated_processed_matrix.txt.gz`, genes, barcodes, metadata, clustering), but this needs a staged/chunked converter card rather than a blind dual-family download.
- `SCP211`: file table exposes one combined adult-kidney expression matrix plus multiple day-specific 10x-style matrices up to ~4.9 GB. Matrix-family selection is still needed before download/ingestion.
- `SCP282`: exposes seven sample/timepoint expression matrices plus metadata; total expression payload is ~2.7 GB before annotations. Needs explicit representation/chunking plan.
- `SCP3697`: public SCP record still exposes no study files (`study_files=[]`, zero cells/genes), so there is no payload to recover.
- `SCP499`: core early-bud matrix is staged; small `EB.idents.txt` and `EB.coordinates.txt` sidecars still need browser-auth recovery or a signed-url fallback before a fully annotated ingestion.

## Next cards

- `t_d8f20272`: ingest `SCP1467` from staged browser-auth files with a TSV/counts representation decision.
- `t_102c5a38`: complete `SCP499` sidecar recovery and then ingest the bounded early-bud matrix.
- `t_ffcf5bbf`: for `SCP3301`, `SCP211`, and `SCP282`, do a matrix-family selection/downloader planning pass before staging large multi-file payloads.
