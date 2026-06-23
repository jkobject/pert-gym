# PRISM P5F Google Drive recovery gate — 2026-06-22

Read-only reconciliation of the staged Google Drive recovery prefix against P5D/P5E residuals and visible Lamin triplet keys. No Lamin writes and no X matrix loads were performed.

- staging_prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- h5ad_objects: `33`
- h5ad_bytes: `99139295837`
- historical_blocked_rows: `36`
- historical_blocked_staged_datasets: `31`
- historical_blocked_missing_datasets: `['GSE247598', 'GSE261157', 'GSE272093', 'GSE272457', 'GSE282731']`
- decision_counts: `{'candidate_ingest_smoke_first': 29, 'block_duplicate_named_staged_object': 2, 'still_missing_source': 5, 'skip_user_excluded': 1}`
- lamin_instance: `laminlabs/pertdata`
- lamin_branch: `jkobject`

## Decisions

| dataset | staged_h5ad_count | staged_bytes | existing_family_overlap | decision | rationale |
| --- | --- | --- | --- | --- | --- |
| GSE210681 | 1 | 26102404344 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE235325 | 1 | 6212884683 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE236057 | 1 | 265170425 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE236519 | 1 | 1058023131 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE241683_carT | 1 | 323811925 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE241683_cropseq | 1 | 6664749343 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE241683_pilot | 1 | 2890310231 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE243244 | 1 | 395698500 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE246714 | 1 | 975289638 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE247274 | 2 | 1727221676 |  | block_duplicate_named_staged_object | Both canonical and browser duplicate-named staged h5ads exist for this dataset; compare/delete duplicate before ingestion. |
| GSE247598 | 0 | 0 |  | still_missing_source | Historical Drive-blocked row still has no staged h5ad under the recovery prefix. |
| GSE247599 | 1 | 1433706825 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE250558 | 1 | 1339211295 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE251715 | 1 | 2823124959 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE252589 | 1 | 835303027 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE254100 | 1 | 1078390433 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE255832 | 1 | 188161214 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE261025 | 1 | 4607920664 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE261157 | 0 | 0 |  | still_missing_source | Historical Drive-blocked row still has no staged h5ad under the recovery prefix. |
| GSE263524 | 1 | 203381564 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE267982 | 2 | 1405726480 |  | block_duplicate_named_staged_object | Both canonical and browser duplicate-named staged h5ads exist for this dataset; compare/delete duplicate before ingestion. |
| GSE269596 | 1 | 2505068264 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE270828 | 1 | 3998858504 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE272093 | 0 | 0 |  | still_missing_source | Historical Drive-blocked row still has no staged h5ad under the recovery prefix. |
| GSE272457 | 0 | 0 |  | still_missing_source | Historical Drive-blocked row still has no staged h5ad under the recovery prefix. |
| GSE273271 | 1 | 3033728368 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE274751 | 1 | 531628032 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE278572 | 1 | 4056291107 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE280767 | 1 | 2010340238 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE281048_IFNB_Perturb_seq | 1 | 4465857408 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE281048_IFNG_Perturb_seq | 1 | 3047289494 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE281048_INS_Perturb_seq | 1 | 5810523276 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE281048_TGFB_Perturb_seq | 1 | 2767634550 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE281048_TNFA_Perturb_seq | 1 | 4867288765 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE282731 | 0 | 0 |  | still_missing_source | Historical Drive-blocked row still has no staged h5ad under the recovery prefix. |
| GSE283614 | 1 | 1514297474 |  | candidate_ingest_smoke_first | Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate. |
| GSE90063_human-004 | 0 | 0 | prism_collection;scperturb | skip_user_excluded | Preserve explicit user exclusion due duplicate/subset ambiguity. |
