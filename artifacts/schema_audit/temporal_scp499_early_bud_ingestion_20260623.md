# SCP499 early-bud blastema ingestion — 2026-06-23

## Summary

Recovered SCP499 idents/coordinates through public SCP visualization APIs because direct study-file endpoints returned HTTP 401 in prior probes. The API-derived sidecars were staged under an explicit `api_derived/` GCS prefix with byte-count verification, then the already-staged EB.matrix.txt.gz was ingested as a same-prefix obs/X/var triplet.

## Contract

- Matrix source: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499/EB.matrix.txt.gz`
- Derived sidecar prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499/api_derived`
- Sidecar rows: 2013
- Matrix parse: {'matrix_path': '/Users/jkobject/.openclaw/worktrees/pert-gym/t_102c5a38-scp499/data/gcs_cache/scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499/EB.matrix.txt.gz', 'cells': 2013, 'genes': 59171, 'nnz': 3876274, 'density': 0.03254331457918117, 'elapsed_seconds': 5.27, 'max_genes': None}
- Lamin prefix: `temporal_pretraining/gse121737_axolotl_blastema/early_bud_blastema`
- Verification: `{'prefix': 'temporal_pretraining/gse121737_axolotl_blastema/early_bud_blastema', 'payload_exists': {'obs': True, 'X': True, 'var': True}, 'obs_rows': 2013, 'var_rows': 59171, 'x_shape': [2013, 59171], 'x_nnz': 3876274, 'linked_x_key': 'temporal_pretraining/gse121737_axolotl_blastema/early_bud_blastema/X.h5ad', 'linked_var_key': 'temporal_pretraining/gse121737_axolotl_blastema/early_bud_blastema/var.parquet', 'cluster_counts': {'Macrophages': 476, 'Fibroblast-like blastema #4': 375, 'Fibroblast-like blastema #5': 326, 'Erythrocyte #1': 220, 'Erythrocyte #2': 215, 'T cells': 83, 'Basal wound epidermis': 81, 'Fibroblast-like blastema #3': 62, 'Early B cells': 59, 'Endothelial cells': 43, 'Myogenic blastemaa': 43, 'Neutrophils': 19, 'Tenocyte': 11}}`

## Caveat

The original SCP files `EB.idents.txt` and `EB.coordinates.txt` were not recovered byte-for-byte. Instead, their equivalent public API payloads (`Cluster/cell_values` and `clusters/tSNE coordinates`) were staged with explicit API-derived names and used for obs annotations/coordinates.
