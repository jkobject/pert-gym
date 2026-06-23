# Temporal SCP1467 Drosophila embryonic heart ingestion — 2026-06-23

Task: `t_d8f20272`
Status: `ingested_verified`

## Source

- SCP: `SCP1467` — Six cell types in the developing Drosophila embryonic heart.
- Raw GCS prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1467/`.
- Dataset prefix: `temporal_pretraining/scp1467_drosophila_embryonic_heart`.

## Representation decision

- Canonical `X.h5ad`: `Heart_counts.tsv`, stored as sparse cell × gene raw counts (`x_semantics = raw_counts`).
- Auxiliary `X_normalized_expression.h5ad`: `Expression_Heart_only.tsv`, stored as a typed same-prefix normalized-expression matrix linked from `obs.features['X_normalized_expression']`.
- Both matrices share the same 2,857-cell header and 9,034-gene order; metadata has one SCP `TYPE` row removed before obs construction.

## Duplicate gate

- Duplicate detected before write: `False`.
- Existing planned-prefix suffixes before write: `['obs.parquet', 'X.h5ad', 'var.parquet', 'X_normalized_expression.h5ad', 'var_normalized_expression.parquet']`.

## Verification

- obs rows: `2857`; var rows: `9034`; auxiliary var rows: `9034`.
- obs→X: `True`; X→var: `True`; X payload: `True`.
- obs→X_normalized_expression: `True`; aux X→aux var: `True`; aux payload: `True`.
- stage counts: `{'Stage13': 506, 'Stage14E': 775, 'Stage14L': 586, 'Stage15': 741, 'Stage16': 249}`.
- cell type counts: `{'1 Cardioblast': 247, '2 EPC (Eve+Tin+)': 482, '3 OPC (Odd+Tin-)': 1005, '4 CPC (Ct+Tin+)': 767, '5 WHPC (Eve+Tin-)': 147, '6 ELPC (Ct+Tin-)': 209}`.
