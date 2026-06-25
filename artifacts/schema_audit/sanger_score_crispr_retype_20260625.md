# Sanger SCORE CRISPR retype repair — 20260625

- Task: `t_5fbcbcd0`
- Dry run: `False`
- Instance/branch: `laminlabs/pertdata` / `jkobject`
- Decision: Option B — preserve the score matrix as typed auxiliary `sanger_score_crispr/X_score.h5ad` linked to `var_score.parquet`, and revise canonical `X.h5ad` to an empty placeholder.

## Before

- Canonical X: `sanger_score_crispr/X.h5ad` shape `[1107, 17645]`, nnz `19532743`.
- Canonical var: `sanger_score_crispr/var.parquet` shape `[17645, 8]`.

## After

- obs→X now resolves to `sanger_score_crispr/X.h5ad` with shape `[1107, 0]` and nnz `0`.
- canonical X→var resolves to `sanger_score_crispr/var.parquet` with shape `[0, 0]`.
- obs→X_score resolves to `sanger_score_crispr/X_score.h5ad` with shape `[1107, 17645]`, nnz `19532743`, `x_semantics=essentiality_score`.
- X_score→var resolves to `sanger_score_crispr/var_score.parquet` with shape `[17645, 8]`.

## Loader contract

- `sanger_score_crispr` must not be selected as canonical expression/model-ready data from `X.h5ad`.
- Essentiality scores are available only through the typed auxiliary score payload path.
- DepMap/CCLE RNA remains a separate baseline expression artifact (`depmap_ccle/26q1`).
- Sanger GDSC loaders should continue to project IC50/AUC/RMSE response metrics from obs with empty X.
