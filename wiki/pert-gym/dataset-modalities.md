# Dataset modalities

## scRNA-seq perturbation

RNA expression in `X`; perturbation, biological, technical, time, control, and
response metadata in `obs`; gene metadata in `var`.

Compute later:

- pseudobulk expression per perturbation/control stratum;
- LFC versus matched controls;
- expression-derived depletion/stress proxies where justified.

## Bulk RNA / L1000 / DRUG-seq

Treat as RNA-compatible with `is_bulk = true`. Store expression/signature in `X`
with explicit `X_semantics`. Store LFC as `X_lfc/var_lfc` or `obsm_lfc`.

## Essentiality and DepMap-like screens

Use `obs` for response rows. Do not store sparse zeros as fake expression. The
loader can create empty `X` or dynamically join baseline expression such as CCLE.

## Temporal datasets

Use `timepoint` in minutes, `trajectory_id`, `pseudotime`, and `is_baseline`.
These fields should also exist in perturbation experiments.

## Image datasets

Keep `X` empty. Future image embeddings go in `obsm["X_embedding"]`; raw image
and segmentation URIs live in `obs`.

## Multimodal and auxiliary payloads

RNA goes in canonical `X`. Other matrix-like modalities become named auxiliary
matrices: `X_protein/var_protein`, `X_atac/var_atac`, `X_lfc/var_lfc`,
`X_cnv/var_cnv`, etc. Embedding-style payloads become `obsm_<name>` artifacts
(for example `obsm_X_embedding` or `obsm_lfc`). A future loader can return
MuData.

## PRoPER-seq / ProPer-seq 2026

The wanted dataset is PRoPER-seq / ProPer-seq 2026 probe-based Perturb-seq, only
after finding its actual scRNA expression source. Do not substitute legacy GSE150818 data; that mistaken dataset is excluded from pert-gym.

## Out of current scope

Do not prioritize new methylation/proteomics ingestion in this phase. Mutation
and CNV are key covariates, but should be stored as linked auxiliary artifacts, not copied into every obs row.
