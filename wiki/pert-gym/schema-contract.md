# Schema contract

The canonical schema is defined in detail at
[`docs/pert_gym_schema.md`](../../docs/pert_gym_schema.md).

This page is the wiki-level summary for agents resuming the project.

## Unified Collection/query contract

P3 is only complete once triplets are exposed through a reviewed Collection
family plus a simple manifest/query UX. Target versioned Collections:

```text
pert-gym/base-public/<YYYY-MM-DD>
pert-gym/additions/<YYYY-MM-DD>
pert-gym/canonical/<YYYY-MM-DD>
pert-gym/model-ready/<YYYY-MM-DD>
```

Use `obs.parquet` artifacts as canonical Collection members; `obs -> X -> var`
links define the loadable matrix. Chunked datasets contribute one member per
chunk obs artifact but share a logical `dataset_id`; chunks are shards, not
separate biological datasets. Auxiliary artifacts enter the Collection only with
typed roles such as `X_protein/var_protein` or `obsm_scvi`.

Required query metadata includes `dataset_id`, source accession, source, logical
dataset, artifact role/key, prefix, split/chunk fields, organism, cell type/line,
tissue, modality, assay, perturbation type/technology, control availability, X
semantics, link-integrity flags, branch, revision status, Collection version, and
harmonization level.

Harmonization levels are monotonic:

```text
present-in-collection
triplet-integrity-ok
schema-audited
loader-projectable
revised-canonical
model-ready
```

Loaders should query a versioned Collection manifest first, then resolve the
selected member's `obs -> X -> var` links. Do not scan all artifact keys at
runtime to infer membership.

## Triplet envelope

Every loadable matrix dataset should have:

```text
<prefix>/obs.parquet
<prefix>/X.h5ad
<prefix>/var.parquet
```

with feature links:

```text
obs -> X -> var
```

## Canonical obs themes

- identity: dataset, sample, cell/patient/donor/batch IDs;
- biology: cell type, cell line, disease, tissue, organism, sex, age, ethnicity;
- technique: sequencer, technology, assay, modality, media;
- perturbation: perturbation, perturbation type, technology, library, guide,
  guide sequence, target, control flag;
- time: timepoint in minutes, trajectory ID, pseudotime, baseline flag;
- response: response metric/value/source, sensitivity/proxy scores;
- QC: counts, genes, mito/ribo fractions, low-quality flag.

## Combination perturbations

Use `_2`, `_3`, ... columns for additional perturbations, not nested objects.
Keep `combination_size` and `combination_id`.

## var alignment

For expression datasets, index or column-level Ensembl IDs should be recoverable.
Audit reports classify var as Ensembl, symbol, mixed, empty, or unknown.

## X semantics

`X` must be interpretable. The audit labels it as raw counts, normalized/log1p
expression, delta/signature, empty, or unknown.

Expression lives in canonical `X`; LFC belongs in a typed auxiliary artifact
such as `X_lfc/var_lfc` or `obsm_lfc`.


## Auxiliary modalities

Additional matrices use `<prefix>/X_<name>.h5ad` plus
`<prefix>/var_<name>.parquet`; embedding-style payloads use
`<prefix>/obsm_<name>.parquet` or h5ad/zarr when large.
