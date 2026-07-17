# pert-gym canonical schema contract

This document defines the target data contract for `laminlabs/pertdata` branch
`jkobject` before further large ingestion work. The goal is to make new pert-gym
datasets mergeable into `main` while remaining compatible with the existing
PertSchema-style `obs.parquet / X.h5ad / var.parquet` triplet convention.

## Scope

The contract applies to:

- existing `laminlabs/pertdata` datasets visible from branch `jkobject`;
- pert-gym additions under `prism_collection`, `viperturb`, `arc_vcc`,
  `sanger_*`, `depmap_ccle`, and future datasets;
- chunked datasets, where multiple triplet prefixes form one logical dataset.

It does not require every field to be non-null in every dataset. It does require
that missingness is explicit, auditable, and either repairable or documented.


## Unified Lamin Collection contract

P3 is not complete when triplets merely exist. It is complete when public
`laminlabs/pertdata` data plus pert-gym additions can be discovered through one
Collection family, filtered by simple metadata, and projected by loaders without
special-case archaeology.

### Target Collection family

Use a small, versioned family of `ln.Collection` records rather than one opaque
mega-object:

```text
pert-gym/base-public/<YYYY-MM-DD>
pert-gym/additions/<YYYY-MM-DD>
pert-gym/canonical/<YYYY-MM-DD>
pert-gym/model-ready/<YYYY-MM-DD>
```

Recommended aliases for the latest approved records:

```text
pert-gym/base-public/latest
pert-gym/additions/latest
pert-gym/canonical/latest
pert-gym/model-ready/latest
```

Definitions:

- `base-public`: public artifacts/datasets already visible from
  `laminlabs/pertdata` `main` or public branches. This is read-only from the
  pert-gym workflow except for branch-local annotations/revisions on `jkobject`.
- `additions`: artifacts created by pert-gym work on branch `jkobject`, including
  PRISM, VIPerturbSeq, Arc VCC, Sanger/DepMap screens, and future staged data.
- `canonical`: the union of selected public and addition members that satisfy at
  least `schema-audited` or have an explicit waiver. This is the default query
  surface for notebooks.
- `model-ready`: the subset that satisfies the `model-ready` level below. This
  is the default surface for benchmark loaders.

Versioned Collection records are immutable snapshots: create a new dated version
when membership or level assignments change materially. Do not mutate an old
version to mean something different. `latest` may point to the newest reviewed
version if Lamin aliasing/tagging supports it; otherwise store the latest pointer
in a small markdown/status artifact and keep the Collection name dated.

### Member granularity

Collection membership is artifact-level because Lamin Collections contain
artifacts, but pert-gym semantics are dataset-level. Use these rules:

1. A canonical triplet member is the `obs.parquet` artifact. Its linked
   `obs -> X -> var` artifacts define the loadable matrix.
2. For chunked/sharded datasets, every chunk `obs.parquet` is a member, and the
   shared logical dataset is recorded with `dataset_id`, `split`, and
   `chunk_id`/`chunk_index` metadata. Chunks are not separate biological
   datasets.
3. Include auxiliary artifacts only when typed and linked (`X_<name>/var_<name>`
   or `obsm_<name>`). They are not canonical expression members by themselves.
4. Exclude orphan/demo/reference artifacts from `canonical` and `model-ready`
   until they have a typed role and a join path to primary observations.
5. Do not add legacy excluded data such as the mistaken GSE150818 substitute to
   any pert-gym Collection.

### Member selection rules

A member may enter `canonical` when all are true:

- it belongs to a logical dataset intended for pert-gym, not a demo/model cache;
- it is active on branch `jkobject` or visible from public `main`;
- it is not a known duplicate/subduplicate without an explicit keep/merge reason;
- it has a valid membership row in the Collection manifest fields below;
- its harmonization level is at least `schema-audited`, or it has a documented
  temporary waiver with the next repair action.

A member may enter `model-ready` only when it satisfies the `model-ready` level.

### Collection manifest features

Every Collection member should have a manifest row, either as artifact features
in Lamin where practical or in a versioned Collection manifest TSV/parquet stored
under `artifacts/schema_audit/` and registered/linked to the Collection. Required
fields:

```text
dataset_id              # stable logical dataset id, e.g. prism_collection/GSE221321
source_accession        # GEO/SRA/DepMap/Figshare/Zenodo/CELLxGENE/etc. when known
source                  # scPerturb, Cellarity, LINCS, Tahoe, PRISM, DepMap, Sanger, ...
artifact_key            # obs artifact key for canonical members
artifact_role           # canonical_obs | canonical_X | canonical_var | X_<name> |
                        # var_<name> | obsm_<name> | reference | demo
logical_dataset         # grouped dataset id used by audits/loaders
prefix                  # triplet prefix without /obs.parquet
split                   # train | validation | test | plate | chunk | none
chunk_id                # stable chunk/plate/shard label, null for unchunked
chunk_index             # integer order when available
n_obs
n_vars
organism
cell_type
cell_line
tissue
disease
modality                # scRNA-seq | bulk_RNA | L1000 | screen | image | protein | ATAC | ...
assay
perturbation_type       # drug | CRISPRko | CRISPRi | CRISPRa | overexpression | cytokine | none | mixed | unknown
perturbation_technology
control_availability    # strict_control_available | relaxed_control_available |
                        # dataset_control_available | no_control_found | unknown
x_semantics             # raw_counts | normalized_expression | log1p_expression |
                        # delta_expression | signature | empty | unknown
has_obs_x_link          # bool
has_x_var_link          # bool
same_prefix_var         # bool for strict same-prefix triplets
var_key                 # explicit linked var artifact key; do not infer by prefix
var_uid                 # linked var artifact uid when recorded
var_hash                # hash used for exact shared-var validation when recorded
var_policy              # same_prefix | shared_exact_hash | shared_alias
var_alias_group         # optional logical shared-var group id
harmonization_level     # one of the levels below
harmonization_level_rank # 1..6 numeric rank for filtering/sorting
revision_status         # original | revised_obs | revised_var | revised_X | revised_triplet
branch                  # jkobject | main-visible | archive/trash excluded
collection_version
notes
```

Loader-facing notebooks should query this manifest first, then resolve the
`obs -> X -> var` links. They should not infer membership by scanning every
artifact key at runtime.

### Harmonization levels

Use monotonic levels. A dataset/chunk can move forward without destructive
rewrites by adding revised artifacts, links, or manifest updates.

1. `present-in-collection`: selected in a Collection/manifest, with stable
   `dataset_id`, source, role, and prefix. No claim is made about loadability.
2. `triplet-integrity-ok`: canonical `obs.parquet`, `X.h5ad`, and linked var
   artifacts exist; `obs -> X -> var` links resolve; row/feature counts are
   plausible; and the explicit var policy is satisfied. Most triplets use strict
   same-prefix `var.parquet`, while reviewed exact-hash chunk families may use a
   dataset-level shared `var.h5ad` recorded by `var_policy`, `var_key`, and
   `var_alias_group`.
3. `schema-audited`: required metadata coverage, X semantics, var ID class,
   control availability, duplicate status, and auxiliary roles have been audited
   and written to reports/manifest.
4. `loader-projectable`: a loader can reconstruct an AnnData-like object or
   response table with canonical field names from existing artifacts, using
   documented alias projection but without changing the stored payload.
5. `revised-canonical`: obs/var/X artifacts have been revised where needed so
   canonical columns and semantics are present directly in the stored triplet;
   prior artifacts remain as revisions/provenance, not overwritten.
6. `model-ready`: passes loader smoke tests, has control/split semantics needed
   for benchmark tasks, is duplicate-gated, and has explicit inclusion in the
   `pert-gym/model-ready/<YYYY-MM-DD>` Collection.

### Query UX contract

The intended notebook/API surface is simple:

```python
from tools.lamin_context import connect_pertdata
from pert_gym.collections import load_collection_manifest, load_member_adata

ln = connect_pertdata()
manifest = load_collection_manifest("pert-gym/canonical/latest")
subset = manifest.query(
    "organism == 'Homo sapiens' and modality == 'scRNA-seq' "
    "and perturbation_type in ['CRISPRko', 'CRISPRi']"
)
subset = subset[subset["harmonization_level_rank"] >= 4]
adata = load_member_adata(subset.iloc[0].artifact_key)
```

Until helper functions exist, notebooks should emulate this by loading the
versioned manifest table and resolving each row's `artifact_key` through Lamin.

## Storage envelope

Every loadable matrix dataset should be represented as a linked triplet:

```text
<dataset_prefix>/obs.parquet
<dataset_prefix>/X.h5ad
<dataset_prefix>/var.parquet
```

Reviewed exact-hash chunk families may instead link each chunk `X.h5ad` to one
dataset-level shared var alias:

```text
<logical_dataset>/var.h5ad
```

The manifest must record this explicitly with `same_prefix_var=False`,
`var_policy=shared_exact_hash` or `shared_alias`, and
`var_key=<logical_dataset>/var.h5ad`. Loaders must still follow `obs -> X -> var`;
they must not recover var by string replacement from the obs or X key.

Links:

- `obs.features["X"] -> X artifact`
- `X.features["var"] -> var artifact`

For chunked datasets, each chunk is a full triplet and the logical dataset is
recorded in `obs["dataset"]` plus the prefix structure.

Auxiliary modality payloads are allowed, but must be typed and named. They must
not masquerade as canonical expression triplets.

## Global obs identity contract

Every row in every canonical obs artifact must contain these columns:

| column | required | meaning |
|---|---:|---|
| `obs_uuid` | yes | Globally unique, stable UUID string for one observation row. |
| `original_obs_index` | yes | Exact source row/index label before pert-gym rewrites or chunking. |

`obs_uuid` is deterministic, not random. New ingestion and repair scripts should
use `pert_gym.obs_identity.add_obs_identity()` so the UUID material is stable
across reruns. The current namespace/version is `pert-gym.obs.v1`; do not change
it for backfills unless a future migration explicitly versions the contract.

The UUID material follows the reviewed 2026-06-24 audit contract exactly:

```text
uuid5(uuid.NAMESPACE_URL, "pert-gym.obs.v1:{dataset_id}:{prefix}:{original_obs_index}")
```

where `dataset_id` is the stable logical dataset id, `prefix` is the canonical
artifact prefix for the exact obs payload/chunk, and `original_obs_index` is the
source row/index label preserved before rewriting. The helper keeps compatibility
arguments for source accession, sample/barcode columns, chunk id, and row kind,
but those values are not part of the v1 UUID material; the canonical `prefix`
namespaces chunks and dataset-specific payloads.

If `original_obs_index` repeats inside a single payload, the helper appends the
zero-based row position only for those repeated labels. This keeps UUIDs unique
without discarding the original index value.

### Implementation API

Use the helper in `src/pert_gym/obs_identity.py`:

```python
from pert_gym.obs_identity import add_obs_identity, validate_obs_identity

obs = add_obs_identity(
    obs,
    dataset_id="prism_collection/GSE221321",
    prefix="prism_collection/GSE221321/chunk_0042",
)
validate_obs_identity(obs)
```

The helper returns a copy and does not mutate the input dataframe. Validation
requires both columns, non-empty values, valid UUID strings, and per-artifact
`obs_uuid` uniqueness. Cross-artifact uniqueness is achieved by including the
logical dataset and canonical prefix in UUID material.

### Rewrite policy

For broad backfills over existing canonical artifacts:

1. Load obs metadata only; do not materialize large matrices.
2. Capture source index into `original_obs_index` before any reset/reindex.
3. Call `add_obs_identity()` with stable `dataset_id` and canonical `prefix`.
4. Run `validate_obs_identity()` and a cross-artifact duplicate check on the
   generated `obs_uuid` values before publishing repaired obs artifacts.
5. Keep `obs -> X -> var` feature links unchanged when replacing only obs
   metadata.
6. Document dataset-id and prefix choices in the rewrite handoff so reruns can
   reproduce the UUIDs.

## Required global obs columns

Canonical columns that should exist where applicable:

```text
dataset
sample
cell_id
donor_id
batch
cell_type
cell_line
disease
tissue_type
organism
sex
age
ethnicity
sequencer
technology
assay
modality
media
is_bulk
is_pseudobulk
perturbation
perturbation_type
perturbation_technology
perturbation_library
guide_id
guide_sequence
perturbation_target
perturbation_target_id
is_control
dose
dose_unit
timepoint
trajectory_id
pseudotime
is_baseline
sensitivity
response_metric
response_value
response_source
n_counts
n_genes
pct_mito
pct_ribo
is_low_quality
```

For time, `timepoint` is stored in minutes whenever the original metadata can be
converted. Original text fields may be retained as provenance columns.

## Perturbation combinations

Combination perturbations use repeated obs columns with numeric suffixes rather
than nested structures.

Primary perturbation:

```text
perturbation
perturbation_type
perturbation_technology
perturbation_library
guide_id
guide_sequence
perturbation_target
perturbation_target_id
dose
dose_unit
```

Second and later perturbations append `_<N>`:

```text
perturbation_2
perturbation_type_2
perturbation_technology_2
perturbation_library_2
guide_id_2
guide_sequence_2
perturbation_target_2
perturbation_target_id_2
dose_2
dose_unit_2
```

Also include:

```text
combination_size
combination_id
```

## scRNA-seq and bulk RNA datasets

RNA expression is stored in `X`.

- Single-cell RNA: `is_bulk = false`, `is_pseudobulk = false`.
- Bulk RNA: `is_bulk = true`, `is_pseudobulk = false`.
- Pseudobulk derived from scRNA: `is_bulk = true`, `is_pseudobulk = true`.

Expected modality/assay examples:

```text
modality = scRNA-seq | bulk_RNA | L1000 | DRUG-seq
assay    = Perturb-seq | CRISPR screen readout | LINCS L1000 | DRUG-seq | RNA-seq
```

`X` must have explicit semantics in the audit manifest, for example:

```text
raw_counts
normalized_expression
log1p_expression
delta_expression
signature
empty
unknown
```

LFC is not stored in `X` for expression datasets. Store LFC as `X_lfc/var_lfc` when matrix-like, or `obsm_lfc` when embedding/obsm-like. A future loader should expose it alongside `X`.

## Essentiality / DepMap-like screens

For screens without measured RNA expression, do not store sparse zero matrices
that look like measured expression.

Use:

- response observations in `obs`;
- `X` empty or created dynamically by loaders;
- baseline expression joined by loader from CCLE/CMP-style expression datasets;
- response values in canonical response columns.

Recommended screen fields:

```text
depmap_id
sanger_model_id
cell_line
perturbation
perturbation_type
perturbation_technology
perturbation_library
gene_effect
dependency_score
lfc
auc
ic50
response_metric
response_value
response_source
has_expression
```

`has_expression = false` for screen rows that do not carry direct RNA.

Expression-derived essentiality should be explicitly labelled as proxy. Accepted
internal proxies are:

1. abundance/depletion over time or selection;
2. transcriptional stress/death signatures.

Use names such as `depletion_score`, `stress_response_score`, or
`expression_fitness_proxy`. Reserve `essentiality_score` for external or
validated essentiality labels.

## Temporal datasets

Temporal fields should exist for perturbation and non-perturbation experiments:

```text
timepoint       # minutes
trajectory_id
pseudotime
is_baseline
```

If no perturbation exists, use explicit values rather than nulls where possible:

```text
perturbation = "none" or "unperturbed"
perturbation_type = "none"
is_control = true
```

## Image datasets

No image datasets have been fully ingested yet. Target convention: do not force
RxRx/JUMP/Cell Painting-style data into fake scRNA expression. Raw/phenotypic
images remain external/staged image artifacts; `obs` carries plate/well/site/cell
and perturbation metadata; typed image-derived matrices carry features or
embeddings. Preferred extraction path for phenotypic microscopy images is
scPortrait (`https://mannlabs.github.io/scPortrait/pages/workflow.html`), which
segments raw microscopy images into single-cell image datasets and supports
downstream featurization/deep-learning embeddings.

- canonical scRNA-like `X` is absent/empty for pure image datasets;
- image-derived features use typed payloads such as `X_cellprofiler`,
  `X_scportrait_embedding`, or `X_recursion_dl_embedding` with matching `var_*`
  metadata;
- image metadata and image/cell identifiers live in `obs`.

Canonical image fields:

```text
image_uri
plate
well
site
channel
stain
magnification
segmentation_mask_uri
cell_id
single_cell_image_uri
scportrait_project_uri
modality = image
assay = CellPainting | microscopy | RxRx
```

## Spatial transcriptomics and temporal modality policy

User decision 2026-07-02:

- **Visium is expression/spatial transcriptomics**: treat it close to scRNA-seq
  for pert-gym ingestion/modeling. Store the RNA expression matrix in canonical
  `X.h5ad` when clear; preserve spot coordinates and image/spatial metadata in
  `obs` and typed sidecars.
- **STOmics / Stereo-seq are quasi-scRNA spatial expression**: treat them as
  expression-like RNA matrices, not image-only data.
- **Microscopy / live imaging** follows the image contract above, preferably via
  scPortrait single-cell image extraction and embeddings.
- **ATAC is out of scope for now**. Do not ingest ATAC subseries into canonical
  pert-gym artifacts until explicitly reopened.
- **Large RNA/spatial files are engineering work, not scientific blockers**:
  process them with backed/chunked converters on the project VM/GCP path
  (`pert-gym-worker-eu` when applicable), not by full materialization on the Mac
  mini.

## Multimodal and auxiliary matrices

For CITE-seq, ATAC-linked, image-derived, LFC, mutation/CNV, or other multimodal
payloads:

- RNA/expression remains in canonical `X.h5ad` when available.
- Matrix-like extra modalities use named artifacts:

```text
<prefix>/X_<name>.h5ad
<prefix>/var_<name>.parquet
```

  Examples: `X_protein`, `var_protein`; `X_atac`, `var_atac`; `X_lfc`,
  `var_lfc`; `X_cnv`, `var_cnv`.
- Embedding / `obsm`-style payloads use named artifacts:

```text
<prefix>/obsm_<name>.parquet
# or .h5ad/.zarr when sparse or very large
```

  Examples: `obsm_X_embedding`, `obsm_lfc`, `obsm_image_embedding`.
- Link these auxiliary artifacts from `obs`/canonical `X` via Lamin features and
  record semantics in the audit manifest. Future loaders may expose MuData.

Do not add methylation or proteomics datasets as priority pert-gym targets for
this ingestion phase unless explicitly requested. Existing public pertdata
multimodal artifacts are still included in schema audit.

## Excluded mistaken dataset and wanted PRoPER-seq target

The legacy GSE150818 mistaken dataset is excluded from pert-gym planning and should be deleted/archived if present in this branch's
pert-gym additions. It is not the desired dataset.

The target is **PRoPER-seq / ProPer-seq 2026 probe-based Perturb-seq**, only once
its real scRNA expression matrix/source is identified.

## var contract

For gene expression datasets:

- var index should ideally be stable Ensembl IDs (`ENSG...`, `ENSMUSG...`);
- retain author-provided gene identifiers as columns.

Canonical var fields:

```text
ensembl_id
gene_symbol
gene_id
organism
feature_type
author_gene_id
author_gene_symbol
```

The audit must classify each var as `ensembl`, `symbol`, `mixed`, `empty`, or
`unknown`. Canonical triplets must have same-prefix `var.parquet`; linked shared
vars for LINCS/Tahoe should be duplicated or aliased to same-prefix keys after
feature-identity verification.

## Registries and auxiliary artifacts

Use existing Lamin registries where possible:

- `bt.Gene` for genes;
- disease, tissue, cell type registries;
- GeneticPerturbation / Compound / PerturbationTarget where already populated.

Additional auxiliary concepts to create or map where possible:

- guide sequence;
- guide library;
- perturbation technology;
- media;
- patient/donor and cell-line registries;
- mutation and CNV covariate artifacts linked by cell-line/patient IDs.

Do not rely on pathway or target-annotation priors for the current benchmark
contract. Doses remain in `obs`.

## Control matching

For each perturbed observation, audit whether an unperturbed control is available
for the matching stratum.

Strict matching candidates:

```text
dataset
cell_type
cell_line
disease
sex
age
ethnicity
sequencer
organism
donor_id
batch
timepoint
```

Report tiers:

```text
strict_control_available
relaxed_control_available
dataset_control_available
no_control_found
```

The exact set of non-null matching fields used for each tier must be recorded in
the control availability report.

## Audit outputs

The canonical read-only audit writes local reports under
`artifacts/schema_audit/`:

```text
artifact_inventory.tsv
logical_dataset_manifest.tsv
triplet_integrity.tsv
obs_column_coverage.tsv
var_alignment.tsv
control_availability.tsv
x_semantics.tsv
duplicate_candidates.tsv
repair_plan.tsv
```

These reports are the gate before new large ingestion resumes.