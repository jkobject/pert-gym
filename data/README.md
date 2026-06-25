# pert-gym data catalogue and layout

This file is the durable reference for local data directories, staged raw data,
Lamin triplets, and dataset catalogue locations. Keep transient run logs and
one-off ingestion notes out of this file; use `artifacts/logs/`, dated reports in
`artifacts/schema_audit/`, or the project wiki instead.

## Source-of-truth map

- Latest project state, exact counts, and dated handoffs:
  [`../wiki/pert-gym/current-status.md`](../wiki/pert-gym/current-status.md)
- Data/schema contract and unified Collection rules:
  [`../docs/pert_gym_schema.md`](../docs/pert_gym_schema.md) and
  [`../wiki/pert-gym/schema-contract.md`](../wiki/pert-gym/schema-contract.md)
- Stable dataset/modality policy:
  [`../wiki/pert-gym/index.md`](../wiki/pert-gym/index.md), especially
  [`../wiki/pert-gym/dataset-modalities.md`](../wiki/pert-gym/dataset-modalities.md),
  [`../wiki/pert-gym/lamin-audit-and-branch-model.md`](../wiki/pert-gym/lamin-audit-and-branch-model.md),
  and [`../wiki/pert-gym/deduplication-policy.md`](../wiki/pert-gym/deduplication-policy.md)
- Human-facing project docs live under [`../docs/`](../docs/).
- Raw per-run evidence, probes, generated manifests, and logs live under
  `../artifacts/`; they are not the catalogue itself.

If these disagree, prefer the most recent dated status in
[`../wiki/pert-gym/current-status.md`](../wiki/pert-gym/current-status.md), then
[`../docs/pert_gym_schema.md`](../docs/pert_gym_schema.md), then this file.

## Directory layout and policy

| Path | Role | Durability policy |
| --- | --- | --- |
| `main/` | Local working area for source files that are being inspected, converted, or temporarily cached before Lamin/GCS staging. | Not a durable store. Keep huge raw files out of Git. Stage to GCS or write reviewed artifacts to Lamin, then clean local copies. |
| `gcs_cache/` | Local materialization of GCS objects for backed readers and bounded smoke checks. | Transient cache. Safe to delete and rebuild from `gs://scperturb/pert-gym/staging/...`. |
| `temporal_pretraining_datasets/` | Durable temporal dataset catalogue: tables, JSON, validation summary, and source mapping. | Versioned catalogue files are durable; raw scrape/output logs should stay in `artifacts/` or source-specific subdirectories. |
| `temporal_pretraining_sources/` | Source-specific temporal files used by follow-up ingestion/probes. | Working/source cache, not the canonical catalogue. Summaries belong in `temporal_pretraining_datasets/` and status/wiki pages. |

Current local examples include temporal regeneration raw files under
`main/temporal_t10_regen_injury_raw/`, STOmics spatial-temporal data under
`main/stomics_spatial_temporal/`, and cached staged objects under `gcs_cache/`.
Treat these as operational working copies unless a dated status report says a
specific file is the durable output.

## Raw, staged, cache, and Git policy

- Durable loadable data belongs in LaminDB on `laminlabs/pertdata`, branch
  `jkobject`, or in the GCS staging bucket before ingestion.
- Large raw downloads, raw `.h5ad`, `.tar`, `.rds`, zip files, extracted matrices,
  and `gcs_cache/` payloads should not be committed to Git.
- Stage large raw/source files to:

  ```text
  gs://scperturb/pert-gym/staging/
  ```

  The common layout mirrors repo-relative source paths, for example
  `gs://scperturb/pert-gym/staging/data/main/prism_collection/...` or
  `gs://scperturb/pert-gym/staging/data/main/tcell_gwps/raw/...`.
- Use `tools/stage_to_gcs.py` for local-to-GCS staging when possible; it verifies
  uploaded objects before deleting local copies.
- Use `tools/gcs_cache.py` or scripts that support `gs://`/range reads for Mac
  local smoke checks. Do not full-materialize 100GB+ matrices on the Mac mini.
- Clean repo-local Lamin/GCS caches after verification with
  `tools/clean_lamin_cache.py` and by deleting unnecessary `data/gcs_cache/`
  payloads.

## Lamin triplet relationship

Canonical expression-like datasets follow the pertdata triplet convention:

```text
<dataset_prefix>/obs.parquet
<dataset_prefix>/X.h5ad
<dataset_prefix>/var.parquet
```

The canonical member of a Collection is the `obs.parquet` artifact. Loaders
resolve the matrix and features through Lamin feature links:

```python
obs_artifact = ln.Artifact.get(key=f"{prefix}/obs.parquet")
x_artifact = obs_artifact.features.get_values()["X"]
var_artifact = x_artifact.features.get_values()["var"]

adata = x_artifact.load()
adata.obs = obs_artifact.load()
adata.var = var_artifact.load()
```

Rules that matter for this catalogue:

- Canonical triplets should use the same prefix for `obs`, `X`, and `var`.
- For chunked/sharded sources, each chunk has its own full triplet; chunks are
  shards of one logical dataset, not separate biological datasets.
- RNA/expression belongs in canonical `X.h5ad`.
- Extra matrix modalities use named artifacts such as `X_protein.h5ad` plus
  `var_protein.parquet`, `X_lfc.h5ad` plus `var_lfc.parquet`, or similar typed
  `X_<name>/var_<name>` pairs.
- Embedding/obsm-style payloads use typed artifacts such as
  `obsm_X_embedding.parquet` or `obsm_lfc.parquet`.
- Avoid vague untyped auxiliary blobs. Do not force image, screen, or response
  datasets into fake expression matrices.

See [`../docs/pert_gym_schema.md`](../docs/pert_gym_schema.md) for the full
obs/var/X contract, harmonization levels, and model-ready criteria.

## Unified Collections and count vocabulary

The current branch uses dated Lamin `ln.Collection` records rather than scanning
all visible artifacts at runtime:

```text
pert-gym/base-public/20260621
pert-gym/additions/20260621
pert-gym/canonical/20260621
pert-gym/model-ready/20260621
```

Use these exact count categories; do not collapse them into one "dataset count":

| Category | Current value | Meaning |
| --- | ---: | --- |
| pertdata global scale | 2000+ | Pertdata has 2000+ datasets globally across the broader resource. This is not the current pert-gym branch catalogue size. |
| latest visible Lamin artifacts | 3407 | Latest artifact records visible from branch `jkobject` at the P3R build time. |
| canonical collection members | 1056 | `obs.parquet` artifacts in `pert-gym/canonical/20260621`. |
| base-public collection members | 60 | Public/base obs-triplet members included in canonical. |
| pert-gym addition members | 996 | Branch-added obs-triplet members included in canonical. |
| logical datasets / families | 120 | Grouped biological/source dataset rows in the unified manifest. |
| triplet prefixes / chunks | 1056 | Canonical obs/X/var prefixes represented by collection members. |
| chunked members | 961 | Manifest rows where `split == chunk`. |
| non-chunk members | 95 | Manifest rows where `split == none`. |
| model-ready members | 1 | Reviewed loader-smoked v0 subset, currently separate from the 1056-member canonical query surface. |

Historical `110`/`111` counts were logical dataset/audit-subset counts, not
Lamin database size. Historical `720`/`721` counts were triplet-prefix counts
from the earlier metadata-only audit, not database size. The P3R canonical query
surface is triplet-integrity/query-surface complete; it is not a claim that every
member is model-ready training data.

The versioned manifest for the canonical Collection lives under
`../artifacts/schema_audit/`, notably:

```text
../artifacts/schema_audit/unified_collection_manifest_20260621.tsv
../artifacts/schema_audit/unified_collection_build_20260621.md
../artifacts/schema_audit/unified_collection_build_20260621.json
```

## Dataset catalogue locations

### Temporal pretraining catalogue

The current temporal catalogue lives in
[`temporal_pretraining_datasets/README.md`](temporal_pretraining_datasets/README.md).
Primary files:

```text
temporal_pretraining_datasets/temporal_pretraining_datasets_v4.tsv
temporal_pretraining_datasets/temporal_pretraining_datasets_v4.json
temporal_pretraining_datasets/temporal_pretraining_datasets_v4.md
temporal_pretraining_datasets/validation_v4.json
temporal_pretraining_datasets/ncbi_sra_bioproject_mapping_v0.tsv
```

The v4 catalogue has 150 retained temporal dataset/family rows, 0 `unclear`, 0
paper-only primary `dataset_url`, and 0 non-`temporal_yes` rows after the 1-6
source pass. The dated prioritization/read-only validation output is in
`../artifacts/schema_audit/temporal_pretraining_dataset_plan_20260621.md` and
`.json`.

Temporal ingestion status is tracked in
[`../wiki/pert-gym/current-status.md`](../wiki/pert-gym/current-status.md), with
separate sections for CELLxGENE limb, development batches, metabolic-labeling,
spatial-temporal, regeneration/injury, organoid, and related probes.

### PRISM and perturbation catalogues

PRISM, VIPerturbSeq, XAtlas/Orion, T-cell GWPS, Sanger/DepMap screens, Arc VCC,
RxRx/JUMP-style image sources, and PRoPER-seq status are summarized in
[`../wiki/pert-gym/current-status.md`](../wiki/pert-gym/current-status.md) and
modality policy pages under [`../wiki/pert-gym/`](../wiki/pert-gym/). Keep long
per-source investigation details in dated `../artifacts/schema_audit/` reports,
not in this README.

Important current source states:

- PRISM has chunked/canonical members plus a residual Drive/source queue; unresolved
  rows remain blocked by quota/private-link/source access and should not be marked
  done from metadata-only probes.
- T-cell GWPS `D4_Rest.assigned_guide` is represented as chunked canonical
  triplets; the remaining huge assigned-guide files require the same remote/range
  read strategy, not local full downloads.
- XAtlas/Orion raw files are staged on GCS, but no canonical triplets exist yet;
  local Mac full materialization is unsafe.
- STOmics spatial-development continuation is source/resource gated: ZESTA 5hpf
  is represented, but HESTA needs a source-manifest resolver, STDS0000060 starts
  at ~1.44 GB per processed H5AD, and MOSTA remains a multi-file atlas requiring
  staged/chunked handling. See
  `../artifacts/schema_audit/temporal_t35_stomics_spatial_probe_20260622.md`.
- Broad PRISM Repurposing, Sanger GDSC, Sanger SCORE, DepMap/CCLE, and
  Sanger dual-guide CRC have source-specific handling notes in current status and
  schema reports. Sanger SCORE is an essentiality-family screen, not RNA
  expression: canonical `sanger_score_crispr/X.h5ad` is intentionally empty after
  the 2026-06-25 retype, while the preserved score matrix lives in typed
  auxiliary `sanger_score_crispr/X_score.h5ad` plus `var_score.parquet` with
  `x_semantics=essentiality_score`. Loaders must not treat SCORE canonical `X` as
  model-ready expression. Sanger dual-guide CRC is verified complete as
  `sanger_dual_guide_crc/mapping_counts` with audit output at
  `../artifacts/schema_audit/sanger_dualguide_crc_verification_20260622.json`.
- PRoPER-seq remains source-TBD; the legacy GSE150818 substitute is excluded.
- PerturBase T29 row 113 (`GSE216481` / directed-differentiation TF atlas) is
  staged at
  `gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/GSE216481_RAW.tar`
  and range/filelist-probed, but not ingested. QC-pass RNA components
  `201218_RNA` and `210322_TFAtlas` are identifiable; ATAC and failed components
  must not enter canonical RNA `X.h5ad`. Ingestion is blocked until a verified
  encoded-barcode/ORF-to-TF-symbol and filtered-cell contract is available. See
  `../artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.md`.
- Temporal T19 source states: `E-MTAB-6967` whole-mouse-embryo processed archive is staged at
  `gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/E-MTAB-6967/atlas_data.tar.gz`
  and ready for a chunked MatrixMarket converter smoke; `E-MTAB-10843` is a source
  mismatch for the extended mouse organogenesis row; Descartes mouse embryogenesis
  still needs a real download/API endpoint. See
  `../artifacts/schema_audit/temporal_t19_mouse_embryo_batch_status_20260622.md`.
- Temporal T13G mouse gastrulation HCA is fully ingested under
  `temporal_pretraining/mouse_gastrulation_hca/`: 141 same-prefix triplet prefixes
  cover all 139,331 cells with 29,452 var rows per prefix. The production run
  streamed 483,512,215 MatrixMarket entries from the staged archive
  `gs://scperturb/pert-gym/staging/data/gcs_cache/mouse_gastrulation/atlas_data.tar.gz`
  without `mmread` or full `raw_counts.mtx` extraction, wrote/verified chunks
  `chunk_00000_00099` through `chunk_139100_139330`, and final read-back verification
  reported 0 link/payload/schema errors. See
  `../artifacts/schema_audit/temporal_t13g_mouse_gastrulation_full_status_20260622.md`.

### Human docs versus data details

- Put project-facing explanations, setup, API, CLI, model environment, and schema
  docs in [`../docs/`](../docs/).
- Put stable project knowledge, dataset/modality decisions, count vocabulary, and
  current handoffs in [`../wiki/pert-gym/`](../wiki/pert-gym/).
- Put machine-generated evidence, manifests, audit tables, and run outputs in
  `../artifacts/`.
- Keep this file as the data-layout/catalogue router plus the short operational
  policy for `data/`.

## What not to put here

Do not append transient ingestion logs, command transcripts, one-off TODO lists,
or stale "missing dataset" scratchpads to this README. If a run produces durable
knowledge, summarize it in the appropriate wiki/status page and link the dated
artifact report. If it only proves a command succeeded or failed, keep it in
`../artifacts/logs/` or the corresponding dated report.

- PRISM set of datasets
  https://www.biorxiv.org/content/10.64898/2025.12.23.696273v1 all h5ads are in
  https://drive.google.com/drive/folders/1Y0Z19JhiTmTch65kvBNNMdVtosH6QHfi
  - GSE217812
  - GSE90063_mouse
  - GSE250378-016
  - GSE90063_human
  - GSE261283
  - GSE264667
  - GSE269596
  - GSE270828
  - GSE241683_cropseq
  - GSE235325
  - GSE225775
  - GSE221321
  - GSE210681
  - GSE182308
  - GSE165291
  - GSE161824
  - GSE150062
  - GSE146194
  - GSE278572
  - GSE274751
  - GSE261025
  - GSE247599
  - GSE225807
  - GSE236304
  - GSE281048
  - GSE208240
  - GSE205310
  - GSE197452
  - GSE243244
  - GSE213511
  - GSE212396
  - GSE255832
  - GSE272093
  - GSE236057
  - GSE263747
  - GSE164996
  - GSE190604

- arc's perturbations for the Arc VCC: https://virtualcellchallenge.org/datasets

- broad's prism: https://depmap.org/repurposing/
- sanger's gdsc: https://www.cancerrxgene.org/
- sanger's drug combinations https://gdsc-combinations.depmap.sanger.ac.uk/
- sanger's score crispr KO: https://cellmodelpassports.sanger.ac.uk/downloads
- sanger's dual guid KO in CRC:
  https://www.nature.com/articles/s41467-025-67256-9#Abs1
  https://figshare.com/articles/dataset/MAPPING_zip/25533091/1?file=45433417

- recursion's rxrx:
  - https://www.rxrx.ai/rxrx3
  - https://www.rxrx.ai/rxrx1
  - https://www.rxrx.ai/rxrx2
  - https://www.rxrx.ai/rxrx19a
  - https://www.rxrx.ai/rxrx19b

- depmap off target analysis given the CRISPR library used
- depmap's CCLE: https://depmap.org/portal/download/
- sanger's CCLE: https://cellmodelpassports.sanger.ac.uk/

- also the atac ones:
  - Liscovitch-BrauerSanjana2021 #GSE161002
  - PierceGreenleaf2021
  - MimitouSmibert2021 #GSE156476
  - SchraivogelSteinmetz2020 #GSE135497

### other dropped:

- GSE140802: weinreb's dataset, 5 perturb, not scRNAseq

## check if not in prism or scperturb

- check overlap of prism and scperturb
- horlbeck: https://pubmed.ncbi.nlm.nih.gov/30033366/
- saunders: https://www.nature.com/articles/s41586-023-06720-2
- shifrut: https://www.sciencedirect.com/science/article/pii/S0092867418313333
- JUMP (check overlap with rxrx)

### recommandations:

- genetic perturbations: the target library used, together with the exact
  CAS9-construct should be available for all genetic perturbation datasets
- WGS/WES data should be available for all patient_id values

#### PerturBase T29 directed differentiation (GSE216481/GSE156170/GSE142078)
- Row 115 / `GSE142078` is accepted as an ingested, verified LUHMES CRISPRi day-8 triplet; keep the PerturBase 7,684-cell vs reconstructed 8,843-cell discrepancy as a QC note.
- Row 114 / `GSE156170` is `excluded_with_reason` from the active path by the 2026-06-23 user decision because bounded source probes found perturbation-label/QC ambiguity. Preserve that provenance, but do not reopen row114 as an active ingestion blocker.
- Row 113 / `GSE216481` is staged and source-probed but not ingested. QC-pass RNA components are `201218_RNA` and `210322_TFAtlas`; ATAC and failed/combinatorial components are excluded from canonical RNA `X.h5ad`. The row113 contract requires a barcode/sequence/numeric-id-to-ORF/TF-symbol map plus component-specific filtered-cell inclusion table before any canonical `perturbation` labels are written. See `../artifacts/schema_audit/temporal_t29_gse216481_row113_metadata_contract_20260623.md` and `../artifacts/scripts/validate_temporal_perturbase_t29_gse216481_contract_20260623.py`.

