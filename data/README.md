# Data

- `main/`: primary datasets used by the project.
- `others/`: auxiliary or experimental datasets, will be ignored by default in
  the project code.
- `temporal_pretraining_datasets/`: deep-research handoff for temporal
  scRNA-seq, spatial transcriptomics, perturbation time-course, organoid,
  developmental trajectory, and microscopy datasets that could seed a
  development/pretraining database.

## Temporal pretraining dataset catalogue

The current handoff lives in `temporal_pretraining_datasets/README.md`.

Current primary files:

- `temporal_pretraining_datasets/temporal_pretraining_datasets_v4.tsv`
- `temporal_pretraining_datasets/temporal_pretraining_datasets_v4.json`
- `temporal_pretraining_datasets/temporal_pretraining_datasets_v4.md`
- `temporal_pretraining_datasets/validation_v4.json`
- `temporal_pretraining_datasets/ncbi_sra_bioproject_mapping_v0.tsv`

The v4 table contains 150 temporal dataset/family rows, with 0 `unclear`, 0
paper-only `dataset_url`, and 0 non-`temporal_yes` rows after the 1-6 source
pass described in the handoff README.

## format of files

- all datasets have 3 files. Starting from their /obs.parquet one can do:

```python
obs_artifact = db.Artifact.get(
    key=FILENAME+"/obs.parquet"
)
X_artifact = obs_artifact.features.get_values()["X"]
var_artifact = X_artifact.features.get_values()["var"]

adata = X_artifact.load()
adata.obs = obs_artifact.load()
adata.var = var_artifact.load()
```

### format of files [future]

- image files: are converted to a spatialdata file. Using scPortrait, we
  generate a cell-specific embedding of each cell and cell specific annotations
  are then put in the obs. Then, Based on cell state and cell count, a sensitivy
  value is computed and stored in the `obs`, per cell.
- sensitivity files: are converted to an anndata file where the sensitivty
  values are stored in the `obs` dataframe and expression values (often defined
  as bulk expression) are stored in the `X` matrix.
- scRNA-seq: are stored as anndata files where the expression values are stored
  in the `X` matrix. based on cell state and cell count a sensitivy value is
  computed and stored in the `obs` dataframe
- scATAC-seq files are stored as mudata files where we compute both gene
  activity and peak accessibility ⚠️ unused for now
- CITE-seq / SHARE-seq files (protein / ATAC with matched scRNA-seq are process
  similarly and stored together in the mudata).

Each file is listed as an artifact where the a description is provided along
with the data type, number of datapoints (n_obs), file size, schema id, and a
link to the file of course.

## annotations

Annotations follow the same format as what is described into the db.Features
field. a set of these features (i.e. a Schema, see db.schema) is present given
the dataset and the file format (image / sensitivty / scRNAseq).

## Phase 3 datasets — format notes

### scRNA-seq / Perturb-seq

#### XAtlas / Orion (Figshare 29190726)
- **Source:** `https://plus.figshare.com/articles/dataset/...29190726`
  - HCT116: `https://plus.figshare.com/ndownloader/files/55021257` (~350 GB)
  - HEK293T: `https://plus.figshare.com/ndownloader/files/55074802` (~150 GB)
- **Format:** two `.h5ad` files; too large for full in-memory load → backed-mode
- **Perturbation type:** CRISPRi, dual-guide, genome-wide
- **Cell lines:** HCT116, HEK293T (human)
- **Organism:** human
- **Key obs columns:** `guide_id`, `n_counts`, `n_genes`, `perturbation`
- **Ingestion:** `tools/ingest_xatlas_orion.py` — `run_xatlas_orion_pipeline()`

#### T-cell Genome-Wide Perturb-seq (GSE314342 / SRP643211)
- **Source:**
  - AWS S3 (public): `s3://genome-scale-tcell-perturb-seq/marson2025_data/`
  - CZI Virtual Cells: `https://virtualcellmodels.cziscience.com/dataset/genome-scale-tcell-perturb-seq`
  - GEO raw: `https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE314342`
- **Format:** `.h5ad` cell-level count matrices + pseudobulk + DE estimates; very large
- **Perturbation type:** CRISPRi, genome-wide (~all protein-coding genes)
- **Cell type:** primary human CD4+ T cells (4 donors, 3 stimulation conditions: rest, TCR, TCR+IL2)
- **Scale:** ~22 million cells
- **Key obs columns:** `perturbation_name`, `donor_id`, `stimulation_condition`, `is_control`
- **Special handling:** backed-mode reads, chunked ingestion (similar to Orion)

#### VIPerturbSeq (Zenodo 18460279)
- **Source:** `https://zenodo.org/records/18460279`
- **Paper:** `https://www.biorxiv.org/content/10.64898/2026.02.12.705613v1.full`
- **Format:** `.h5ad` files via Zenodo API
- **Perturbation type:** CRISPRi genome-wide (GuEST-List library, combinatorial indexing)
- **Organism:** human
- **Workflows:** unbiased genome-wide screen + phenotypically enriched (VIP-enrichment)
- **Key obs columns:** `guide_id`, `perturbation`, `is_vip_enriched`, `n_counts`, `n_genes`
- **Access:** Zenodo REST API `https://zenodo.org/api/records/18460279`

#### PROPER-seq (GSE150818)
- **Source:** `https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE150818`
- **Paper:** Molecular Cell 2021, `https://www.cell.com/molecular-cell/fulltext/S1097-2765(21)00574-8`
- **Format:** count matrix + metadata via GEO; also processed `.h5ad` in scPerturb
- **Perturbation type:** CRISPR KO (endogenous regulatory elements + ORFs)
- **Cell line:** K562 (human, chronic myelogenous leukemia)
- **Organism:** human
- **Key obs columns:** `perturbation`, `perturbation_type`, `guide_id`, `n_counts`, `n_genes`
- **Note:** check scPerturb (Zenodo 13350497) first — may already be ingested

#### PRISM Perturb-seq Collection (~36 datasets, Google Drive)
- **Source paper:** `https://www.biorxiv.org/content/10.64898/2025.12.23.696273v1`
- **Data:** Google Drive `https://drive.google.com/drive/folders/1Y0Z19JhiTmTch65kvBNNMdVtosH6QHfi`
- **Format:** standardised `.h5ad` files (one per GEO accession); accessible via `gdown`
- **Perturbation types:** CRISPRi / CRISPRko — mixed across studies
- **Organisms:** human (most), some mouse
- **Key obs columns:** `perturbation`, `perturbation_type`, `cell_line`, `organism`,
  `n_counts`, `n_genes`, `is_control` — harmonised across all 36 studies
- **Datasets (36 total):** GSE217812, GSE90063_mouse, GSE90063_human, GSE250378-016,
  GSE261283, GSE264667, GSE269596, GSE270828, GSE241683_cropseq, GSE235325,
  GSE225775, GSE221321, GSE210681, GSE182308, GSE165291, GSE161824, GSE150062,
  GSE146194, GSE278572, GSE274751, GSE261025, GSE247599, GSE225807, GSE236304,
  GSE281048, GSE208240, GSE205310, GSE197452, GSE243244, GSE213511, GSE212396,
  GSE255832, GSE272093, GSE236057, GSE263747, GSE164996, GSE190604
- **Batch ingest:** `migrate_h5ad_to_triplet` per file

#### Sanger Dual-guide KO in CRC (Figshare 25533091)
- **Source:** `https://figshare.com/articles/dataset/MAPPING_zip/25533091/1?file=45433417`
- **Paper:** Nature Communications 2025, DOI `10.1038/s41467-025-67256-9`
- **Format:** `MAPPING.zip` containing processed genetic-interaction score matrices and
  per-cell count data (likely `.h5ad` or tab-separated matrices)
- **Perturbation type:** dual-guide CRISPR KO (tRNA spacer system, pairwise interactions)
- **Cell line:** colorectal cancer (CRC) lines (Sanger panel)
- **Organism:** human
- **Key obs columns:** `guide_pair`, `gene_pair`, `cell_line`, `interaction_score`,
  `guide_a`, `guide_b`, `n_counts`, `n_genes`
- **Note:** contains genetic interaction scores — store in `obs["interaction_score"]`

#### Arc VCC Perturbations (virtualcellchallenge.org)
- **Source:** `https://virtualcellchallenge.org/datasets`
- **Format:** likely `.h5ad` or `.zarr`; format TBC on the challenge website
- **Perturbation type:** genetic (CRISPR) — TBC
- **Note:** format and access method require checking the challenge portal directly

---

### Bulk / Screen datasets

#### Broad PRISM Repurposing
- **Source:** `https://depmap.org/repurposing/`
- **Format:** CSV — long format with columns `depmap_id`, `cell_line`, `compound`,
  `dependency` (log fold change vs DMSO) + compound metadata (MOA, target, disease)
- **Scale:** ~4 686 compounds × ~578 cell lines
- **Conversion:** pivot to AnnData: `obs` = cell lines, `var` = compounds,
  `X` = log fold change (viability); compound metadata → `var` columns

#### Sanger GDSC (GDSC1 + GDSC2)
- **Source:** `https://www.cancerrxgene.org/downloads/bulk_download`
- **Format:** CSV fitted dose-response — columns: `SANGER_MODEL_ID`, `DRUG_NAME`,
  `LN_IC50`, `AUC`, `RMSE`, `DATASET` + cell-line metadata
- **Conversion:** pivot to AnnData: `obs` = cell lines, `var` = drugs, `X` = `LN_IC50`
- **Note:** two separate screens (GDSC1 and GDSC2) with different drug sets

#### Sanger SCORE CRISPR KO (Cell Model Passports)
- **Source:** `https://cellmodelpassports.sanger.ac.uk/downloads`
- **Format:** CSV gene-effect matrix — rows = cell lines, columns = genes;
  values = Bayes Factor or log fold change (CRISPRcleanR-corrected)
- **Scale:** 323+ cancer cell lines × ~18 000 genes
- **Conversion:** AnnData `obs` = cell lines, `var` = genes, `X` = gene effect score

#### DepMap CCLE
- **Source:** `https://depmap.org/portal/download/` (latest public release, e.g. 25Q2)
- **Format:** CSV expression matrix — `depmap_id` × gene; values = log₂(TPM + 1)
  Additional files: proteomics (MS), mutations (MAF), copy number
- **Conversion:** AnnData `obs` = cell lines, `var` = genes, `X` = RNA expression;
  proteomics stored in `obsm["proteomics"]`

#### Sanger Drug Combinations
- **Source:** `https://gdsc-combinations.depmap.sanger.ac.uk/`
- **Format:** combination synergy scores — CSV with `cell_line`, `drug_a`, `drug_b`,
  `bliss_synergy`, `hsa_synergy`, `zip_synergy`
- **Conversion:** multi-layer AnnData or store raw CSV as sidecar artifact

---

## missing datasets to add

- xaira/orion https://www.biorxiv.org/content/10.1101/2025.06.11.659105v1.ful.
  https://plus.figshare.com/articles/dataset/Processed_data_for_X-Atlas_Orion_Genome-wide_Perturb-seq_Datasets_via_a_Scalable_Fix-Cryopreserve_Platform_for_Training_Dose-Dependent_Biological_Foundation_Models/29190726
- T-cell gwps:
  https://virtualcellmodels.cziscience.com/dataset/genome-scale-tcell-perturb-seq
  GSE314342
- viperturb-seq's dataset.
  https://www.biorxiv.org/content/10.64898/2026.02.12.705613v1.full
  https://zenodo.org/records/18460279
- proper-seq's dataset:
  https://www.cell.com/molecular-cell/fulltext/S1097-2765(21)00574-8 GSE150818

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
