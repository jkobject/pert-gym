# pert-gym — AI Agent Guide

## Project Overview

`pert-gym` is a benchmarking framework for perturbation response prediction. It
connects to the `laminlabs/pertdata` LaminDB instance which stores single-cell
perturbation datasets. All data lives in a **triplet format** (obs.parquet /
X.h5ad / var.parquet).

### Key entry points

- `notebooks/alignment_and_merging.ipynb` — main dataset curation and migration
  notebook
- `tools/convert_triplet_artifacts.py` — migrate `.h5ad` → triplet (obs/X/var)
- `tools/ingest_xatlas_orion.py` — specialized ingestion for large XAtlas/Orion
  files
- `tools/preprocess_collection.py` — plan/discover preprocessing across all
  artifacts
- `data/README.md` — dataset catalogue, format spec, list of missing datasets
- `src/pert_gym/` — Python package (models, metrics, CLI)

### Lamin instance

```python
import lamindb as ln
ln.connect("laminlabs/pertdata")
```

Always call `ln.track()` at the top of notebooks before any artifact operations
to get run provenance.

### Triplet format

Every dataset is stored as three linked artifacts:

```
<dataset_prefix>/obs.parquet   ← cell metadata (obs)
<dataset_prefix>/X.h5ad        ← expression matrix (X only, empty obs/var)
<dataset_prefix>/var.parquet   ← gene metadata (var)
```

Links: `obs → X → var` via `artifact.features.set_values({"X": x_artifact})`
etc.

Loading pattern:

```python
obs_artifact = ln.Artifact.get(key=f"{prefix}/obs.parquet")
x_artifact   = obs_artifact.features.get_values()["X"]
var_artifact = x_artifact.features.get_values()["var"]
adata = x_artifact.load()
adata.obs = obs_artifact.load()
adata.var = var_artifact.load()
```

---

## Roadmap / Plan

### Phase 1 — Dataset migration (unprocessed → triplet)

These datasets still exist as legacy `.h5ad` artifacts and need
`migrate_h5ad_to_triplet()`:

- [x] `XieHon2017` (`scperturb/records/.../XieHon2017.h5ad`) — GSE81884
- [x] `PapalexiSatija2021` (4 files: eccite_arrayed_RNA, eccite_arrayed_protein,
      eccite_protein, eccite_RNA) — merge RNA modalities, protein values into
      obs, then migrate
- [x] `GehringPachter2019` — first migration succeeded; the **second call** in
      the notebook errors because the source artifact was deleted by the first
      call — remove duplicate call
- [x] `SchiebingerLander2019` (2 files: GSE115943 + GSE106340) — concat then
      migrate
- [x] `GasperiniShendure2019` (3 files: atscale, highMOI, lowMOI) — concat then
      migrate
- [x] `Parse_10M_PBMC_cytokines` — migrate directly

### Phase 2 — Dataset understanding & annotation audit

For each dataset group listed in the notebook (`others`, `srivatsan`, `gwps`,
`adamson`, `nadig`, `dixit`, `lincs`, `tahoe`, `GSE305979`, `GSE306429`):

- [ ] Load obs and inspect all columns — flag missing / inconsistent obs fields
      (`perturbation`, `perturbation_type`, `cell_line`, `organism`,
      `tissue_type`, etc.)
- [ ] Identify which scperturb/records bundle datasets are already migrated
      (triplet exists) and skip them; flag any that still need migration
- [ ] Document per-dataset notes in the notebook (cell counts, modality,
      perturbation library, known issues)
- [ ] `parse10m` — redo ingestion to match the same obs schema as other datasets
- [ ] `mcfarland20` — add gene effect (DepMap) and other extra features
- [ ] `GSE305979` — understand why day0-7 normalized has fewer cells than raw
      counts
- [ ] `GSE306429` — vscores vs demuxed: check if they need to be merged or kept
      separate
- [ ] LINCS — add Level 4 data on top of Level 2 (see notebook comments)

### Phase 3 — Missing datasets ingestion

Datasets not yet in the Lamin instance (from `data/README.md`):

**Preparation (done):**

- [x] `data/README.md` — dataset catalogue documented with sources, formats,
      accessions
- [x] `notebooks/phase3_ingestion.ipynb` — thin wrapper notebook (imports +
      commented run cells)
- [x] `tools/ingest_phase3_scrna.py` — all scRNA-seq ingestion functions
- [x] `tools/ingest_phase3_bulk.py` — all bulk/sensitivity ingestion functions
      (hybrid obs model)

Priority (genomic perturbations, scRNA-seq):

- [ ] **XAtlas/Orion** — `ingest_xatlas_orion.py` already written; run pipeline
      for both HCT116 and HEK293T files (Figshare article 29190726)
- [ ] **PRISM** — ~36 datasets (GSE217812, GSE90063, GSE250378, GSE261283, etc.)
      h5ads available at the linked Google Drive. the exact list of datasets to
      use is in the data/README file.
- [ ] **T-cell GWPS** — GSE314342
      (https://virtualcellmodels.cziscience.com/dataset/genome-scale-tcell-perturb-seq)
- [ ] **VIPerturbSeq** — zenodo.org/records/18460279
- [ ] **PROPER-seq** — GSE150818
- [ ] **Sanger dual-guide KO in CRC** — figshare 25533091
- [ ] **Arc VCC perturbations** — virtualcellchallenge.org/datasets

Bulk/screen datasets (sensitivity / gene effect):

- [ ] **Broad PRISM repurposing** — depmap.org/repurposing
- [ ] **Sanger GDSC** — cancerrxgene.org
- [ ] **Sanger SCORE CRISPR KO** — cellmodelpassports.sanger.ac.uk
- [ ] **Sanger drug combinations** — gdsc-combinations.depmap.sanger.ac.uk
- [ ] **DepMap CCLE** — depmap.org/portal/download
- [ ] **RxRx datasets** (rxrx1/2/3/19a/19b) — rxrx.ai — image-based

Overlap checks before ingestion:

- [ ] Check overlap between PRISM and scperturb (avoid double-ingestion)
- [ ] Check overlap of JUMP and RxRx datasets
- [ ] Check horlbeck, saunders, shifrut — if not already present

### Phase 4 — obs feature enrichment (per-cell annotations)

For every dataset in the instance, compute and store the following in obs/obsm:

- [ ] **Quality metrics** — `obs["pct_mito"]`, `obs["pct_ribo"]`,
      `obs["n_genes"]`, `obs["n_counts"]`, `obs["is_low_quality"]` flag.
- [ ] **Missing metadata** — `obs["sex"]`, `obs["ethnicity"]`,
      `obs["pert_library"]` (CRISPRa/i/KO, drug, etc.) where not already
      present.
- [ ] **Differential expression** — precompute per-perturbation DE results and
      store as parquet sidecar (`<prefix>/de.parquet`).
- [ ] **`obsm["X_embedding"]`** — cell embedding (scVI, scGPT, or similar
      foundation model). Use scVI trained per-dataset or a pretrained universal
      model.
- [ ] **`obs["cell_state"]`** — coarse cell state label (e.g. from clustering on
      the embedding, or propagated from author annotations).
- [ ] **`obs["sensitivity"]`** — perturbation sensitivity score per cell.
      Defined as distance of perturbed cell from control centroid in embedding
      space, or from a viability/proliferation signal if available.

### Phase 5 — Baseline models

Implement and benchmark the following perturbation response models in
`src/pert_gym/models/`:

#### 5.1 Trivial baselines (MIN / MAX reference points)

- [ ] **Mean control** — predict the mean expression of unperturbed (control)
      cells. The floor: no perturbation signal whatsoever.
- [ ] **Mean perturbation** — predict the mean expression across **all**
      perturbations. Represents the "average drug effect".
- [ ] **Binary split** — split perturbations into 2 groups by mean effect
      magnitude (strong vs weak); predict group mean. Tests whether a single
      discriminator adds value.

#### 5.2 Classical regression / classification

- [ ] **Linear regression (per-gene)** — one linear model per gene, features =
      perturbation one-hot + cell state covariates.
- [ ] **Ridge / ElasticNet** — regularised versions of the above.
- [ ] **Random forest / gradient boosting** — non-linear baselines.
- [ ] **Logistic classifier (cell-state prediction)** — classify cell state
      given perturbation.

#### 5.3 Latent perturbation models

- [ ] **LPM** (Latent Perturbation Model) — simple VAE with perturbation latent
      shift.
- [ ] **CPA** (Compositional Perturbation Autoencoder) — `scvi-tools` or
      standalone.
- [ ] **chemCPA** — chemical CPA with drug fingerprint conditioning.
- [ ] **trVAE** — transfer VAE for perturbation response.
- [ ] **scPRAM** — single-cell perturbation response via attention mechanism.

#### 5.4 Evaluation

- [x] Implement standard metrics in `src/pert_gym/metrics.py`: R², Pearson
      correlation, MSE on held-out perturbations (mean across genes, top 20 DE
      genes, top 100 DE genes).
- [x] Implement train/val/test split by perturbation identity (not cell).
- [ ] Create evaluation harness: `src/pert_gym/evaluate.py`.

---

## Coding conventions

- Python 3.11, managed with `uv` (see `Makefile`).
- `lamindb==2.2.0` — do not upgrade without testing.
- All notebook cells must call `ln.track()` before artifact operations.
- New tools go in `tools/`, new source code in `src/pert_gym/`.
- Prefer editing existing files over creating new ones.
- No auto-commit; ask before git push.

## Running the environment

```bash
source .venv/bin/activate
# or:
uv run jupyter notebook
```
