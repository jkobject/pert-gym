# pert-gym — AI Agent Guide

## Project Overview

`pert-gym` is a benchmarking framework for perturbation and temporal response
prediction. Treat perturbations, time evolution, dose response, genetic
dependency, microscopy phenotypes, protein measurements, and molecular profiles
as different views of the same question: how does a biological system move away
from a baseline state, and can a model predict that movement?

The project connects to the `laminlabs/pertdata` LaminDB instance and stores
datasets in a **triplet format** (obs.parquet / X.h5ad / var.parquet). The first
ingestion target is a broad catalogue of known perturbation datasets, harmonized
enough that scRNA-seq, bulk sensitivity, proteomics, and imaging-derived
phenotypes can all expose a depmap-like target such as cell survival,
dependency, or state displacement.

### Key entry points

- `notebooks/alignment_and_merging.ipynb` — main dataset curation and migration
  notebook
- `tools/convert_triplet_artifacts.py` — migrate `.h5ad` → triplet (obs/X/var)
- `tools/ingest_xatlas_orion.py` — specialized ingestion for large XAtlas/Orion
  files
- `tools/preprocess_collection.py` — plan/discover preprocessing across all
  artifacts
- `tools/plan_phase3_ingestion.py` — local-only manifest/dry-run for missing
  dataset families; safe to run before Lamin writes
- `data/README.md` — dataset catalogue, format spec, list of missing datasets
- `src/pert_gym/` — Python package (models, metrics, CLI)

### Lamin instance

```python
from tools.lamin_context import connect_pertdata

ln = connect_pertdata()
```

Always call `ln.track()` at the top of notebooks before any artifact operations
to get run provenance. Do not use the global `lamin` CLI for this project: other
local sessions may keep it pointed at `jkobject/jouvencekb`. `connect_pertdata()`
uses the Python API, connects explicitly to `laminlabs/pertdata`, and pins the
writable Lamin branch:

```python
assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
assert ln.setup.settings.branch.name == "jkobject"
```

If another process may be using LaminDB, do local planning/download/inspection
first and only run artifact writes in a short, explicit batch.

`connect_pertdata()` also redirects Lamin's local cache to `.lamin-cache/` in
this repo so large temporary artifact files do not fill the shared `/data`
mount. After verifying a triplet in Lamin, clear cached payload copies:

```bash
uv run python tools/clean_lamin_cache.py
```

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

### Large local files / GCS staging

Disk on the VPS is limited. Large raw downloads (`.h5ad`, `.tar`, `.rds`,
zips, extracted raw matrices) should be treated as temporary cache only:

```bash
python3 tools/stage_to_gcs.py <paths...> \
  --bucket scperturb \
  --prefix pert-gym/staging \
  --delete-local
```

This stages files to `gs://scperturb/pert-gym/staging/...`, verifies the object,
then deletes the local copy only after upload succeeds. Keep `data/main/`,
`data/source_cache/`, `data/temporal_pretraining_sources/`, local Lamin caches,
virtualenvs, and generated benchmark artifacts out of Git; durable ingested data
should live in LaminDB, and pre-ingestion raw files should live in the GCS
staging bucket.

### Git / review workflow

Canonical repo strategy as of 2026-06-22: `pert-gym` is a standalone GitHub repo
at `https://github.com/jkobject/pert-gym.git`, not a subdirectory of the broader
OpenClaw workspace repo. The active shared checkout is:

```text
/Users/jkobject/.openclaw/workspace/work/pert-gym
```

The shared checkout is acceptable for read-only inspection, cache/materialized
data, and emergency ops. Implementation/model-code Kanban cards should use
isolated worktrees instead of piling edits into the shared checkout:

```bash
cd /Users/jkobject/.openclaw/workspace/work/pert-gym
git fetch origin main
git worktree add -b fix/t_12345678-loader-contract \
  /Users/jkobject/.openclaw/worktrees/pert-gym/t_12345678 origin/main
```

Rules:

- Before editing, `git rev-parse --show-toplevel` must resolve to the pert-gym
  checkout/worktree, not `/Users/jkobject/.openclaw/workspace`.
- Branches should include the Kanban task id, for example
  `ops/t_51be75dd-restore-git-workflow`.
- Commit only reviewable code, docs, tests, config, and artifact manifests.
  Exclude raw data, `data/source_cache/`, `data/temporal_pretraining_sources/`,
  Lamin caches, virtualenvs, `.omx/`, generated benchmark blobs, and local
  model-ready `.h5ad` exports unless a task explicitly asks for a tiny fixture.
- Open a PR for implementation/model-code changes before marking code work done.
  If a worker cannot safely create the PR, leave a review-required Kanban block
  with branch/path, changed files, tests, and remaining risk.
- If Git metadata is broken again, stop. Restore standalone repo metadata from
  `origin/main` without overwriting the working tree, and record the forensic
  backup path.

---

## Roadmap / Plan

### Phase 0 — Shared task framing

- [ ] Define the canonical prediction task family:
      baseline state + perturbation/evolution descriptor -> response state.
- [ ] Define cross-modality response targets:
      `sensitivity`, `cell_survival`, `state_displacement`, `growth_effect`,
      `gene_effect`, `drug_effect`, and `phenotype_embedding`.
- [ ] Define the obs schema needed for all triplets:
      `perturbation`, `perturbation_type`, `is_control`, `cell_line`,
      `cell_type`, `organism`, `disease`, `tissue_type`, `timepoint`,
      `dose`, `modality`, `assay`, `sensitivity`, `response_metric`.
- [ ] Specify loss families to benchmark beyond standard expression metrics:
      response-vector loss, delta-from-control loss, sensitivity/ranking loss,
      distributional state matching, and bias-aware stratified losses.

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
- [ ] Write a per-dataset schema manifest with required/missing obs fields and
      whether `sensitivity` can be computed directly or must be inferred.

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
- [x] `tools/plan_phase3_ingestion.py` — local ingestion manifest, no Lamin writes

Priority (genomic perturbations, scRNA-seq):

- [ ] **XAtlas/Orion** — `ingest_xatlas_orion.py` already written; run pipeline
      for both HCT116 and HEK293T files (Figshare article 29190726)
- [ ] **PRISM** — ~36 datasets (GSE217812, GSE90063, GSE250378, GSE261283, etc.)
      h5ads available at the linked Google Drive. the exact list of datasets to
      use is in the data/README file.
      - [x] Drive folder listed: 77 `.h5ad` files available.
      - [x] `GSE217812` downloaded, standardized, ingested, verified as triplet
            at `prism_collection/GSE217812`.
      - [x] `GSE90063_mouse` downloaded, standardized, ingested, verified as
            triplet at `prism_collection/GSE90063_mouse`.
      - [x] First automated PRISM pass completed over all 77 Drive h5ads:
            18 compatible/moderate h5ads ingested as triplets, 6 large files
            staged to `gs://scperturb/pert-gym/staging`, 53 Google Drive
            quota/link failures recorded for retry in
            `artifacts/phase3_ingestion_progress.json`.
- [ ] **T-cell GWPS** — GSE314342
      (https://virtualcellmodels.cziscience.com/dataset/genome-scale-tcell-perturb-seq)
- [ ] **VIPerturbSeq** — zenodo.org/records/18460279
      - [x] Zenodo metadata inspected; files are `.rds` chunks (1.6-3.8 GB)
            plus a small manifest. Manifest staged to GCS; ingestion needs an
            R/Seurat-to-h5ad conversion path, not the old h5ad downloader.
- [ ] **PROPER-seq** — GSE150818
      - [x] GEO raw tar downloaded and inspected.
      - [ ] Find processed expression matrix source or implement an interaction
            sidecar; GEO raw tar contains `chimericReadPairs.csv.gz`, not a
            direct scRNA expression h5ad/10x matrix.
- [ ] **Sanger dual-guide KO in CRC** — figshare 25533091
      - [x] Figshare API inspected: `MAPPING.zip` is 1.15 GB. Treat as a
            larger/raw mapping dataset; inspect layout before attempting triplet
            conversion.
- [ ] **Arc VCC perturbations** — virtualcellchallenge.org/datasets

Bulk/screen datasets (sensitivity / gene effect):

- [x] **Broad PRISM repurposing** — depmap.org/repurposing
      - Ingested at `broad_prism_repurposing` with `obs["lfc"]`; source CSVs
        staged to GCS. `X` is currently empty pending CCLE baseline join.
- [x] **Sanger GDSC** — cancerrxgene.org
      - Ingested GDSC1 and GDSC2 at `sanger_gdsc/gdsc1` and
        `sanger_gdsc/gdsc2`; source Excel files staged to GCS. `X` is
        currently empty pending CMP/CCLE baseline expression join.
- [x] **Sanger SCORE CRISPR KO** — cellmodelpassports.sanger.ac.uk
      - Ingested at `sanger_score_crispr` using the SCORE2 fold-change matrix;
        source zip staged to GCS.
- [ ] **Sanger drug combinations** — gdsc-combinations.depmap.sanger.ac.uk
- [x] **DepMap CCLE** — depmap.org/portal/download
      - Ingested DepMap Public 26Q1 expression at `depmap_ccle/26q1`;
        expression CSV staged to GCS. Proteomics filename changed in 26Q1 and
        still needs resolution.
- [ ] **RxRx datasets** (rxrx1/2/3/19a/19b) — rxrx.ai — image-based

Ingestion order:

1. Run `tools/plan_phase3_ingestion.py --json artifacts/phase3_ingestion_manifest.json`
   to check local files and entrypoints without touching Lamin.
2. Validate Lamin connectivity and active branch.
3. Ingest one small/moderate dataset end-to-end, inspect triplets, then batch.
4. Do large datasets (Orion, T-cell GWPS, RxRx) only after disk space and
   chunking strategy are confirmed.

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
