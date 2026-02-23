# Data

- `main/`: primary datasets used by the project.
- `others/`: auxiliary or experimental datasets, will be ignored by default in
  the project code.

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

## missing datasets to add

- xaira/orion https://www.biorxiv.org/content/10.1101/2025.06.11.659105v1.ful.
  https://plus.figshare.com/articles/dataset/Processed_data_for_X-Atlas_Orion_Genome-wide_Perturb-seq_Datasets_via_a_Scalable_Fix-Cryopreserve_Platform_for_Training_Dose-Dependent_Biological_Foundation_Models/29190726
- sanger's dual guid KO in CRC:
  https://www.nature.com/articles/s41467-025-67256-9#Abs1
  https://figshare.com/articles/dataset/MAPPING_zip/25533091/1?file=45433417
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
