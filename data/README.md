# Data

- `main/`: primary datasets used by the project.
- `others/`: auxiliary or experimental datasets, will be ignored by default in
  the project code.

## format of files

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

## missing projects to add

- depmap off target analysis given the CRISPR library used
- broad's prism
- PRISM set of datasets https://www.biorxiv.org/content/10.64898/2025.12.23.696273v1
- xaira/orion
- T-cell gwps: https://virtualcellmodels.cziscience.com/dataset/genome-scale-tcell-perturb-seq
- sanger's gwps in iPSCs: https://www.nature.com/articles/s41467-025-67256-9#Abs1
- arc's perturbations for the Arc VCC
- dual KO in CRC: https://www.nature.com/articles/s41467-025-67256-9#Abs1
- viperturb-seq's dataset
- proper-seq's dataset
- recursion's rxrx
- depmap's CCLE
- sanger's SCORE
- sanger's gdsc
- sanger's gdsc tdr or something
- pdx data broad (met500)

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

