# T29F row113 GSE216481 TF atlas staged/range probe — 2026-06-22

Scope: source/component/range inspection only; no Lamin writes and no full matrix loads.

## Source/staging
- GEO RAW tar: `https://ftp.ncbi.nlm.nih.gov/geo/series/GSE216nnn/GSE216481/suppl/GSE216481_RAW.tar`
- Expected size: `17908162560` bytes.
- GCS staging target: `gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/GSE216481_RAW.tar` — verified complete at 17,908,162,560 bytes; GCS md5 `F/KU8vFWRdT8CqJSggeYnQ==`, crc32c `4qSU/w==`.
- Filelist saved at `artifacts/schema_audit/temporal_t29_gse216481_filelist_20260622.txt`.
- Filelist members: 167 files, payload bytes 17,908,032,769; computed tar coverage 17,908,156,413 bytes with 6,147 bytes slack/end padding vs HEAD size.

## Components from filelist
- `180124_perturb`: 24 files, 703,045,373 bytes, types={'TSV': 16, 'MTX': 8}; PerturBase id=98 modality=RNA qc=Failed filtered=0×0 perts=0
- `201218_RNA`: 16 files, 1,706,127,653 bytes, types={'CSV': 16}; PerturBase id=1 modality=RNA qc=Pass filtered=56857×36844 perts=139
- `201218_TFmap`: 4 files, 21,826,932 bytes, types={'CSV': 4}
- `210322_TFAtlas`: 20 files, 12,549,485,393 bytes, types={'CSV': 20}; PerturBase id=32 modality=RNA qc=Pass filtered=527594×16873 perts=1183
- `210322_TFmap`: 24 files, 172,745,005 bytes, types={'CSV': 24}
- `210715_combinatorial`: 9 files, 1,562,113,286 bytes, types={'CSV': 9}; PerturBase id=99 modality=RNA qc=Failed filtered=0×0 perts=0
- `other`: 70 files, 1,192,689,127 bytes, types={'TSV': 44, 'MTX': 22, 'CSV': 4}

## RNA expression header probe
- `201218_RNA`: 16 dense gene×cell CSV.gz files; header cell columns sum to 2,608,231; PerturBase filtered cells 56,857, genes 36,844, perturbations 139.
  - `GSM6706657_201218_RNA_D4_S1.csv.gz`: 103,395 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.27', 'R1.01,R2.01,R3.02,P1.27', 'R1.01,R2.01,R3.03,P1.27']
  - `GSM6706658_201218_RNA_D4_S2.csv.gz`: 89,627 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.28', 'R1.01,R2.01,R3.02,P1.28', 'R1.01,R2.01,R3.04,P1.28']
  - `GSM6706659_201218_RNA_D4_S3.csv.gz`: 75,320 encoded cell columns; examples ['R1.01,R2.01,R3.07,P1.29', 'R1.01,R2.01,R3.10,P1.29', 'R1.01,R2.01,R3.36,P1.29']
  - `GSM6706660_201218_RNA_D4_S4.csv.gz`: 126,980 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.30', 'R1.01,R2.01,R3.02,P1.30', 'R1.01,R2.01,R3.04,P1.30']
  - `GSM6706661_201218_RNA_D4_S5.csv.gz`: 249,267 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.65', 'R1.01,R2.01,R3.02,P1.65', 'R1.01,R2.01,R3.03,P1.65']
  - `GSM6706662_201218_RNA_D4_S6.csv.gz`: 265,619 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.66', 'R1.01,R2.01,R3.02,P1.66', 'R1.01,R2.01,R3.03,P1.66']
  - ... 10 more samples in JSON.
- `210322_TFAtlas`: 20 dense gene×cell CSV.gz files; header cell columns sum to 16,012,964; PerturBase filtered cells 527,594, genes 16,873, perturbations 1,183.
  - `GSM6719950_210322_TFAtlas_S05.csv.gz`: 849,424 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.54', 'R1.01,R2.01,R3.02,P1.54', 'R1.01,R2.01,R3.03,P1.54']
  - `GSM6719951_210322_TFAtlas_S06.csv.gz`: 818,182 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.62', 'R1.01,R2.01,R3.02,P1.62', 'R1.01,R2.01,R3.03,P1.62']
  - `GSM6719952_210322_TFAtlas_S07.csv.gz`: 856,110 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.07', 'R1.01,R2.01,R3.02,P1.07', 'R1.01,R2.01,R3.03,P1.07']
  - `GSM6719953_210322_TFAtlas_S08.csv.gz`: 835,134 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.15', 'R1.01,R2.01,R3.02,P1.15', 'R1.01,R2.01,R3.03,P1.15']
  - `GSM6719954_210322_TFAtlas_S09.csv.gz`: 837,674 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.23', 'R1.01,R2.01,R3.02,P1.23', 'R1.01,R2.01,R3.03,P1.23']
  - `GSM6719955_210322_TFAtlas_S10.csv.gz`: 836,406 encoded cell columns; examples ['R1.01,R2.01,R3.01,P1.31', 'R1.01,R2.01,R3.02,P1.31', 'R1.01,R2.01,R3.03,P1.31']
  - ... 14 more samples in JSON.

## TFmap probe
- `201218_TFmap`: 4 small CSV.gz files, 2,354,994 rows total.
  - `GSM6706673_201218_TFmap_S1.csv.gz`: 612,008 rows; description='TF mapping results for 201218_ATAC_D4_S1-S4 and 201218_RNA_D4_S1-S4'; first row=['R1.54,R2.54,R3.53', 'TCACAGAAGTCACGGAGGTCAGGT', '464']
  - `GSM6706674_201218_TFmap_S2.csv.gz`: 597,965 rows; description='TF mapping results for 201218_ATAC_D4_S5-S8 and 201218_RNA_D4_S5-S8'; first row=['R1.93,R2.92,R3.58', 'TGCTGGGAGAAAGCTCCAAATGGT', '11']
  - `GSM6706675_201218_TFmap_S3.csv.gz`: 587,006 rows; description='TF mapping results for 201218_ATAC_D7_S1-S4 and 201218_RNA_D7_S1-S4'; first row=['R1.87,R2.84,R3.09', 'TTCATGAGAGAAACACCTCCCGCG', '41']
  - `GSM6706676_201218_TFmap_S4.csv.gz`: 558,015 rows; description='TF mapping results for 201218_ATAC_D7_S5-S8 and 201218_RNA_D7_S5-S8'; first row=['R1.27,R2.02,R3.55', 'AGGAAACTGAACGCAAATGATTCG', '2365']
- `210322_TFmap`: 24 small CSV.gz files, 14,156,130 rows total.
  - `GSM6719970_210322_TFmap_S01.csv.gz`: 512,818 rows; description='library strategy: amplicon'; first row=['R1.40,R2.55,R3.89', 'AGTGGTCGGAGGTACCGTCAAGCT', '505']
  - `GSM6719971_210322_TFmap_S02.csv.gz`: 499,901 rows; description='library strategy: amplicon'; first row=['R1.49,R2.57,R3.93', 'AACAAATTATGCGCTTCCACCAGG', '13']
  - `GSM6719972_210322_TFmap_S03.csv.gz`: 448,795 rows; description='library strategy: amplicon'; first row=['R1.66,R2.43,R3.52', 'AAATCTAAAGAAAGCTAATGGTCT', '214']
  - `GSM6719973_210322_TFmap_S04.csv.gz`: 520,366 rows; description='library strategy: amplicon'; first row=['R1.35,R2.31,R3.22', 'AGCAGGAGTGAGGCTAGTACATGA', '726']
  - ... 20 more TFmap files in JSON.

## Duplicate probe
- No Lamin key hits in first 100 results for GSE216481 / PRJNA893678 / component-prefix terms on `laminlabs/pertdata` branch `jkobject`.

## Decision
- RNA components are identifiable: QC-pass `201218_RNA` and large QC-pass `210322_TFAtlas`.
- Exclude ATAC and failed/combinatorial components from canonical RNA `X.h5ad`; ATAC would need a typed auxiliary modality contract if ever represented.
- No canonical Lamin write was performed. The blocker is perturbation-label safety, not source size: expression cell IDs are encoded (`R1.*,R2.*,R3.*,P1.*`) and TFmap files expose encoded barcode/sequence/numeric rows, but no verified barcode/ORF-to-TF-symbol library map was found in the GEO/PerturBase metadata probed here. Writing `perturbation` now would risk wrong labels.
- A future converter should first resolve that library map or obtain the PerturBase filtered object/metadata contract, then ingest in sample/component chunks with dense-CSV streaming/transposition; do not full-load the 527k-cell TFAtlas component.
