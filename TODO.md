# pert-gym TODO / Kanban mirror

_Last updated: 2026-06-22 23:55 CEST from Hermes Kanban board `pert-gym`._

This file is a compact, human-readable mirror of the high-level Kanban/project
state. It is intentionally not a full event log. For exact counts, validation
commands, and artifact paths, use the canonical status documents linked below.

## Documentation map

- `README.md` — human project overview: what pert-gym is, how to install/use it,
  and where to start reading.
- `CLAUDE.md` — agent operating guide: Lamin rules, safety constraints,
  current source-of-truth decisions, and execution order.
- `TODO.md` — this high-level Kanban mirror: phases, blockers, done milestones,
  and next cards.
- `data/README.md` — dataset catalogue and data-layout notes, including phase 3
  source notes and temporal-pretraining catalogue pointers.
- `docs/` — human-facing docs and project details:
  - `docs/pert_gym_schema.md` — canonical schema / unified Collection contract.
  - `docs/model_environments.md` — isolated model environment policy.
  - `docs/cpa_baseline.md`, `docs/lpm_baseline.md` — benchmark-specific notes.
  - scaffold/user docs: `docs/index.md`, `docs/getting-started.md`,
    `docs/usage.md`, `docs/cli.md`, `docs/api.md`, `docs/configuration.md`,
    `docs/development.md`, `docs/structure.md`.
- `wiki/pert-gym/` — detailed internal/project knowledge:
  - `wiki/pert-gym/current-status.md` — canonical latest status, count vocabulary,
    validation commands, temporal/model benchmark summaries, and blockers.
  - `wiki/pert-gym/index.md` — wiki table of contents.
  - `wiki/pert-gym/schema-contract.md` — agent-readable schema summary.
  - `wiki/pert-gym/harmonization-roadmap.md` — phase roadmap.
  - `wiki/pert-gym/lamin-audit-and-branch-model.md` — branch/counting model.
  - `wiki/pert-gym/dataset-modalities.md` — modality decisions.
  - `wiki/pert-gym/deduplication-policy.md` — duplicate/subduplicate gate.

## Current source-of-truth status

Use `wiki/pert-gym/current-status.md` for detailed numbers. Short version:

- Lamin instance: `laminlabs/pertdata`, working branch `jkobject`.
- Query/write rule: use `tools.lamin_context.connect_pertdata()`; do not rely on
  the global Lamin CLI state.
- Real P3 is complete at the triplet-integrity/query-surface level via the dated
  unified Collection family:
  - `pert-gym/base-public/20260621` — 60 members.
  - `pert-gym/additions/20260621` — 996 members.
  - `pert-gym/canonical/20260621` — 1056 canonical obs-triplet members.
  - `pert-gym/model-ready/20260621` — first reviewed v0 subset, currently 1
    tiny loader-smoked member.
- Count vocabulary matters: do not conflate latest visible Lamin artifacts,
  canonical collection members, logical dataset families, triplet prefixes,
  chunks, and model-ready members.
- `triplet-integrity-ok` means the collection/query contract is validated; it is
  not the same as fully curated or model-ready training data.
- P7S final scoped audit is current as of 2026-06-22:
  `artifacts/schema_audit/final_audit_p7s_20260622.md` plus lightweight evidence
  JSON. It supersedes stale old P7 dependency wording but does not mark deferred
  blockers complete.

## Repository / PR workflow status

- 2026-06-22 OPS2: canonical Git strategy restored/documented. `pert-gym` is a standalone repo at `https://github.com/jkobject/pert-gym.git`; the Mac shared checkout has local caches/data and should not be used as the default place for implementation edits.
- Future implementation/model-code Kanban cards should use task branches and isolated worktrees under `/Users/jkobject/.openclaw/worktrees/pert-gym/<task-id>`, then open PRs for reviewable changes.
- Keep raw data, `data/source_cache/`, `data/temporal_pretraining_sources/`, Lamin caches, virtualenvs, `.omx/`, generated artifacts, and local model-ready `.h5ad` exports out of Git unless a task explicitly asks for a tiny fixture or manifest.

## High-level phase mirror

### Core harmonization and PRISM path

Done:

- P1A/P1B — repaired true urgent triplets:
  - `cellarity/GSE305979/GSE305979_day1-7_raw_counts`.
  - `scperturb/replogle22_RPE1`.
- P2 — enforced same-prefix `var.parquet` for LINCS phase1 delta and Tahoe
  plates 1–14.
- P3 metadata-only gate — completed, then superseded by the real-P3 replacement
  graph below.
- P4A — classified auxiliary modality/orphan payloads and documented target
  naming (`X_<name>/var_<name>` or `obsm_<name>`), with unsafe rewrites deferred.
- P4B — non-destructively archived the mistaken legacy GSE150818/properseq
  active-branch triplet; PRoPER-seq remains source-gated.
- P5A — built PRISM duplicate/subduplicate gate against visible public/main plus
  `jkobject` artifacts.
- P5B/P5B-tail/P5D/P5E — completed accessible PRISM ingestion/residual cleanup;
  the four P5A-cleared candidates are represented, including GSE225775 as
  272/272 verified chunks. Late 2026-06-22 manual/Chrome Google Drive recovery
  staged the residual PRISM h5ads under
  `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`.
  P5F continuation on PR #12 has now verified 4 recovered datasets:
  `GSE255832` (27,912 obs / 28 chunks), `GSE263524` (42,289 / 43),
  `GSE236057` (15,866 / 16), and duplicate-resolved `GSE267982` (45,808 / 46).
  Duplicate-named P5F staged pairs were compared by streaming SHA-256:
  `GSE247274` and `GSE267982` canonical/`(1)` objects are byte-identical; ingest
  canonical objects only and leave redundant GCS copies untouched unless a cleanup
  card explicitly deletes them. Remaining P5F state is in
  `artifacts/schema_audit/prism_p5f_status_20260622.md`: ~27 accessible
  smoke-first candidates remain, 5 rows are still missing/source-blocked, and
  `GSE90063_human-004` remains user-excluded.
- OPS0 — Mac mini path fixed: use native GCS access plus repo-local
  `data/gcs_cache/`; do not assume `/mnt/gcs/scperturb` or Homebrew gcsfuse.

Superseded/guarded:

- P5C (`t_74c9afa3`) is intentionally blocked as a stale old-workspace guard.
  Do not unblock it unless the board is rewired; fresh Mac replacement cards are
  the live path.
- Old VPS P6/P7 cards (`t_2de8b410`, `t_ce55ba6c`, `t_a3d63dd9`) are stale
  todo descendants of the blocked P5C guard. Prefer the Mac replacement cards.

### Real-P3 replacement graph

Done:

- P3R-A — inventoried existing `ln.Collection` records; no existing collection
  contained the full current global artifact universe.
- P3R-B — documented unified Collection/query contract.
- P3R-C — built dated `base-public`, `additions`, and `canonical` Collections.
- P3R-D — implemented query helpers and validation.
- P3R-E — created notebook/smoke exploration of the unified collection.
- P3R-F — final review accepted real P3 at triplet-integrity/query-surface level.

Canonical follow-up after P3R:

- Define/promote richer reviewed model-ready subsets beyond the first tiny v0.
- Continue bounded alias/projection work where needed.
- Continue targeted var ID/symbol and duplicate/subduplicate review tiers.

### Huge datasets / post-PRISM ingestion

Done:

- P6A-Mac — T-cell GWPS streaming/chunked ingestion path completed enough to be
  recorded in `wiki/pert-gym/current-status.md`.
- P6B-Mac — produced the post-PRISM huge dataset plan and is currently blocked
  where source/resource decisions are genuinely needed; Sanger dual-guide CRC was
  rechecked on 2026-06-22 and is verified complete as
  `sanger_dual_guide_crc/mapping_counts` (2,081,431 × 1; source MAPPING.zip
  staged on GCS).

Active / deferred:

Priority order from Jérémie (2026-06-22):

1. PRISM staged GCS ingestion from `prism_google_drive_datasets_20260622` (`t_c9a686f5`).
2. SCP Chrome-auth retries via OMX/computer-use (`t_a469ca1d`).
3. PerturBase row113 metadata contract (`t_f7311bc4`): row114/GSE156170 is user-excluded for ambiguity; row115/GSE142078 is accepted.
4. Models and validations (`t_7523ac9a`), with side tracks for a Lamin collection exploration notebook (`t_e7277a9b`) and one real-dataset scGEN + metrics run (`t_b821b177`).

- P6B-Mac (`t_d61b0ed9`) — blocked: needs source/resource decisions for other
  huge missing datasets. XAtlas/Orion is resource-deferred until a high-scratch
  host and safe `connect_pertdata()` chunker exist; do not run it on the current
  Mac scratch budget. Do not prioritize it ahead of the four-item order above.
- P7S-Mac (`t_eb23ad95`) — scoped final audit refreshed after OPS2,
  GEARS/scGEN real-subset approvals, and T29 status changes; old P7-Mac
  (`t_b0e396a4`) remains a stale dependency artifact rather than the live audit
  path.

### Temporal pretraining catalogue and ingestions

Done:

- T0 — validated/prioritized the temporal-pretraining catalogue.
- T1 — smoke-ingested CELLxGENE human embryonic limb subset.
- T2 — full-ingested the smallest human embryonic limb scRNA dataset.
- T3/T4 — continued explicit temporal dataset batches.
- T6 — probed SCP/BioStudies/adjacent candidates; ingested verified
  E-MTAB-3321 mouse blastomeres (`124 × 41,480`).
- T7/T8/T10/T11/T12 are supervisor-approved/done on the board:
  - T7 — three CELLxGENE development temporal triplets accepted.
  - T8 — bounded ClassPlacodes triplet accepted; larger CELLxGENE collections
    remain backed/chunked-ingestion blockers.
  - T10 — GSE296916 fibrotic lung triplet accepted; SCP1846 remains a
    streaming/GCS/high-disk blocker and GSE312010 needs source repair/salvage.
  - T11 — four NASC-seq GSE128273 prefixes accepted with new/old RNA side
    matrices; scSLAM remains a converter/semantics blocker, while scNT was
    finished by T22R-Mac.
  - T12 — ZESTA 5hpf spatial-temporal triplet accepted; MOSTA remains a
    chunked/staged resource blocker.
- T-roadmap-Mac produced the current batched continuation plan:
  `artifacts/schema_audit/temporal_ingestion_batches_20260622.{md,json}`.
  It covers all 84 `A_explicit` catalogue rows and groups pending work into
  source/tooling-oriented batches, mostly 2–3 datasets per card.
- T22R-Mac (`t_d276238d`) repaired/finished the interrupted scNT-seq GSE141851
  state after workspace consolidation: 19 verified same-prefix triplets under
  `temporal_pretraining/metabolic_labeling/scnt_seq/GSE141851/`, totaling 56,500
  obs rows. Canonical `X.h5ad` is C_new + T_old total RNA counts, with linked
  `X_new_counts.h5ad` and `X_old_counts.h5ad` preserving new/pre-existing RNA
  semantics. Nine pre-existing partial prefixes were metadata-verified and ten
  remaining prefixes were written and verified. scSLAM GSE115612 remains blocked
  pending explicit multi-timepoint/labeling-window semantics. Artifacts:
  `artifacts/schema_audit/temporal_scnt_seq_gse141851_t22r_20260622.{md,json}`
  and `artifacts/scripts/ingest_temporal_scnt_seq_gse141851_20260622.py`.
- T19-Mac large mouse embryo/gastrulation source probe staged `E-MTAB-6967`
  `atlas_data.tar.gz` to GCS and identified a chunked MatrixMarket converter path;
  rows 61/66 remain source-blocked. See
  `artifacts/schema_audit/temporal_t19_mouse_embryo_batch_status_20260622.md`.
- T13-Mac (`t_152cb737`) completed Zebrahub stage continuation: all 11 stage
  triplets now verify under `temporal_pretraining/zebrahub/` (`124,306` obs rows
  total, same-prefix obs→X→var links and payloads OK). T13F-Mac (`t_6ca5cc4f`)
  implemented and smoked the mouse gastrulation HCA archive converter without
  `mmread` or full MTX extraction; T13G-Mac (`t_b70afb5d`) then completed the
  full sequential ingestion under `temporal_pretraining/mouse_gastrulation_hca/`:
  141 same-prefix triplet prefixes, 139,331 obs rows total, 29,452 var rows per
  prefix, 483,512,215 MatrixMarket entries streamed, 0 column-order violations,
  final read-back verifier status `verified`. Artifacts:
  `artifacts/schema_audit/temporal_t13_final_status_20260622.{md,json}`,
  `artifacts/schema_audit/temporal_t13f_mouse_gastrulation_stream_lamin_20260622.{md,json}`,
  and `artifacts/schema_audit/temporal_t13g_mouse_gastrulation_full_status_20260622.md`.
- T14-Mac (`t_73201941` + SCP454 continuation `t_520bb583`) is now complete for
  the developmental small-animal SCP batch after Chrome/OMX authenticated-source
  recovery and chunked dense-TSV ingestion. Row 56 `SCP667` zebrafish hindbrain is
  verified as `temporal_pretraining/scp667_zebrafish_hindbrain` (`9,026 × 27,282`,
  stages 16/24/44 hpf); row 76 `SCP162` zebrafish embryogenesis is verified as
  `temporal_pretraining/scp162_zebrafish_embryogenesis` (`38,731 × 17,239`,
  60,284,826 sparse log2TPM nonzeros, 12 developmental stages); row 58 `SCP454`
  Ciona ten-stage developmental atlas is verified under
  `temporal_pretraining/scp454_proto_vertebrate_lineages/` as 19 same-prefix
  chunks (`90,579 × 15,037`, final chunk 579 cells, 57 artifacts). For SCP454,
  expression cell/gene IDs were normalized by stripping balanced double quotes,
  metadata was subset/reordered to the 90,579 expression cells, and final light
  verification confirmed obs→X→var links plus X payload existence for every
  chunk. Artifacts:
  `artifacts/schema_audit/temporal_t14_scp667_ingestion_20260622.{md,json}`,
  `artifacts/schema_audit/temporal_t14_scp162_ingestion_20260622.{md,json}`,
  `artifacts/schema_audit/temporal_t14_scp162_scp454_browser_recovery_20260622.{md,json}`,
  `artifacts/schema_audit/temporal_t14_scp454_ingestion_20260622.{md,json}`,
  `artifacts/schema_audit/temporal_t14_scp454_light_verification_20260622.json`,
  and `artifacts/schema_audit/temporal_t14_final_status_20260622.{md,json}`.
- T15-Mac (`t_625ca1ff`) resolved SCP1290/SCP3301/SCP1467 public manifests. SCP
  browser-auth continuation `t_1454d364` later recovered and byte-verified the
  `SCP1467` expression/metadata files in GCS, so SCP1467 is now a conversion
  planning/ingestion follow-up rather than an auth blocker. SCP1290 has a bounded
  GEO scRNA alternate (`GSE153162`): 20 per-sample 10x H5 triplets were ingested
  and verified under
  `temporal_pretraining/scp1290_gse153162_mammalian_cerebral_cortex/` (`128,746 ×
  27,998` per-sample feature space; E10–P4/Fezf2 contexts; obs→X→var verified).
  SCP3301/GSE315712 is deferred for processed-vs-raw matrix-family selection and
  staged/chunked conversion; avoid the 33 GB GEO RAW tar as the first path.
  Artifacts:
  `artifacts/schema_audit/temporal_t15_scp_gse153162_status_20260622.{md,json}`
  and `artifacts/scripts/ingest_temporal_t15_scp_gse153162_20260622.py`; SCPAUTH
  continuation artifact: `artifacts/schema_audit/temporal_scp_browser_auth_continuation_t1454d364_20260623.{md,json}`.
- T17-Mac (`t_c8478c79`) resolved BioStudies/ArrayExpress embryo processed
  exports and ingested/verified E-MTAB-3929 human preimplantation embryos as
  `temporal_biostudies/E-MTAB-3929_preimplantation_embryo` (`1,519 × 34,570`,
  timepoints 3–7 day, obs→X→var verified). E-MTAB-9304 and E-MTAB-8060 now have
  source-resolved SCEA h5ad/MTX exports, but need backed/MatrixMarket conversion
  rather than full-load conversion under current RAM pressure. Artifact:
  `artifacts/schema_audit/temporal_t17_biostudies_embryo_batch_status_20260622.md`.
- T18-Mac (`t_a8a338f8`) ingested and verified the bounded GXA rabbit embryo PGC
  dataset `E-MTAB-10894` as
  `temporal_pretraining/gxa/E-MTAB-10894_rabbit_embryo_pgc` (`381 × 16,587`,
  ages 4/5/6/7 day, obs→X→var verified). Residual rows `E-MTAB-8894` human fetal
  LGE and `E-GEOD-234602` Drosophila organogenesis are source-resolved via GXA
  MatrixMarket endpoints but deferred: design TSVs are 30.9 MB / 98.8 MB and
  raw/normalised zip HEAD probes timed out after 20 s, so continue only with a
  staged/backed GXA converter. Artifacts:
  `artifacts/schema_audit/temporal_t18_biostudies_batch_b_status_20260622.{md,json}`
  and `artifacts/schema_audit/temporal_t18_biostudies_batch_b_verification_20260622.json`.
- T24-Mac (`t_9217af79`) ingested and verified the bounded OrganoidDB/GEO kidney
  organoid Takasato iPS timecourse (`Odd001100 / GSE118184`) as
  `temporal_pretraining/organoid/odd001100_gse118184_kidney_organoid_takasato_ips_timecourse`
  (`9,190 × 14,821`; Day0/7/12/19/26 labels; obs→X→var verified). Artifacts:
  `artifacts/schema_audit/temporal_t24_organoiddb_20260622.{md,json}` and
  `artifacts/scripts/ingest_temporal_t24_organoiddb_20260622.py`.
- T25-Mac (`t_a61d362c`) ingested and verified all three OrganoidDB mesoderm/heart/intestine rows:
  `Odd001154 / GSE194214` paraxial mesoderm somitoids
  (`18,716 × 33,694`; days 1/2/3/5), `Odd001137 / GSE158999` mouse
  gastruloid cardiogenesis (`30,496 × 23,961`; days 4/5/6/7), and
  `Odd001151 / GSE148093` mouse intestinal organoids (`5,212 × 24,870`; sample
  days 0/1/2/3, restricted to OrganoidDB-listed GSM4453981–GSM4454011). All have
  payloads and obs→X→var links verified. Artifacts:
  `artifacts/schema_audit/temporal_organoiddb_t25_ingestion_20260622.{md,json}`
  and `artifacts/scripts/ingest_temporal_organoiddb_t25_20260622.py`.
- T26-Mac (`t_0b093cca`) ingested and verified the bounded OrganoidDB cerebral
  cortex organoid month timecourse (`Odd001111 / GSE130238`) as
  `temporal_pretraining/organoiddb/GSE130238_cortical_organoid_months`
  (`16,086 × 33,694`; 1/3/6/10 month labels; obs→X→var verified). T26 residuals:
  `Odd001138 / GSE162547` pancreas organoid exposes raw Cell Ranger barcode-universe
  matrices (`33,939 × 6,794,880` per sample) and must not be ingested without
  filtered-cell/cell-calling metadata; scHOB static crawl found no concrete matrix
  payload URL. Artifacts: `artifacts/schema_audit/temporal_organoiddb_t26_status_20260622.{md,json}` and
  `artifacts/scripts/ingest_temporal_organoiddb_t26_20260622.py`.
- T40-Mac row 86 (`Ceratitis capitata` LSFM embryogenesis) has a metadata-only
  representation probe in
  `artifacts/schema_audit/temporal_ceratitis_lsfm_representation_probe_20260622.{md,json}`.
  It should remain blocked until an image/non-expression contract is reviewed; do
  not ingest the ~275.6 GB Zenodo microscopy payload as expression triplets.
- T35-Mac STOmics spatial-development batch has a metadata-only source/duplicate
  probe in
  `artifacts/schema_audit/temporal_t35_stomics_spatial_probe_20260622.{md,json}`.
  HESTA needs a dedicated SPA/source-manifest resolver; STDS0000060 exposes five
  processed Drosophila Stereo-seq H5ADs but the smallest is ~1.44 GB; MOSTA
  remains a 187-file atlas. No Lamin writes were performed.
- T36-Mac (`t_2fa866a6`) resolved the GEO/CNGB spatial-development batch and
  ingested the bounded craniosynostosis Visium timecourse `GSE303344` as three
  verified same-prefix triplets:
  `temporal_pretraining/gse303344_craniosynostosis/{e14_5_2um,e18_5_2um,p3_2um}`
  (`4,671,144 × 19,059`, `532,208 × 19,059`, `340,320 × 19,059`; spatial
  coordinates complete; obs→X→var verified). `CNP0002220` is already represented
  by the ZESTA/STDS0000057 5hpf triplet and should not be duplicated as a raw
  project prefix. `GSE313896` is source-resolved but not ingested: GEO exposes a
  2.4 GB RAW tar with 46 Visium samples and should continue via a staged/chunked
  extractor. Artifacts:
  `artifacts/schema_audit/temporal_t36_spatial_source_probe_20260622.{md,json}`
  and `artifacts/schema_audit/temporal_t36_gse303344_craniosynostosis_20260622.{md,json}`.
- T37-Mac (`t_ca492e7d`) resolved the human/axolotl/planarian spatial
  regeneration batch and ingested one bounded ARTISTA/STDS0000056 axolotl
  telencephalon slice as
  `temporal_pretraining/artista_stds0000056/10DPI_1_stereoseq` (`9,440 ×
  27,600`; 10DPI; spatial coordinates complete; obs→X→var verified). The source
  h5ad has corrupt HDF5 metadata under `/uns`, but X/obs/var/spatial groups were
  readable and the canonical triplet was reconstructed/repaired from those groups.
  Residuals: GSE326326 is a 2.43 TB GEO RAW tar requiring range-aware member
  extraction/staging; STAPR needs a browser/API download resolver. Artifacts:
  `artifacts/schema_audit/temporal_t37_spatial_regeneration_status_20260622.{md,json}`
  and `artifacts/schema_audit/temporal_t37_artista_triplet_verification_20260622.json`.
- T38-Mac (`t_4c3abcd9`) completed ZESTA continuation: zf3/zf10/zf12/zf18/zf24
  Stereo-seq triplets were added and verified, and zf5 was reverified as pre-existing
  (6/6 ZESTA timepoint prefixes verified). STT0000071 zebrafish heart was source-
  probed but not ingested: metadata and 9 sample/timepoint records are public, but
  samples are `is_download=false` with `files=[]`, STSP sequencing/gene-expression
  table probes are empty for project/relation/samples, and no Lamin duplicate keys
  were found. Artifacts: `artifacts/schema_audit/temporal_t38_zesta_heart_partial_status_20260622.{md,json}`
  and `artifacts/schema_audit/temporal_t38_heart_source_probe_20260622.{md,json}`.
- T31-Mac row 124 (Tian/Kampmann 2019 iPSC/neuron CRISPRi) is
  `already_present_verified_schema_gap`:
  `artifacts/schema_audit/temporal_t31_tian_kampmann_status_20260622.{md,json}`.
  Do not redownload the broad Zenodo/scPerturb bundle; exact existing prefixes
  `scperturb/tian19_iPSC` and `scperturb/tian19_day7neuron` are verified. If this
  dataset needs model-ready promotion, do an obs-only canonical schema repair for
  `pert_name`/`pert_type`/`pert_time` → perturbation fields.
- T30-Mac PerturBase reprogramming/endoderm batch is audited with no Lamin writes:
  `artifacts/schema_audit/temporal_t30_perturbase_status_20260622.{md,json}`.
  Row 117 is duplicate-reviewed against existing `SchiebingerLander2019` and not
  ingested; rows 116 and 118 remain source-converter/contract blockers because
  public GEO payloads do not directly match the PerturBase filtered objects.
- T43-Mac (`t_f70e9535`) is partial-complete: row 82 human gastrulation
  `E-MTAB-9388` was ingested and verified as
  `temporal_pretraining/human_gastrulation/E-MTAB-9388` (`1195 × 25330`, CS7,
  `obs -> X -> var` links/payloads OK). Row 60 Drosophila `GSE190149` is
  source-resolved but deferred: the RNA subseries `GSE190147` has a direct
  MatrixMarket triplet (`23932 × 547805`, 212,703,167 nnz, ~698 MB gz) needing
  staged/chunked ingestion, while ATAC `GSE190130` should be typed auxiliary
  `X_atac/var_atac` only after representation review. Artifact:
  `artifacts/schema_audit/temporal_t43_drosophila_human_gastrulation_status_20260622.md`.
- T41-Mac (`t_6cc6cd95`) completed a CELLxGENE high-value B-candidate probe and
  two bounded verified ingestions. Row 2 outflow/aortic valve snRNA-seq is
  `temporal_pretraining/human_outflow_tract_aortic_valve_derivatives/cellxgene_single_nuclei_sn_rna_seq_of_the_human_outflow_tract_and_aortic_valve_tissue_cs16`
  (`30,125 × 31,008`; 13th week post-fertilization / Carnegie 17 / adult raw
  labels; obs repaired so only 13th-week cells have minute-valued `timepoint`). Row 8 mouse embryonic timelapse smallest cluster is
  `temporal_pretraining/mouse_embryonic_timelapse_gastrula_to_pup/cellxgene_major_cell_cluster_testis_and_adrenal`
  (`3,342 × 45,525`; Theiler stage 18–27 order labels). Both have verified
  payloads and obs→X→var links. Artifacts:
  `artifacts/schema_audit/temporal_cellxgene_t41_ingestion_20260622.{md,json}`
  plus read-only probe `temporal_cellxgene_t41_probe_20260622.json`.
- T42-Mac (`t_51169565`) completed the CELLxGENE small/partial B-candidate
  continuation with 9 verified triplets / 203,296 obs across fetal lung, fetal
  bone marrow Down syndrome, and mouse cerebellum. Fetal lung added 7 bounded
  10x subsets (fibroblast/smooth muscle, epithelium/no-cilium, cilium,
  endothelium, B cells, T/NK/ILC, myeloid) beyond the already-ingested PNS;
  bone marrow added Down syndrome fetal BM (`16,743 × 33,715`); mammalian
  cerebellum added mouse cerebellum stages (`115,282 × 20,287`). All final
  payloads and obs→X→var links were verified from Lamin. Artifacts:
  `artifacts/schema_audit/temporal_cellxgene_t42_final_status_20260622.{md,json}`.
- T21-Mac (`t_c329fc94`) completed bounded source/duplicate probes for rat kidney
  row 71, mouse retina sodium-iodate row 119, and rabbit wound row 131. No Lamin
  writes were performed. Artifact:
  `artifacts/schema_audit/temporal_t21_geo_repair_status_20260622.md`.
- T20-Mac (`t_7ed6d66c`) completed GEO source/duplicate probes for sea lamprey
  GSE334273, mouse cortical inhibitory neurons GSE280655, and human embryos
  GSE325829. The bounded mouse E18.5 cortical inhibitory neuron matrix was
  ingested and verified as
  `temporal_pretraining/developing_mouse_cortical_inhibitory_neurons/GSE280655_E18_5`
  (`8,015 × 21,909`, obs→X→var verified). Artifacts:
  `artifacts/schema_audit/temporal_t20_geo_developmental_status_20260622.md` and
  `artifacts/schema_audit/temporal_gse280655_cortical_inhibitory_neurons_20260622.json`.
- T33/T102-Mac (`t_a13812f2`, `t_102c5a38`) resolved axolotl regeneration sources/duplicates for
  rows 134–136 and ingested row 136: row 134 maps to bounded-but-tar-only
  `GSE165901` axolotl MatrixMarket members; row 135 resolves to a SciLifeLab
  ShinyCell/GitHub app without published original RDS payloads; row 136 `SCP499`
  Early-Bud Blastema is verified as
  `temporal_pretraining/gse121737_axolotl_blastema/early_bud_blastema` (`2,013 ×
  59,171`, obs→X→var links/payloads OK) using the browser-auth staged matrix plus
  API-derived idents/coordinates staged under `api_derived/`. Artifacts:
  `artifacts/schema_audit/temporal_t33_axolotl_status_20260622.md` and
  `artifacts/schema_audit/temporal_scp499_early_bud_ingestion_20260623.md`.

Current blockers:

- T29-Mac (`t_22409681` plus row113 continuation `t_f7311bc4`) — row 115/GSE142078 is ingested and verified as `temporal_pretraining/perturbase/gse142078_luhmes_crispri_day8` (`8,843 × 33,694`, obs→X→var links/payloads OK; PerturBase 7,684-cell discrepancy preserved in obs `qc_note`). Row 114/GSE156170 is `excluded_with_reason` from the active path by user decision due perturbation-label/QC ambiguity; preserve the provenance note but do not reopen it as an active blocker. Row 113/GSE216481 is active with a concrete metadata contract, staged/probed at GCS with QC-pass RNA components `201218_RNA` and `210322_TFAtlas`; exact missing inputs before canonical write are the barcode/sequence/numeric-id-to-ORF/TF-symbol map and component-specific filtered-cell inclusion table. Artifacts: `artifacts/schema_audit/temporal_t29_gse216481_row113_metadata_contract_20260623.md`, `artifacts/scripts/validate_temporal_perturbase_t29_gse216481_contract_20260623.py`, `artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.md`.
- T41-Mac residual — row 3 human fetal gene expression atlas remains
  `streaming_or_chunked_required` (CELLxGENE assets are ~4.94 GB subset and
  ~20.03 GB full, 5.06M cells reported); row 8 remaining mouse timelapse assets
  remain chunked/streaming follow-up after the bounded smoke; row 2 Visium assets
  are deferred for the spatial representation contract. Artifact:
  `artifacts/schema_audit/temporal_cellxgene_t41_ingestion_20260622.md`.
- T42-Mac residual — do not claim full collection completion from the small
  continuation. Fetal lung PNS was already ingested; Visium assets are deferred
  for spatial representation, while lung Organoid/All cells exceed the bounded
  full-ingest threshold. Fetal bone marrow normal CITE-seq was already ingested;
  CD34+ (606 MB) and all-cells fetal BM (1.75 GB) need chunked/explicit
  continuation. Human cerebellum (840 MB) is deferred for a chunked/backed path.
  Artifact: `artifacts/schema_audit/temporal_cellxgene_t42_final_status_20260622.md`.
- T5 (`t_48dc431c`) — stale/excluded by user decision: GSE173650 is mouse
  cerebrum snATAC, not rat kidney, and this path should not be reopened as an
  active blocker unless a new explicit rat-kidney source is provided.
- T21 (`t_c329fc94`) — GEO repair/source batch remains ingestion-blocked after
  bounded probes: row 71 should not use GSE173650 (mouse cerebrum snATAC; rat
  kidney likely PRJNA649702 raw SRA), row 119 GSE312010 re-download still has
  gzip CRC/EOF failures in GEO supplementary members, and row 131 GSE328779 is
  source-resolved but exposes unfiltered 10x raw droplet H5s (one-sample smoke:
  6,794,880 barcodes × 23,407 features). Need processed/filtered matrices or a
  reviewed cell-calling/salvage workflow before any Lamin writes.
- T20 (`t_7ed6d66c`) residuals after verified GSE280655 ingestion: GSE334273 sea
  lamprey exposes four 10x MTX samples inside a 1.19 GB RAW tar plus metadata and
  needs staged/streamed conversion; GSE325829 human embryos exposes a direct
  11.08 GB h5ad and needs GCS staging + backed/chunked ingestion. Do not full-load
  either locally. Artifact:
  `artifacts/schema_audit/temporal_t20_geo_developmental_status_20260622.md`.
- T33 residual — axolotl regeneration batch remains partially source-blocked after
  row 136/SCP499 ingestion: row 134 `GSE165901` has a bounded axolotl subset but only as
  members inside a 1.97 GB GEO RAW tar, requiring a selective tar extractor and
  MatrixMarket smoke before write; row 135 SciLifeLab Serve is a ShinyCell app
  whose GitHub repo names but does not publish the original Seurat RDS objects.
  Row 136 `SCP499` is no longer a blocker: API-derived sidecars were byte-verified
  in GCS and the triplet verifies in Lamin. Artifacts: `artifacts/schema_audit/temporal_t33_axolotl_status_20260622.md`,
  `artifacts/schema_audit/temporal_scp_browser_auth_continuation_t1454d364_20260623.{md,json}`, and
  `artifacts/schema_audit/temporal_scp499_early_bud_ingestion_20260623.{md,json}`.
- T7 (`t_23b14d41`) — review-required blocked row after 3 CELLxGENE development
  triplets; supervisor comment says accepted.
- T23 (`t_103c7d05`) — OrganoidDB retina/blood-vessel batch source-resolved
  and blocked: Odd001126/GSE142526, Odd001132/GSE152212, and
  Odd001155/GSE196799 have GEO SOFT sample/time metadata and no duplicate hits,
  but exposed GEO supplements are RAW tar/metadata-only with no obvious processed
  cell×gene matrices; no Lamin writes. Artifact:
  `artifacts/schema_audit/temporal_organoiddb_t23_status_20260622.md`.
- T24 (`t_9217af79`) residual OrganoidDB blockers after verified kidney
  organoid ingestion: Odd001097/GSE106245 cortical organoid has explicit week
  metadata but no processed scRNA matrix in GEO supplements; Odd001099/GSE138002
  retina has processed MatrixMarket/barcode/gene payloads but needs a
  streaming/chunked converter for the 565 MB gzip / 178M-nnz final matrix.
  Artifact: `artifacts/schema_audit/temporal_t24_organoiddb_20260622.md`.
- T35 (`t_3404197c`) — metadata-only STOmics spatial probe: HESTA manifest not
  resolved from the static page/API, STDS0000060 has 5 large processed H5ADs
  (1.44–7.05 GB) and 0 exact duplicate hits, and MOSTA remains large
  (187 files/61 H5ADs). Needs dedicated staged/chunked spatial strategy before
  any Lamin write. Artifact:
  `artifacts/schema_audit/temporal_t35_stomics_spatial_probe_20260622.md`.
- T14/T16/T32 SCP cards were unblocked on 2026-06-22 after Jérémie confirmed Broad Single Cell Portal is logged in in Chrome and OMX computer-use agents can download datasets directly. Retry SCP payloads via logged-in Chrome/computer-use, stage to GCS with byte verification, then convert/ingest safely; do not treat old headless HTTP 401 probes as terminal blockers. A headless Hermes shell retry for T16 still could not access authenticated SCP downloads, so the execution surface matters.
- T16 (`t_43b90af5`) — remains blocked for this headless worker and needs an actual
  browser-capable OMX/computer-use SCP export for Hand-GFP Drosophila embryos
  (`SCP1469`), early chick development (`SCP1570`), and retinal ganglion
  injury/regeneration (`SCP1846`). `SCP1570` still needs matrix-family selection
  before ingestion because it exposes 68 HH-stage whole/subset files. Artifacts:
  `artifacts/schema_audit/temporal_t16_scp_probe_20260622.md` and
  `artifacts/schema_audit/temporal_t16_scp_retry_20260622.md`.
- T27 (`t_e004636c`) — `SCP211` is no longer a plain auth blocker after
  SCPAUTH continuation `t_1454d364`; logged-in Chrome can expose the file rows,
  but matrix-family selection is still needed because the table mixes one
  combined adult-kidney expression matrix and multiple day-specific 10x-style
  matrices up to ~4.9 GB. Non-SCP residuals remain source-gated: `SCP3697`
  exposed no study files in the old probe and `GSE293573` exposed only GEO/SRA
  metadata with no processed supplementary matrix. Artifacts:
  `artifacts/schema_audit/temporal_t27_organoid_public_batch_20260622.md` and
  `artifacts/schema_audit/temporal_scp_browser_auth_continuation_t1454d364_20260623.{md,json}`.
- T28 (`t_40508832`) — `SCP282` is now a representation/download-planning task,
  not a terminal auth block: logged-in Chrome can expose the known 3/6-month
  expression rows, but seven sample/timepoint matrices total ~2.7 GB before
  annotations and need explicit selection/chunking before staging. Non-SCP
  residuals remain source-gated: `GSE269572` has no GEO supplementary processed
  matrix and `GSE329346` exposes only a bulk RNA-seq gene-expression supplement,
  not scRNA. Artifact:
  `artifacts/schema_audit/temporal_t28_organoid_followup_status_20260622.md`.
- T39 (`t_16e0d7b6`) — plant spatial/development batch is probe-only/source
  blocked for now: tomato GSE293948 exposes a single 12 GB processed RDS that
  needs staged/high-memory Seurat inspection; Arabidopsis life-cycle needs exact
  repository/accession resolution; Arabidopsis STOmics row needs the exact
  STDS/project accession. Artifact:
  `artifacts/schema_audit/temporal_t39_plant_spatial_probe_20260622.md`.
- T32 (`t_d2e1109a`) — still not ingested; requires a browser/computer-use-capable
  retry, not plain CLI. Zebrafish beta-cell regeneration (`SCP1549`), regenerating
  fin osteoblasts (`SCP1674`), and zebrafish retina regeneration (`SCP1973`) expose
  bounded expression+metadata payloads (9 files, 374,893,756 expected bytes) and
  exact planned Lamin prefixes had 0 duplicate hits. Direct requests remain HTTP
  401; Chrome has encrypted SCP cookies but this worker cannot safely use them
  without OMX/computer-use or CDP. Download the payload-plan files, stage to GCS
  with byte verification, then inspect metadata before conversion/Lamin writes.
  Artifacts: `artifacts/schema_audit/temporal_t32_zebrafish_regen_scp_20260622.md`,
  `artifacts/schema_audit/temporal_t32_browser_retry_payload_plan_20260622.json`.
- T34 (`t_e8fd9411`) — ingested/verified the three T34 `GSE121737` axolotl
  blastema SCP rows: SCP422 intact limb (`3421 × 387212`), SCP500 medium-bud
  merged from two composite matrices (`12531 × 516932`), and SCP489 wound-healing
  (`7270 × 410988`). Same-prefix obs→X→var links and payloads verified on
  branch `jkobject`; duplicate probe was clean before writing. Caveat: GEO
  `repGene` features are de-novo transcript/contig IDs, not canonical gene
  symbols, so this needs model/review awareness before treating `var` as
  orthologized genes. Artifact:
  `artifacts/schema_audit/temporal_t34_gse121737_axolotl_blastema_ingestion_20260622.md`.

### Model-ready subset and benchmark path

Done:

- M0 — perturbation prediction model environment/skeleton.
- M1 — first model-ready subset (`pert-gym/model-ready/20260621`, one tiny
  loader-smoked VIPerturb member).
- M2 — isolated model environment design.
- M3/M4/M5/M6/M7/M8 — classical/LPM/CPA/chemCPA/legacy-trVAE/scPRAM/trVAE-replacement environment and smoke
  setup or precise blocker artifacts.
- MB0 — canonical benchmark loader.
- MB1 — classical baselines on canonical loader/synthetic fallback.
- MB2 — LPM smoke benchmark in isolated env.
- MB3 — CPA smoke benchmark.
- MB4 — chemCPA generic model-ready-v0 path correctly rejected VIPerturb/synthetic
  substitution; MB4F added a real tiny DRUG-seq molecular expression loader with
  PubChem/RDKit fingerprints (`artifacts/model_benchmarks/chemcpa_drugseq_tiny_20260622.*`).
- MB5 — legacy trVAE correctly blocked by TensorFlow 1.15/Python/Mac incompatibility.
- M8-Mac — blocked trVAE path replaced by maintained `trvae-replacement` conditional VAE smoke adapter; artifacts under `artifacts/model_benchmarks/trvae_replacement_20260622.*`.
- MB6 — scPRAM tiny CPU smoke accepted with smoke-only perturbation identity
  adapter semantics.
- MB6F — scPRAM real adapter semantic smoke added: upstream scPRAM uses binary
  control/stimulated `condition` for one perturbation and real
  cell-type/context transfer, not perturbation identities as cell types. Current
  artifact: `artifacts/model_benchmarks/scpram_real_adapter_20260622.md`.
  Synthetic adapter smoke passed in `.venv-models/scpram`; current
  `model-ready-v0` is explicitly infeasible for a real scPRAM benchmark until a
  bounded multi-context control/stimulated subset is promoted/exported.
- M8B-Mac — scGEN upstream `scgen==2.1.0` was assessed but not enabled because
  current Python 3.11 scverse resolver combinations failed import; added
  `ScgenPerturbationAdapter`, `.venv-models/scgen`, and synthetic-only AnnData
  condition/control smoke artifacts under `artifacts/model_benchmarks/scgen_20260622.*`.
- M8B-follow-up (`t_8a3acae6`) — scGEN now has a bounded real VIPerturb
  expression adapter smoke: `scgen_real_viperturb_tiny_20260622.{json,h5ad}`
  exports 22 obs × 96 genes from the reviewed model-ready member, with 12
  controls and five CRISPRi perturbation identities; `scgen_real_20260622.*`
  passes without synthetic fallback. This remains an adapter smoke, not a
  biological performance claim.
- GEARS follow-up (`t_e9f3c295`) — bounded Datlinger17 real GEARS-ready adapter/API
  smoke is supervisor-accepted: `gears_datlinger17_tiny_20260622` exports
  240 cells × 128 genes with 40 controls and held-out NFKB1/NFATC1; official
  `cell-gears` import and contract smoke pass. This is not upstream GEARS GNN
  training or a biological performance claim.

Next model work:

- Promote more real model-ready members so benchmarks stop relying on tiny or
  synthetic fallbacks.
- Promote/export a scPRAM-specific bounded subset with controls plus one
  stimulated perturbation across at least two real cell types/contexts.
- Promote/export a scGEN-ready bounded expression subset beyond the current
  VIPerturb adapter smoke only if a larger/reviewed biological scGEN evaluation
  protocol is defined; keep synthetic/model-env smokes clearly separate from
  performance claims.
- Add model-specific data adapters only when their biological assumptions are
  explicit and reviewable.

### Documentation cleanup path

Done / accepted:

- DOC0 (`t_302726a1`) — created this high-level Kanban mirror and documentation
  navigation map.
- DOC1 (`t_973b7d68`) — supervisor-approved root `README.md` as the concise
  human project overview.
- DOC2 (`t_fc44edcc`) — completed `CLAUDE.md` as the concise agent operating
  guide.
- DOC4/DOC5 — cleaned human-facing `docs/*.md` and detailed `wiki/*.md` pages;
  use the linked docs/wiki pages as canonical detail, not this mirror.

Current DOC6 job: refresh only `TODO.md` after supervisor approvals; keep this
file concise and defer exact details to the canonical docs/artifacts above.

## Practical next cards

1. Create/run temporal continuation cards from
   `artifacts/schema_audit/temporal_ingestion_batches_20260622.{md,json}`;
   do not create one card per catalogue row.
2. Use the accepted README/CLAUDE/docs/wiki/TODO split as the stable navigation
   model; keep future mirror edits concise.
3. Create narrower follow-up cards only for remaining temporal large/source/
   converter blockers from accepted partial ingestions, not for already accepted
   T7/T8/T10/T11/T12 rows.
4. Decide P6B-Mac resource/source strategy for huge datasets, especially a
   high-scratch/high-memory XAtlas/Orion route. The refreshed P7S final
   audit/status artifact is under `artifacts/schema_audit/`.
5. Promote additional model-ready subsets from `triplet-integrity-ok` members.
6. Re-run/read the canonical status checks in `wiki/pert-gym/current-status.md`
   before making broad claims about database size, model readiness, or dataset
   coverage.

## Do-not-forget safety constraints

- Docs-only tasks must not write Lamin, data, model outputs, or staged payloads.
- Large `.h5ad`/archives are temporary cache only; use GCS staging/cache rules.
- Do not load huge matrices blindly; prefer metadata/backed/chunked probes.
- Do not treat old numeric summaries (`110`, `111`, `720`, `721`) as database
  size. Use the current count vocabulary.
- Do not resurrect the legacy GSE150818/properseq mistaken dataset.
