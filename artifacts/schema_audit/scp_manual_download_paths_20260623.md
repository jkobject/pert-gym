# SCP manual download paths — 2026-06-23
Use logged-in Broad Single Cell Portal in Chrome. Download only the files below, then upload to the exact `stage_prefix` shown. Prefer the active GCS Fuse mount `/Users/jkobject/mnt/gcs/scperturb` or `gcloud storage cp`; verify remote byte size before deleting local files.
TSV companion: `artifacts/schema_audit/scp_manual_download_paths_20260623.tsv`.
## SCP1469
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP1469
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1469/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `expression_matrix.tsv.gz` |  | Expression Matrix / This is the Expression Matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1469/download?filename=expression_matrix.tsv.gz |
| MED | `hrt_singletgenelist.txt` |  | Gene List / This is the Gene List | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1469/download?filename=hrt_singletgenelist.txt |
| MED | `Hrt_singletmetadata.txt` |  | Metadata / This is the Metadata Matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1469/download?filename=Hrt_singletmetadata.txt |
| MED | `Hrt_singletCluster.txt` |  | Cluster / Nine Hand-positive cell types and their transcriptomes. | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1469/download?filename=Hrt_singletCluster.txt |

## SCP1549
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP1549
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1549/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `220629_bfx1987_counts_1_.csv.gz` | 16706916 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1549/download?filename=220629_bfx1987_counts_1_.csv.gz |
| MED | `220629_bfx1987_log2cpm.csv.gz` | 80733371 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1549/download?filename=220629_bfx1987_log2cpm.csv.gz |
| MED | `bfx1537_meta_v2.csv` | 2655601 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1549/download?filename=bfx1537_meta_v2.csv |

## SCP1570
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP1570/single-cell-atlas-of-early-chick-development
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1570/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `expression_matrix.mtx.gz` |  | preferred whole-stage file: MM Coordinate Matrix / HH7 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=expression_matrix.mtx.gz |
| MED | `genes.tsv.gz` |  | preferred whole-stage file: 10X Genes File / HH7 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=genes.tsv.gz |
| MED | `barcodes.tsv.gz` |  | preferred whole-stage file: 10X Barcodes File /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=barcodes.tsv.gz |
| MED | `HH6_expression_matrix.mtx.gz` |  | preferred whole-stage file: MM Coordinate Matrix / HH6 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH6_expression_matrix.mtx.gz |
| MED | `HH6_genes.tsv.gz` |  | preferred whole-stage file: 10X Genes File / HH6 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH6_genes.tsv.gz |
| MED | `HH6_barcodes.tsv.gz` |  | preferred whole-stage file: 10X Barcodes File / HH6 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH6_barcodes.tsv.gz |
| MED | `HH5_expression_matrix.mtx.gz` |  | preferred whole-stage file: MM Coordinate Matrix / HH4 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH5_expression_matrix.mtx.gz |
| MED | `HH5_genes.tsv.gz` |  | preferred whole-stage file: 10X Genes File / HH5 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH5_genes.tsv.gz |
| MED | `HH5_barcodes.tsv.gz` |  | preferred whole-stage file: 10X Barcodes File / HH5 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH5_barcodes.tsv.gz |
| MED | `HH4-cluster.tsv` |  | preferred whole-stage file: Cluster / HH4 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH4-cluster.tsv |
| MED | `HH5-cluster_new.tsv` |  | preferred whole-stage file: Cluster / HH5 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH5-cluster_new.tsv |
| MED | `HH6-cluster.tsv` |  | preferred whole-stage file: Cluster / HH6 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH6-cluster.tsv |
| MED | `HH7-cluster.tsv` |  | preferred whole-stage file: Cluster / HH7 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH7-cluster.tsv |
| MED | `HH4_expression_matrix.mtx.gz` |  | preferred whole-stage file: MM Coordinate Matrix / HH4 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH4_expression_matrix.mtx.gz |
| MED | `HH4_genes.tsv.gz` |  | preferred whole-stage file: 10X Genes File / HH4 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH4_genes.tsv.gz |
| MED | `HH4_barcodes_prefix.txt` |  | preferred whole-stage file: 10X Barcodes File / HH4 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=HH4_barcodes_prefix.txt |
| MED | `Metadata_ALL_withSubClusters_plus_CombinedData_new39_modif_noRNA.txt` |  | preferred whole-stage file: Metadata /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1570/download?filename=Metadata_ALL_withSubClusters_plus_CombinedData_new39_modif_noRNA.txt |

## SCP1674
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP1674
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1674/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `bfx1543_counts.csv.gz` | 11856731 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1674/download?filename=bfx1543_counts.csv.gz |
| MED | `bfx1543_normalised.csv.gz` | 57894546 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1674/download?filename=bfx1543_normalised.csv.gz |
| MED | `211209_bfx1543_meta.csv` | 1177167 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1674/download?filename=211209_bfx1543_meta.csv |

## SCP1846
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP1846
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1846/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `PtenAll_count_mat.mtx` |  | MM Coordinate Matrix /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=PtenAll_count_mat.mtx |
| MED | `PtenAll_count_mat_gene_names.csv` |  | 10X Genes File /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=PtenAll_count_mat_gene_names.csv |
| MED | `PtenAll_count_mat_cell_names.csv` |  | 10X Barcodes File /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=PtenAll_count_mat_cell_names.csv |
| MED | `SCP_clusterfile.txt` |  | Cluster / RGC types | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=SCP_clusterfile.txt |
| MED | `SCP_meta_202206.csv` |  | Metadata /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=SCP_meta_202206.csv |
| MED | `PtenAll_data.mtx` |  | MM Coordinate Matrix / Expression data | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=PtenAll_data.mtx |
| MED | `PtenAll_data_gene_names.csv` |  | 10X Genes File /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=PtenAll_data_gene_names.csv |
| MED | `PtenAll_data_cell_names.csv` |  | 10X Barcodes File /  | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1846/download?filename=PtenAll_data_cell_names.csv |

## SCP1973
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP1973
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP1973/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `220909_bfx1544_counts.csv.gz` | 20085253 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1973/download?filename=220909_bfx1544_counts.csv.gz |
| MED | `220909_bfx1544_lognorm.csv.gz` | 181731424 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1973/download?filename=220909_bfx1544_lognorm.csv.gz |
| MED | `220929_bfx1544_meta.csv` | 2052747 | T32 regeneration bounded payload | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP1973/download?filename=220929_bfx1544_meta.csv |

## SCP211
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP211
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP211/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| MED | `gene_sorted-2020-06-05.d0_organoids.mtx` | 1995091905 | SCP211 selected day-family matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.d0_organoids.mtx |
| MED | `d0_genes.tsv` |  | sidecar for gene_sorted-2020-06-05.d0_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d0_genes.tsv |
| MED | `d0_barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.d0_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d0_barcodes.tsv |
| MED | `day0.integrated_iPSC_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.d0_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=day0.integrated_iPSC_metadata.txt |
| MED | `gene_sorted-2020-06-05.d7_organoids.mtx` | 2825968784 | SCP211 selected day-family matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.d7_organoids.mtx |
| MED | `d7_genes.tsv` |  | sidecar for gene_sorted-2020-06-05.d7_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d7_genes.tsv |
| MED | `d7_barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.d7_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d7_barcodes.tsv |
| MED | `day7.integrated_organoids_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.d7_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=day7.integrated_organoids_metadata.txt |
| MED | `gene_sorted-2020-06-05.d15_organoids.mtx` | 4941369579 | SCP211 selected day-family matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.d15_organoids.mtx |
| MED | `d15_genes.tsv` |  | sidecar for gene_sorted-2020-06-05.d15_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d15_genes.tsv |
| MED | `d15_barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.d15_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d15_barcodes.tsv |
| MED | `day15.integrated_organoids_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.d15_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=day15.integrated_organoids_metadata.txt |
| MED | `gene_sorted-2020-06-05.d29_organoids.mtx` | 4858455186 | SCP211 selected day-family matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.d29_organoids.mtx |
| MED | `genes.tsv` |  | sidecar for gene_sorted-2020-06-05.d29_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=genes.tsv |
| MED | `barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.d29_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=barcodes.tsv |
| MED | `day29.integrated_organoids_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.d29_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=day29.integrated_organoids_metadata.txt |
| MED | `gene_sorted-2020-06-05.d32_MA.mtx` | 280384959 | SCP211 selected day-family matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.d32_MA.mtx |
| MED | `d32_MA_genes.tsv` |  | sidecar for gene_sorted-2020-06-05.d32_MA.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d32_MA_genes.tsv |
| MED | `d32_MA_barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.d32_MA.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d32_MA_barcodes.tsv |
| MED | `2020-06-05.d32_MA_orgs_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.d32_MA.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=2020-06-05.d32_MA_orgs_metadata.txt |
| MED | `gene_sorted-2020-06-05.d32_control_sub_organoids.mtx` | 1006368942 | SCP211 selected day-family matrix | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.d32_control_sub_organoids.mtx |
| MED | `d32_genes.tsv` |  | sidecar for gene_sorted-2020-06-05.d32_control_sub_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d32_genes.tsv |
| MED | `d32_barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.d32_control_sub_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d32_barcodes.tsv |
| MED | `day32.integrated_organoids_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.d32_control_sub_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=day32.integrated_organoids_metadata.txt |
| HIGH | `gene_sorted-2020-06-05.ma51_control_organoids.mtx` | 131448493 | SCP211 selected day-family matrix; stage smoke d51 first | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=gene_sorted-2020-06-05.ma51_control_organoids.mtx |
| HIGH | `d51_genes.tsv` |  | sidecar for gene_sorted-2020-06-05.ma51_control_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d51_genes.tsv |
| HIGH | `d51_barcodes.tsv` |  | sidecar for gene_sorted-2020-06-05.ma51_control_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d51_barcodes.tsv |
| HIGH | `d51_control_organoids_metadata.txt` |  | sidecar for gene_sorted-2020-06-05.ma51_control_organoids.mtx | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP211/download?filename=d51_control_organoids_metadata.txt |

## SCP282
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP282
- Stage prefix: `gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP282/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| HIGH | `expression_PGP1.3mon.txt.gz` | 329301790 | 3 months PGP1 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_PGP1.3mon.txt.gz |
| HIGH | `expression_HUES66.3mon.txt` | 504366669 | 3 months HUES66 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_HUES66.3mon.txt |
| HIGH | `expression_PGP1.6mon.txt.gz` | 338974602 | 6 months PGP1 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_PGP1.6mon.txt.gz |
| HIGH | `expression_PGP1.3mon.batch2.txt.gz` | 474361094 | 3 months PGP1 Batch 2 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_PGP1.3mon.batch2.txt.gz |
| HIGH | `expression_GM.6mon.txt.gz` | 276201426 | 6 months GM8330 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_GM.6mon.txt.gz |
| HIGH | `expression_11a.6mon.txt.gz` | 365276898 | 6 months 11a | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_11a.6mon.txt.gz |
| HIGH | `expression_PGP1.6mon.Batch2.txt.gz` | 436326443 | 6 months PGP1 Batch 3 | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=expression_PGP1.6mon.Batch2.txt.gz |
| HIGH | `meta_combined.txt` |  | combined metadata; byte size unresolved in prior audit | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP282/download?filename=meta_combined.txt |

## SCP3301
- Study: https://singlecell.broadinstitute.org/single_cell/study/SCP3301
- Stage prefix: `gs://scperturb/pert-gym/staging/manual_scp/2026-06-23/SCP3301/`
| priority | filename | expected bytes | note | download URL |
|---|---:|---:|---|---|
| HIGH | `WTintegrated_addendum_metadata_convention.txt.gz` | 3190665 | MISSING sidecar; matrix/genes/barcodes already staged | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP3301/download?filename=WTintegrated_addendum_metadata_convention.txt.gz |
| HIGH | `WTintegrated_addendum_clustering.txt.gz` | 3375784 | MISSING sidecar; matrix/genes/barcodes already staged | https://singlecell.broadinstitute.org/single_cell/api/v1/site/studies/SCP3301/download?filename=WTintegrated_addendum_clustering.txt.gz |
