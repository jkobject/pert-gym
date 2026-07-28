#!/usr/bin/env python3
"""Build reviewable correction plans from cohort-A live audit evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "artifacts" / "first10_audit" / "cohort_a"


def decision(action: str, target: str | None, reason: str) -> dict[str, Any]:
    return {"action": action, "target": target, "reason": reason}


PLANS: dict[str, dict[str, Any]] = {
    "E-MTAB-9304": {
        "scientific_identity": {
            "title": "Single-cell RNA-seq of Drosophila embryos during dorsal-ventral patterning",
            "organism": "Drosophila melanogaster",
            "organism_ontology_term": "NCBITaxon:7227",
            "assay": "10x 3' v3 single-cell RNA-seq",
            "modality": "scRNA-seq",
            "source_accession": "E-MTAB-9304",
        },
        "source_evidence": [
            {
                "url": "https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-9304",
                "sha256": "169ba5b1673d2ed36b66b311b096f336783b0edd4a7f1b4491a065863e337925",
                "bytes": 24463,
                "supports": "title, organism, assay class, control and gd7/Toll mutant design",
            },
            {
                "url": "https://www.ebi.ac.uk/gxa/sc/experiment/E-MTAB-9304/download?fileType=experiment-design&accessKey=",
                "sha256": "2bc4db0fd6e7460841deb5a942dc6180edd1fbd8b75acf87e337f44ecad7db5c",
                "bytes": 31858845,
                "supports": "119362 cell rows, genotype/strain, constant 2.5-3.5 hour and stage 5, partial author/ontology cell labels",
            },
            {
                "url": "https://www.ebi.ac.uk/biostudies/files/E-MTAB-9304/E-MTAB-9304.sdrf.txt",
                "sha256": "ae47756c10e3512c65fb8b5e56c8a5adad4d72c60509664a7b77ce3afc1524c9",
                "bytes": 21122,
                "supports": "20 assay rows, source samples, 10x v3 protocol, age unit, genotype and FASTQ relations",
            },
        ],
        "temporal_verdict": {
            "verdict": "non_temporal_single_stage",
            "evidence": "All 119362 authoritative design rows are 2.5-3.5 hour, stage 5 embryo; no within-dataset time variation exists.",
            "action": "Remove cell-level timepoint/developmental-time fields; retain age and stage once in dataset metadata. Do not label developmental stage as a perturbation.",
        },
        "dataset_metadata": {
            "age_original": "2.5 to 3.5 hour",
            "developmental_stage": "stage 5 embryo",
            "organism_part": "whole embryo",
            "sex": "mixed",
            "source_title": "Single-cell RNA-seq of Drosophila embryos to investigate gene expression during dorsal-ventral patterning",
        },
        "obs_decisions": {
            "assay": decision("keep", "assay", "Inference-relevant assay constant."),
            "dataset": decision(
                "map", "dataset", "Use the stable logical dataset key."
            ),
            "design_factor_value_inferred_cell_type_authors_labels": decision(
                "map_preserve",
                "source_cell_type_author",
                "Author labels are source evidence for 16786 cells; missing rows stay unknown.",
            ),
            "design_factor_value_inferred_cell_type_ontology_labels": decision(
                "map_preserve",
                "cell_type",
                "Use source ontology labels as canonical text where present; do not fill absent cells.",
            ),
            "design_factor_value_ontology_term_inferred_cell_type_authors_labels": decision(
                "drop", None, "Entirely missing in the inspected payload."
            ),
            "design_factor_value_ontology_term_inferred_cell_type_ontology_labels": decision(
                "map_preserve",
                "cell_type_ontology_term",
                "Retain the source term only where present; absence remains unknown.",
            ),
            "design_factor_value_ontology_term_strain": decision(
                "drop", None, "Entirely missing."
            ),
            "design_factor_value_strain": decision(
                "map_preserve",
                "source_strain",
                "Variable experimental genotype/strain evidence; keep even where canonical genotype differs textually.",
            ),
            "design_ontology_term_age": decision(
                "drop", None, "Entirely missing and age is dataset-wide."
            ),
            "design_ontology_term_developmental_stage": decision(
                "drop", None, "Entirely missing and stage is dataset-wide."
            ),
            "design_ontology_term_genotype": decision(
                "drop", None, "Entirely missing; do not invent ontology mappings."
            ),
            "design_ontology_term_organism_part": decision(
                "drop", None, "Entirely missing and organism part is dataset-wide."
            ),
            "design_ontology_term_strain": decision("drop", None, "Entirely missing."),
            "design_sample_characteristic_age": decision(
                "move_to_dataset_metadata",
                "age_original",
                "Constant single stage; not a temporal observation axis.",
            ),
            "design_sample_characteristic_developmental_stage": decision(
                "move_to_dataset_metadata",
                "developmental_stage",
                "Constant single stage.",
            ),
            "design_sample_characteristic_genotype": decision(
                "map_preserve",
                "genotype",
                "Variable in the full source and required to distinguish control from maternal pathway mutants.",
            ),
            "design_sample_characteristic_organism_part": decision(
                "move_to_dataset_metadata", "organism_part", "Constant whole embryo."
            ),
            "design_sample_characteristic_strain": decision(
                "map_preserve",
                "source_strain",
                "Variable in the full source and useful conditioning/provenance.",
            ),
            "developmental_time_label": decision(
                "move_to_dataset_metadata",
                "age_original",
                "Constant and therefore not a temporal cell covariate.",
            ),
            "developmental_time_label_source": decision(
                "drop",
                None,
                "Internal harness lineage; source citation belongs in dataset provenance.",
            ),
            "modality": decision(
                "keep", "modality", "Inference-relevant modality constant."
            ),
            "organism": decision(
                "normalize_keep",
                "organism",
                "Normalize capitalization and retain as inference-relevant constant.",
            ),
            "perturbation": decision(
                "replace",
                "perturbation",
                "Current developmental_time value is biologically wrong; derive from genotype.",
            ),
            "perturbation_type": decision(
                "replace",
                "perturbation_type",
                "Use none for controls and maternal_genetic for gd7/Toll mutants.",
            ),
            "source_accession": decision(
                "move_to_dataset_metadata",
                "source_accession",
                "Dataset-wide provenance.",
            ),
            "source_experiment_design_path": decision(
                "drop",
                None,
                "Host-local non-portable path; replace with immutable URL and checksum.",
            ),
            "source_raw_zip_path": decision(
                "drop",
                None,
                "Host-local non-portable path; replace with immutable source/receipt identity.",
            ),
            "source_title": decision(
                "move_to_dataset_metadata", "source_title", "Dataset-wide description."
            ),
            "timepoint_source_hint": decision(
                "drop",
                None,
                "Vague harness hint contradicted by the single-stage source distribution.",
            ),
        },
        "derived_obs": {
            "sample": "Source sample prefix in Assay (SAMEA accession), validated against SDRF.",
            "cell_id": "Exact authoritative GXA Assay value.",
            "original_obs_index": "Exact authoritative GXA Assay value.",
            "obs_uuid": "pert_gym.obs_identity.add_obs_identity with canonical dataset/prefix.",
            "perturbation": "control for w[1118] and w[*]; P{EGFP-PCNA}attP2; otherwise exact maternal genotype gd7, Tollrm9/rm10, or Toll10B.",
            "perturbation_type": "none for controls; maternal_genetic for the three source-declared mutant-mother conditions.",
            "is_control": "True only for the two source control genotypes; False for gd7/Toll mutants.",
            "is_baseline": "not_applicable because this is not a time series.",
            "cell_type_state": "present where source label exists; unknown otherwise.",
        },
        "var_plan": {
            "verdict": "malformed_authoritative_row_pair_in_legacy_payload_and_canonical_payload_unavailable",
            "evidence": "All 16936 inspected legacy var indices contain one actual TAB, not '/t' or a literal backslash-t. The first field is nonblank and unique; both fields must be preserved before choosing the identifier.",
            "steps": [
                "Restore or reconstruct the 119362-row canonical generation before revision; all three accepted canonical payload URIs currently return no object.",
                "Parse the authoritative GXA mtx_rows record as two tab-delimited fields without stripping arbitrary characters.",
                "Set feature_id/index from field 1 only after asserting ^FBgn[0-9]+$ and uniqueness; preserve field 2 as source_gene_label.",
                "Require 16936 rows and exact X ordered-axis parity before publication.",
                "Set organism Drosophila melanogaster, feature_namespace FlyBase, and mapping_state source_exact; do not map through human/mouse Ensembl.",
            ],
        },
        "chunk_verdict": "One 119362x16936 CSR triplet is appropriate (accepted X was 189224969 bytes); the 1000-row chunk_0000 is a partial legacy fallback, not a complete dataset and must not enter the canonical Collection.",
        "remediation_steps": [
            "Fail closed on the live defect: accepted canonical obs/X/var records exist but all three GCS payloads are absent; the accepted manifest generation URI is also the required provenance anchor.",
            "Rebuild/restore from the authoritative GXA design and raw MatrixMarket source, preserving the recorded accepted axis hashes where parity is possible.",
            "Create revised obs/var/X only after full source row/feature parity; do not revise the 1000-row fallback into the canonical key.",
            "Apply the OBS and VAR decisions above, re-link obs->X->var, and validate payload existence by generation-pinned readback.",
        ],
        "residual_risks": [
            "The canonical payloads are missing and the accepted manifest was not discovered by the bounded Lamin query, so the full accepted canonical OBS schema and matrix cannot be read back in this audit.",
            "Only source-backed cell labels may be restored; 102576 source rows without labels remain unknown.",
        ],
    },
    "GSE130238": {
        "scientific_identity": {
            "title": "Complex oscillatory waves emerging from cortical organoids model early human brain network development",
            "organism": "Homo sapiens",
            "organism_ontology_term": "NCBITaxon:9606",
            "assay": "10x single-cell RNA-seq / Cell Ranger 2.1.1",
            "modality": "scRNA-seq",
            "source_accession": "GSE130238",
            "organoiddb_id": "ODD001111",
        },
        "source_evidence": [
            {
                "url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE130nnn/GSE130238/soft/GSE130238_family.soft.gz",
                "sha256": "a0cbbbe97e15b8667b89ccea0aadaa62433642cb5ddb789999a8efd3590b1ea6",
                "bytes": 2972,
                "supports": "four source samples at 1/3/6/10 months, WT-iPSC, passage 37, Cell Ranger 2.1.1, hg38 and raw feature-barcode matrices",
            },
            {
                "url": "https://pubmed.ncbi.nlm.nih.gov/31474560/",
                "doi": "10.1016/j.stem.2019.08.002",
                "supports": "controlling publication and cortical-organoid developmental interpretation",
            },
        ],
        "temporal_verdict": {
            "verdict": "temporal_month_resolved_single_trajectory",
            "evidence": "Live OBS and GEO both show 1/3/6/10 month cortical organoids with 4832/3158/4888/3208 cells.",
            "action": "Retain the varying source month as timepoint_original_value/unit and trajectory; derive canonical minutes only under a named project month-unit convention.",
        },
        "dataset_metadata": {
            "source_accession": "GSE130238",
            "organoiddb_id": "ODD001111",
            "reprogramming_strategy": "Retroviral",
            "source_matrix_semantics": "raw UMI count matrix",
            "x_semantics": "raw_counts",
            "source_name": "Cortical organoids",
        },
        "obs_decisions": {
            "assay": decision("keep", "assay", "Inference-relevant assay constant."),
            "development_stage": decision(
                "map_preserve",
                "timepoint_original_label",
                "Preserve exact 1/3/6/10 month source label.",
            ),
            "is_control": decision(
                "replace_and_preserve_as_baseline",
                "is_baseline",
                "Current field marks month 1 only and therefore encodes baseline, not perturbation control.",
            ),
            "organism": decision(
                "keep", "organism", "Inference-relevant organism constant."
            ),
            "organoiddb_id": decision(
                "move_to_dataset_metadata",
                "organoiddb_id",
                "Dataset-wide source identifier.",
            ),
            "reprogramming_strategy": decision(
                "move_to_dataset_metadata",
                "reprogramming_strategy",
                "Dataset-wide descriptive protocol field.",
            ),
            "sample_accession": decision(
                "map_keep", "sample", "Stable GEO sample join key."
            ),
            "sample_title": decision(
                "map_preserve",
                "source_sample_title",
                "Source-backed sample description.",
            ),
            "source_accession": decision(
                "move_to_dataset_metadata",
                "source_accession",
                "Dataset-wide provenance.",
            ),
            "source_cell_barcode": decision(
                "preserve",
                "source_cell_barcode",
                "Source barcode can repeat across samples and must be namespaced by sample.",
            ),
            "source_cell_line": decision(
                "map_keep",
                "cell_line",
                "Constant but inference-relevant WT-iPSC context.",
            ),
            "source_file": decision(
                "map_preserve", "source_file", "Sample-varying raw provenance."
            ),
            "source_matrix_semantics": decision(
                "move_to_dataset_metadata", "x_semantics", "Dataset-wide X semantics."
            ),
            "source_name": decision(
                "move_to_dataset_metadata", "source_name", "Dataset-wide description."
            ),
            "source_passage": decision(
                "map_keep",
                "passage",
                "Constant but potentially inference-relevant culture context.",
            ),
            "timepoint": decision(
                "rename_preserve",
                "timepoint_original_value",
                "Source unit is month, not canonical minutes.",
            ),
            "timepoint_unit": decision(
                "rename_preserve",
                "timepoint_original_unit",
                "Preserve exact month unit.",
            ),
            "tissue": decision(
                "map_keep", "tissue_type", "Inference-relevant organoid tissue context."
            ),
            "trajectory_id": decision(
                "keep",
                "trajectory_id",
                "One explicit cortical-organoid developmental trajectory.",
            ),
        },
        "derived_obs": {
            "cell_id": "sample accession + source barcode; assert global uniqueness.",
            "original_obs_index": "Exact current ordered OBS index before revision.",
            "obs_uuid": "pert_gym.obs_identity.add_obs_identity with canonical dataset/prefix.",
            "perturbation": "none",
            "perturbation_type": "none",
            "is_control": "True for all rows because there is no perturbation arm.",
            "is_baseline": "True only at 1 month; False at 3/6/10 months.",
            "timepoint": "Blocked until the project names a month-to-minute convention; never silently treat month integers as minutes.",
            "timepoint_state": "unknown pending unit convention; source month remains complete.",
        },
        "var_plan": {
            "verdict": "valid_source_ensembl_human_axis",
            "evidence": "33694 unique ENSG identifiers; zero tabs, control characters, whitespace defects or duplicates; exact X ordered-axis parity.",
            "steps": [
                "Retain the current ordered var and its human/hg38 source fields.",
                "Assert index == feature_id, feature_namespace Ensembl, organism Homo sapiens and exact X n_vars/order.",
                "Do not rewrite X or var for OBS-only remediation.",
            ],
        },
        "chunk_verdict": "One 16086x33694 CSR triplet (uint32, 69866889 bytes) is appropriate; no chunking or shared-var indirection is needed, and its X links to one exact same-prefix var.",
        "remediation_steps": [
            "Revise OBS only; preserve row order and existing X/var artifacts.",
            "Separate no-perturbation control semantics from earliest-time baseline semantics.",
            "Add global OBS identity, canonical sample/cell/tissue/cell-line fields and explicit source/state fields.",
            "Re-link revised obs to exact X lVUodgrG2F2izaVd0000 and verify X->var y0V1sLQ45pbtf6oS0000.",
        ],
        "residual_risks": [
            "No cell-type annotation is present in GEO/live OBS; it remains unknown unless a paper/source supplement provides a joinable per-cell mapping.",
            "Canonical minute conversion is blocked on a named month-unit convention; source month values are not missing.",
        ],
    },
    "GSE138002": {
        "scientific_identity": {
            "title": "Single-cell analysis of developing human retina and retinal organoids",
            "organism": "Homo sapiens",
            "organism_ontology_term": "NCBITaxon:9606",
            "assay": "10x Genomics Chromium v2 3' single-cell RNA-seq",
            "modality": "scRNA-seq",
            "source_accession": "GSE138002",
            "organoiddb_id": "ODD001099",
        },
        "source_evidence": [
            {
                "url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE138nnn/GSE138002/soft/GSE138002_family.soft.gz",
                "sha256": "3831efe73e940746c47084840571efc2d352ed905b388e0a6cfd07407916ce56",
                "bytes": 2623,
                "supports": "organoid days 24/30/42/59 plus gestational, postnatal and adult primary retina; hg38; finalized 118555-cell matrix",
            },
            {
                "url": "https://pubmed.ncbi.nlm.nih.gov/32386599/",
                "doi": "10.1016/j.devcel.2020.04.009",
                "supports": "controlling developmental-retina publication",
            },
            {
                "url": "https://www.ebi.ac.uk/ols4/ontologies/cl",
                "supports": "Cell Ontology mappings for eight unambiguous broad author labels; composite precursor labels remain unknown.",
            },
        ],
        "temporal_verdict": {
            "verdict": "temporal_mixed_trajectories_and_units",
            "evidence": "Live OBS contains retinal-organoid days, primary gestational weeks, postnatal days and adult rows; 11618 adults have no numeric age.",
            "action": "Create explicit trajectory/source-context fields and canonical minute values for day/week rows; preserve adult age as unknown rather than inventing a number.",
        },
        "dataset_metadata": {
            "source_accession": "GSE138002",
            "organoiddb_id": "ODD001099",
            "x_semantics": "raw_counts",
            "genome_build": "GRCh38",
        },
        "obs_decisions": {
            "age": decision(
                "map_preserve",
                "age",
                "Inference-relevant mixed developmental age label.",
            ),
            "assay": decision("keep", "assay", "Inference-relevant assay constant."),
            "cell_id": decision("keep", "cell_id", "Unique current cell identity."),
            "cell_type": decision(
                "map_preserve",
                "source_cell_type",
                "Preserve author label and map only unambiguous broad terms.",
            ),
            "dataset": decision("keep", "dataset", "Stable logical dataset key."),
            "disease": decision(
                "state_normalize",
                "disease",
                "All-null applicable field stays unknown; do not infer healthy from absence.",
            ),
            "donor_id": decision(
                "state_normalize",
                "donor_id",
                "All-null applicable field stays unknown.",
            ),
            "ethnicity": decision(
                "state_normalize",
                "ethnicity",
                "All-null applicable field stays unknown.",
            ),
            "is_control": decision(
                "replace",
                "is_control",
                "All rows are unperturbed developmental observations, not missing controls.",
            ),
            "is_low_quality": decision(
                "state_normalize",
                "is_low_quality",
                "No source-backed QC verdict; state is unknown.",
            ),
            "is_organoid": decision(
                "keep",
                "is_organoid",
                "Critical context separating organoid and primary retina.",
            ),
            "modality": decision(
                "keep", "modality", "Inference-relevant modality constant."
            ),
            "organism": decision(
                "keep", "organism", "Inference-relevant organism constant."
            ),
            "organoiddb_id": decision(
                "move_to_dataset_metadata", "organoiddb_id", "Dataset-wide provenance."
            ),
            "sample": decision("keep", "sample", "Sample/replicate grouping key."),
            "sex": decision(
                "state_normalize", "sex", "All-null applicable field stays unknown."
            ),
            "source_accession": decision(
                "move_to_dataset_metadata",
                "source_accession",
                "Dataset-wide provenance.",
            ),
            "source_age_label": decision(
                "preserve",
                "source_age_label",
                "Exact source age label and unit parser input.",
            ),
            "source_barcode": decision(
                "preserve", "source_barcode", "Exact source identity."
            ),
            "source_cell_type": decision(
                "preserve", "source_cell_type", "Exact author annotation."
            ),
            "source_num_genes_expressed": decision(
                "map_keep", "n_genes", "Per-cell source QC metric."
            ),
            "source_sample_type": decision(
                "map_preserve",
                "source_sample_type",
                "Separates whole retina, macula, periphery and organoid.",
            ),
            "source_total_mrnas": decision(
                "map_keep", "n_counts", "Per-cell source UMI/count metric."
            ),
            "technology": decision(
                "keep", "technology", "Inference-relevant technology constant."
            ),
            "timepoint": decision(
                "rename_preserve",
                "timepoint_original_value",
                "Current values use three different units and are not canonical minutes.",
            ),
            "timepoint_unit": decision(
                "rename_preserve",
                "timepoint_original_unit",
                "Preserve gestational_week/day/postnatal_day.",
            ),
            "tissue_type": decision(
                "keep", "tissue_type", "Inference-relevant retina context."
            ),
            "umap1_coord": decision(
                "type_auxiliary",
                "obsm_source_umap",
                "Embedding coordinate, not biological OBS metadata.",
            ),
            "umap2_coord": decision(
                "type_auxiliary",
                "obsm_source_umap",
                "Embedding coordinate, not biological OBS metadata.",
            ),
            "umap3_coord": decision(
                "type_auxiliary",
                "obsm_source_umap",
                "Embedding coordinate, not biological OBS metadata.",
            ),
        },
        "cell_type_ontology_map": {
            "RPCs": {
                "label": "retinal progenitor cell",
                "term": "CL:0002672",
                "status": "source_label_to_exact_broad_CL",
            },
            "Rods": {
                "label": "retinal rod cell",
                "term": "CL:0000604",
                "status": "source_label_to_exact_broad_CL",
            },
            "Amacrine Cells": {
                "label": "amacrine cell",
                "term": "CL:0000561",
                "status": "source_label_to_exact_broad_CL",
            },
            "Retinal Ganglion Cells": {
                "label": "retinal ganglion cell",
                "term": "CL:0000740",
                "status": "source_label_to_exact_broad_CL",
            },
            "Bipolar Cells": {
                "label": "retinal bipolar neuron",
                "term": "CL:0000748",
                "status": "source_label_to_exact_broad_CL",
            },
            "Cones": {
                "label": "retinal cone cell",
                "term": "CL:0000573",
                "status": "source_label_to_exact_broad_CL",
            },
            "Horizontal Cells": {
                "label": "retina horizontal cell",
                "term": "CL:0000745",
                "status": "source_label_to_exact_broad_CL",
            },
            "Muller Glia": {
                "label": "Mueller cell",
                "term": "CL:0000636",
                "status": "source_label_to_exact_broad_CL",
            },
            "Neurogenic Cells": {
                "label": "Neurogenic Cells",
                "term": None,
                "status": "unknown_no_exact_CL_mapping",
            },
            "BC/Photo_Precurs": {
                "label": "BC/Photo_Precurs",
                "term": None,
                "status": "unknown_composite_label",
            },
            "AC/HC_Precurs": {
                "label": "AC/HC_Precurs",
                "term": None,
                "status": "unknown_composite_label",
            },
        },
        "derived_obs": {
            "original_obs_index": "Exact current ordered OBS index before revision.",
            "obs_uuid": "pert_gym.obs_identity.add_obs_identity with canonical dataset/prefix.",
            "perturbation": "none",
            "perturbation_type": "none",
            "is_control": "True for every unperturbed row.",
            "trajectory_id": "organoid, fetal_primary_retina, postnatal_primary_retina, or adult_primary_retina from source context/unit.",
            "is_baseline": "Earliest observed point within each numeric trajectory; adult_primary_retina is not_applicable as a trajectory baseline.",
            "timepoint": "day/postnatal_day * 1440; gestational_week * 10080; adult without numeric age remains unknown.",
            "timepoint_state": "present for numeric day/week/postnatal rows; unknown for Adult rows.",
            "cell_type": "Mapped broad CL label where exact table exists; otherwise preserve source label with ontology state unknown.",
        },
        "var_plan": {
            "verdict": "valid_source_ensembl_human_axis",
            "evidence": "33694 unique ENSG identifiers; zero tabs, control characters, whitespace defects or duplicates; exact X ordered-axis parity.",
            "steps": [
                "Retain current ordered var and source gene_symbol/feature metadata.",
                "Assert index == feature_id, Ensembl namespace, Homo sapiens and exact X n_vars/order.",
                "Do not rewrite X or var for OBS/UMAP remediation.",
            ],
        },
        "chunk_verdict": "One 118555x33694 CSR int32 triplet (416010806 bytes) is appropriate and its X links to one exact same-prefix var; size alone does not justify new chunks. Move the 3D UMAP to typed obsm rather than changing X.",
        "remediation_steps": [
            "Revise OBS only and publish the source UMAP as typed obsm_source_umap; preserve exact X/var.",
            "Split temporal trajectories/units, derive minute-valued timepoint for numeric rows, and keep Adult age unknown.",
            "Apply exact broad CL mappings only; preserve three composite/unmatched labels with ontology state unknown.",
            "Add global OBS identity, no-perturbation semantics, QC aliases and explicit field state/source columns.",
            "Re-link revised obs to exact X GPge5BVaaXcCGgsU0000 and verify X->var jib8jqqYfSI3vzIW0000.",
        ],
        "residual_risks": [
            "Donor, sex, ethnicity, disease and per-cell quality verdicts are not source-backed in the current payload and remain unknown.",
            "Adult rows have no numeric age; a numeric temporal coordinate cannot be derived honestly.",
            "Three composite/precursor author labels have no exact one-term CL mapping in the bounded OLS check.",
        ],
    },
}


PROPOSED_SCHEMAS = {
    "E-MTAB-9304": [
        "obs_uuid",
        "original_obs_index",
        "dataset",
        "sample",
        "cell_id",
        "organism",
        "assay",
        "modality",
        "genotype",
        "source_strain",
        "cell_type",
        "cell_type_ontology_term",
        "source_cell_type_author",
        "perturbation",
        "perturbation_type",
        "is_control",
        "is_baseline",
    ],
    "GSE130238": [
        "obs_uuid",
        "original_obs_index",
        "dataset",
        "sample",
        "cell_id",
        "organism",
        "cell_line",
        "passage",
        "tissue_type",
        "assay",
        "modality",
        "trajectory_id",
        "timepoint",
        "timepoint_state",
        "timepoint_original_value",
        "timepoint_original_unit",
        "timepoint_original_label",
        "source_sample_title",
        "perturbation",
        "perturbation_type",
        "is_control",
        "is_baseline",
        "source_cell_barcode",
        "source_file",
    ],
    "GSE138002": [
        "obs_uuid",
        "original_obs_index",
        "dataset",
        "sample",
        "cell_id",
        "organism",
        "tissue_type",
        "is_organoid",
        "source_sample_type",
        "assay",
        "technology",
        "modality",
        "age",
        "source_age_label",
        "trajectory_id",
        "timepoint",
        "timepoint_state",
        "timepoint_original_value",
        "timepoint_original_unit",
        "cell_type",
        "cell_type_ontology_term",
        "cell_type_ontology_state",
        "source_cell_type",
        "perturbation",
        "perturbation_type",
        "is_control",
        "is_baseline",
        "n_counts",
        "n_genes",
        "is_low_quality",
        "is_low_quality_state",
        "donor_id",
        "donor_id_state",
        "sex",
        "sex_state",
        "ethnicity",
        "ethnicity_state",
        "disease",
        "disease_state",
        "source_barcode",
    ],
}


def build_report(dataset: str, live: dict[str, Any], live_file: Path) -> dict[str, Any]:
    plan = PLANS[dataset]
    current_columns = set(live["obs"]["columns"])
    decided_columns = set(plan["obs_decisions"])
    if current_columns != decided_columns:
        raise ValueError(
            f"{dataset} OBS decisions mismatch: missing={sorted(current_columns - decided_columns)}, "
            f"extra={sorted(decided_columns - current_columns)}"
        )
    return {
        "format": "pert-gym.first10-cohort-a-correction-plan/v1",
        "task_id": "t_2122b5f4",
        "dataset": dataset,
        "read_only": True,
        "live_evidence_file": str(live_file.relative_to(ROOT)),
        "live_audited_at": live["audited_at"],
        "instance": live["instance"],
        "branch": live["branch"],
        "scientific_identity": plan["scientific_identity"],
        "source_evidence": plan["source_evidence"],
        "raw_provenance": {
            "confidence": "high"
            if live["manifests"]
            else "source_high_live_payload_broken",
            "manifests": live["manifests"],
            "related_lamin_artifacts": live["related_lamin_artifacts"],
        },
        "current_identity": {
            "canonical_prefix": live["canonical_prefix"],
            "canonical_artifacts": live["canonical_artifacts"],
            "payload_inspection": live["payload_inspection"],
        },
        "current_obs_inventory": live["obs"],
        "obs_column_decisions": plan["obs_decisions"],
        "dataset_metadata_proposal": plan["dataset_metadata"],
        "derived_obs_proposal": plan["derived_obs"],
        "proposed_post_fix_obs_schema": PROPOSED_SCHEMAS[dataset],
        "temporal_verdict": plan["temporal_verdict"],
        "current_var_inventory": live["var"],
        "var_plan": plan["var_plan"],
        "current_X": live["X"],
        "triplet_validation": live["triplet_validation"],
        "chunk_verdict": plan["chunk_verdict"],
        "cell_type_ontology_map": plan.get("cell_type_ontology_map"),
        "remediation_steps": plan["remediation_steps"],
        "validators": [
            "Every current OBS column has exactly one keep/drop/map/derive decision.",
            "OBS row count/order and X obs index hash are unchanged by OBS-only revisions.",
            "VAR row count/order equals X var axis; identifiers are unique and control-character-free.",
            "obs->X and X->var resolve to exact intended keys/UIDs and every payload exists by fresh readback.",
            "The X artifact links to one exact same-prefix var.parquet.",
            "Missing applicable values use unknown; genuinely inapplicable values use not_applicable.",
            "Global obs identity validates and is unique.",
        ],
        "residual_risks": plan["residual_risks"],
    }


def render_markdown(report: dict[str, Any]) -> str:
    dataset = report["dataset"]
    identity = report["current_identity"]
    lines = [
        f"# {dataset} — read-only scientific audit and correction plan",
        "",
        f"Live audit: `{report['live_evidence_file']}` at {report['live_audited_at']}",
        f"Canonical prefix: `{identity['canonical_prefix']}`",
        "",
        "## Verdict",
        "",
        f"- Temporal: **{report['temporal_verdict']['verdict']}** — {report['temporal_verdict']['action']}",
        f"- Chunking: {report['chunk_verdict']}",
        f"- VAR: **{report['var_plan']['verdict']}**",
        "",
        "## Current triplet",
        "",
    ]
    for role in ("obs", "X", "var"):
        artifact = identity["canonical_artifacts"][role]
        payload = artifact["payload_evidence"]
        lines.append(
            f"- {role}: `{artifact['key']}` / `{artifact['uid']}`; "
            f"payload exists={payload.get('path_exists')}; "
            f"SHA-256=`{payload.get('downloaded_sha256') or 'unavailable'}`"
        )
    lines += [
        "",
        "## Main corrections",
        "",
        *[
            f"{index}. {step}"
            for index, step in enumerate(report["remediation_steps"], 1)
        ],
        "",
        "## OBS decisions",
        "",
        "| Current column | Action | Target | Reason |",
        "|---|---|---|---|",
    ]
    for column, item in report["obs_column_decisions"].items():
        lines.append(
            f"| `{column}` | `{item['action']}` | `{item['target'] or ''}` | {item['reason']} |"
        )
    lines += [
        "",
        "## Proposed OBS schema",
        "",
        ", ".join(f"`{column}`" for column in report["proposed_post_fix_obs_schema"]),
        "",
        "## Residual risks",
        "",
        *[f"- {risk}" for risk in report["residual_risks"]],
        "",
        "No LaminDB, GCS, or Collection mutation was performed by this audit.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_DIR)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for dataset in PLANS:
        live_file = args.input_dir / f"{dataset}.audit.json"
        live = json.loads(live_file.read_text(encoding="utf-8"))
        report = build_report(dataset, live, live_file)
        (args.output_dir / f"{dataset}.report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (args.output_dir / f"{dataset}.report.md").write_text(
            render_markdown(report), encoding="utf-8"
        )


if __name__ == "__main__":
    main()
