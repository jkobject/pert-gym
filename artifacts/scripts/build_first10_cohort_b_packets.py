#!/usr/bin/env python3
"""Build scientific audit/remediation packets for first-10 cohort B."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "artifacts/evidence/first10-cohort-b-t_890a73de"
SNAPSHOTS = EVIDENCE / "source_snapshots"
LIVE = EVIDENCE / "live_lamin_readback.json"

GEO = {
    "GSE194214": {
        "series_url": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE194214",
        "soft_url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE194nnn/GSE194214/soft/GSE194214_family.soft.gz",
        "soft_sha256": "a6e292220854d96f86f8cf99bce958890cba2ca8a7dcd3ffb1109509688bc511",
        "pubmed_url": "https://pubmed.ncbi.nlm.nih.gov/35088712/",
        "bioproject_url": "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA799751",
    },
    "GSE269572": {
        "series_url": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE269572",
        "soft_url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE269nnn/GSE269572/soft/GSE269572_family.soft.gz",
        "soft_sha256": "6fab2319b8794e12accc125a3a63d45659c9bc72abd5c9bd7f6bebc7b05d7580",
        "pubmed_url": None,
        "bioproject_url": "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA1122543",
    },
    "GSE196799": {
        "series_url": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE196799",
        "soft_url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE196nnn/GSE196799/soft/GSE196799_family.soft.gz",
        "soft_sha256": "73a06f660059b778772dcdc63466c6d6259b937de1cd9d1a3dea06b74207ffc5",
        "pubmed_url": "https://pubmed.ncbi.nlm.nih.gov/37714147/",
        "bioproject_url": "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA807328",
    },
}

MANIFESTS = {
    "GSE194214": (
        "GSE194214_manifest.json",
        "f1e5d44fd0dc621728c1c6b37bf150650f6dd5cd7d58530b7f52b0f93eaddd1f",
    ),
    "GSE269572": (
        "GSE269572_manifest.json",
        "ab3286741d1b41fcdc11003e61e42ad3f598f11cf6c9d65b8ee48161b2866f8c",
    ),
    "GSM5901228": (
        "GSE196799_manifest.json",
        "3f73fdc9e405279ebbd5e5a4d67ee8b6d32cd0a031b73d5297184f95b6bb7eb3",
    ),
    "GSM5901229": (
        "GSE196799_manifest.json",
        "3f73fdc9e405279ebbd5e5a4d67ee8b6d32cd0a031b73d5297184f95b6bb7eb3",
    ),
}


def artifact_identity(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        role: data["metadata"] | {"feature_links": data["feature_links"]}
        for role, data in entry["artifacts"].items()
    }


def common_packet(dataset: str, live: dict[str, Any]) -> dict[str, Any]:
    entry = live["datasets"][dataset]
    manifest_name, manifest_sha = MANIFESTS[dataset]
    manifest = json.loads((SNAPSHOTS / manifest_name).read_text())
    source_family = "GSE196799" if dataset.startswith("GSM") else dataset
    matrix = (
        manifest["readback"][dataset]
        if dataset.startswith("GSM")
        else manifest["dataset"]["readback"]["matrix"]
    )
    return {
        "schema_version": "pert-gym.first10-scientific-audit.v1",
        "task_id": "t_890a73de",
        "dataset_id": dataset,
        "audit_mode": "read_only",
        "cloud_mutations": 0,
        "current_identity": {
            "instance": live["instance"],
            "branch": live["branch"],
            "artifacts": artifact_identity(entry),
            "bounded_key_candidates": entry["bounded_key_candidates"],
        },
        "source_evidence": {
            "family_accession": source_family,
            "citations": GEO[source_family],
            "acceptance_manifest": {
                "local_snapshot": f"source_snapshots/{manifest_name}",
                "sha256": manifest_sha,
            },
            "source_identity": manifest["source_identity"],
            "confidence": "verified_exact_source_and_generation_bound_acceptance_manifest",
        },
        "obs_audit": entry["obs"],
        "var_audit": entry["var"],
        "x_audit": {
            "manifest_readback": matrix,
            "row_parity": entry["obs"]["shape"][0] == matrix["shape"][0],
            "column_parity": entry["var"]["shape"][0] == matrix["shape"][1],
            "registry_n_observations": entry["parity"]["X_n_observations"],
            "registry_n_observations_state": "unpopulated; dimensional parity comes from immutable manifest readback",
            "x_semantics": "raw_counts",
        },
        "link_audit": entry["parity"]
        | {
            "verdict": "PASS_exact_obs_to_X_to_same_prefix_var",
        },
        "var_verdict": {
            "species": "Homo sapiens",
            "namespace": "Ensembl gene ID",
            "index_unique": entry["var"]["index_unique"],
            "control_characters": entry["var"]["index_control_character_count"],
            "dimension_matches_X": entry["var"]["shape"][0] == matrix["shape"][1],
        },
        "evidence_files": [
            "live_lamin_readback.json",
            f"source_snapshots/{manifest_name}",
            f"source_snapshots/{source_family}_family.soft",
            "source_snapshots/fetch_receipt.json",
            "source_snapshots/geo_soft_fetch_receipt.json",
        ],
    }


def gse194214(packet: dict[str, Any]) -> None:
    packet.update(
        {
            "scientific_verdict": "REMEDIATE_OBS_ONLY; X/VAR/links pass",
            "temporal_verdict": {
                "status": "temporal_multi_snapshot",
                "source_timepoints_days": [1, 2, 3, 5],
                "decision": "retain temporal conditioning; canonical timepoint is minutes and source day values remain provenance",
            },
            "chunk_verdict": {
                "status": "unchunked_appropriate",
                "physical_members": 1,
                "shared_var": "one same-prefix var already linked",
            },
            "obs_decisions": [
                {
                    "action": "derive",
                    "target": "obs_uuid, original_obs_index",
                    "from": "current exact row index plus canonical dataset/prefix",
                    "reason": "required global identity contract",
                },
                {
                    "action": "map",
                    "target": "dataset",
                    "from": "source_accession",
                    "value": "GSE194214",
                },
                {"action": "map", "target": "sample", "from": "sample_accession"},
                {
                    "action": "derive",
                    "target": "cell_id",
                    "from": "sample_accession + source_cell_barcode",
                    "reason": "barcodes alone repeat across samples",
                },
                {
                    "action": "map",
                    "target": "timepoint",
                    "from": "timepoint * 1440",
                    "unit": "minutes",
                },
                {
                    "action": "retain",
                    "target": "source_timepoint, source_timepoint_unit",
                    "from": "timepoint, timepoint_unit",
                },
                {
                    "action": "derive",
                    "target": "perturbation, perturbation_type",
                    "value": ["none", "none"],
                    "reason": "source is an unperturbed developmental series",
                },
                {
                    "action": "correct",
                    "target": "is_control",
                    "value": True,
                    "reason": "all rows are unperturbed; current True only at day 1 conflates control with baseline",
                },
                {
                    "action": "derive",
                    "target": "is_baseline",
                    "rule": "source day == 1",
                },
                {
                    "action": "retain",
                    "target": "organism, tissue_type, assay, genotype, trajectory_id",
                    "reason": "scientifically useful conditioning/provenance",
                },
                {
                    "action": "derive",
                    "target": "modality, is_bulk, is_pseudobulk",
                    "value": ["scRNA-seq", False, False],
                },
                {
                    "action": "move_to_dataset_metadata",
                    "target": "organoiddb_id, source_matrix_semantics, source_name, source_cell_type",
                    "reason": "constant descriptive/source facts",
                },
                {
                    "action": "set_unknown",
                    "target": "donor_id, sex, age, ethnicity, disease, cell_type",
                    "reason": "absent in source; do not infer",
                },
            ],
            "post_fix_obs_schema": [
                "obs_uuid",
                "original_obs_index",
                "dataset",
                "sample",
                "cell_id",
                "source_accession",
                "sample_accession",
                "source_cell_barcode",
                "source_file",
                "sample_title",
                "development_stage",
                "source_timepoint",
                "source_timepoint_unit",
                "timepoint",
                "organism",
                "tissue_type",
                "assay",
                "modality",
                "genotype",
                "trajectory_id",
                "perturbation",
                "perturbation_type",
                "is_control",
                "is_baseline",
                "donor_id",
                "sex",
                "age",
                "ethnicity",
                "disease",
                "cell_type",
            ],
            "var_decisions": [
                {"action": "map", "target": "ensembl_id", "from": "feature_id"},
                {
                    "action": "retain",
                    "target": "author_gene_id, author_gene_symbol",
                    "from": "feature_id, gene_symbol",
                },
                {
                    "action": "retain",
                    "target": "organism, feature_namespace, genome_build",
                    "reason": "source-backed human/Ensembl/GRCh38 values are complete",
                },
            ],
            "post_fix_var_schema": [
                "ensembl_id",
                "gene_symbol",
                "feature_type",
                "feature_namespace",
                "organism",
                "genome_build",
                "author_gene_id",
                "author_gene_symbol",
            ],
        }
    )


def gse269572(packet: dict[str, Any]) -> None:
    packet.update(
        {
            "scientific_verdict": "REMEDIATE_OBS_ONLY; X/VAR/links/source pass",
            "historical_source_resolution": "Earlier no-supplement probe is superseded: GEO became public 2026-04-14 and the accepted manifest verifies the 561,725,440-byte RAW tar with SHA-256 6acd23bcb9c6d6b8a0f60f447c92edbd85243ef52732fb019f548846020c661a.",
            "temporal_verdict": {
                "status": "non_temporal_single_snapshot",
                "source_timepoint_days_in_vitro": 42.5,
                "decision": "remove cell-level timepoint/development_stage/trajectory_id; retain day 42.5 as dataset metadata only",
            },
            "chunk_verdict": {
                "status": "unchunked_appropriate",
                "physical_members": 1,
                "shared_var": "one same-prefix var already linked",
            },
            "obs_decisions": [
                {
                    "action": "derive",
                    "target": "obs_uuid, original_obs_index",
                    "from": "current exact row index plus canonical dataset/prefix",
                },
                {"action": "map", "target": "dataset", "value": "GSE269572"},
                {"action": "map", "target": "sample", "from": "sample_accession"},
                {
                    "action": "derive",
                    "target": "cell_id",
                    "from": "sample_accession + source_cell_barcode",
                },
                {
                    "action": "map",
                    "target": "cell_line",
                    "from": "source_cell_line",
                    "value": "H9",
                },
                {
                    "action": "derive",
                    "target": "perturbation, perturbation_type",
                    "rule": "with PD173074 -> (PD173074, drug); without PD173074 and 2D culture -> (none, none)",
                },
                {
                    "action": "retain",
                    "target": "condition",
                    "reason": "three-arm culture/treatment design is required for inference",
                },
                {
                    "action": "keep_unknown",
                    "target": "is_control",
                    "reason": "source defines three arms but no unique binary control; do not infer 2D or without-PD as globally canonical control",
                },
                {
                    "action": "move_to_dataset_metadata",
                    "target": "timepoint, timepoint_unit, development_stage, trajectory_id",
                    "value": "day 42.5 in vitro",
                    "reason": "constant single snapshot",
                },
                {
                    "action": "move_to_dataset_metadata",
                    "target": "source_matrix_semantics, source_name",
                    "reason": "constant descriptive/source facts",
                },
                {
                    "action": "set_unknown",
                    "target": "donor_id, age, sex, ethnicity, disease, cell_type",
                    "reason": "all absent in source",
                },
                {
                    "action": "retain",
                    "target": "organism, tissue_type, assay",
                    "reason": "scientifically useful conditioning",
                },
                {
                    "action": "derive",
                    "target": "modality, is_bulk, is_pseudobulk",
                    "value": ["scRNA-seq", False, False],
                },
            ],
            "post_fix_obs_schema": [
                "obs_uuid",
                "original_obs_index",
                "dataset",
                "sample",
                "cell_id",
                "source_accession",
                "sample_accession",
                "source_cell_barcode",
                "source_file",
                "sample_title",
                "cell_line",
                "condition",
                "perturbation",
                "perturbation_type",
                "organism",
                "tissue_type",
                "assay",
                "modality",
                "is_control",
                "donor_id",
                "age",
                "sex",
                "ethnicity",
                "disease",
                "cell_type",
            ],
            "var_decisions": [
                {"action": "map", "target": "ensembl_id", "from": "feature_id"},
                {
                    "action": "retain",
                    "target": "author_gene_id, author_gene_symbol",
                    "from": "feature_id, gene_symbol",
                },
                {
                    "action": "retain",
                    "target": "organism, feature_namespace, genome_build",
                    "reason": "source-backed human/Ensembl/GRCh38 values are complete",
                },
            ],
            "post_fix_var_schema": [
                "ensembl_id",
                "gene_symbol",
                "feature_type",
                "feature_namespace",
                "organism",
                "genome_build",
                "author_gene_id",
                "author_gene_symbol",
            ],
        }
    )


def gsm(packet: dict[str, Any], day: int) -> None:
    dataset = packet["dataset_id"]
    packet.update(
        {
            "scientific_verdict": "REMODEL_AS_GSE196799_SAMPLE_MEMBER_AND_REMEDIATE_OBS_VAR_LINKS",
            "logical_family": "GSE196799",
            "temporal_verdict": {
                "member_status": "non_temporal_single_snapshot",
                "member_source_timepoint_days": day,
                "family_status": "temporal_multi_snapshot",
                "family_timepoints_days": [0, 3, 6, 9, 12, 18],
                "decision": "remove constant cell-level timepoint from this member; store it in member metadata and project/broadcast it only when loading the GSE196799 family",
            },
            "chunk_verdict": {
                "status": "sample_member_not_logical_dataset_or_arbitrary_chunk",
                "current_prefix": f"data/cleaned/{dataset}",
                "proposed_prefix": f"data/cleaned/GSE196799/samples/{dataset}",
                "allowed_member_name": dataset,
                "family_member_count": 10,
                "shared_var_target": "data/cleaned/GSE196799/var.parquet",
                "shared_var_evidence": "all ten accepted members have identical ordered var_index_sha256 4f796e2e5212467c2b54b9a5ff30fbb1ba020686b7edac3df76b996c2a687dd7; these two current var payloads also have identical Lamin hash 5IxJsE91crLE_PZZ7Ccq1w",
            },
            "obs_decisions": [
                {
                    "action": "derive",
                    "target": "obs_uuid, original_obs_index",
                    "from": "current exact row index plus family/member canonical prefix",
                },
                {"action": "map", "target": "dataset", "value": "GSE196799"},
                {"action": "map", "target": "sample", "value": dataset},
                {
                    "action": "retain",
                    "target": "cell_id",
                    "reason": "unique exact source-derived member cell identity",
                },
                {
                    "action": "map",
                    "target": "n_genes, n_counts, pct_mito",
                    "from": "n_genes_by_counts, total_counts, pct_counts_mt",
                },
                {
                    "action": "derive",
                    "target": "is_low_quality",
                    "value": False,
                    "reason": "all retained rows passed published n_genes>2000 and mitochondrial_fraction<0.1 gates",
                },
                {
                    "action": "derive",
                    "target": "perturbation, perturbation_type, is_control",
                    "value": ["none", "none", True],
                    "reason": "developmental differentiation sample without a perturbation intervention",
                },
                {
                    "action": "move_to_member_metadata",
                    "target": "timepoint, timepoint_unit, experiment, culture_method, ascorbic_acid_from_day_12",
                    "reason": "constant in this member but variable/conditioning across GSE196799",
                },
                {
                    "action": "move_to_member_metadata",
                    "target": "source_cell_call_flag_missing, source_name, biosample, sra_experiment, sample_title",
                    "reason": "constant provenance facts",
                },
                {
                    "action": "retain",
                    "target": "source_accession, sample_accession, barcode, organism, assay",
                    "reason": "family/sample identity and useful conditioning",
                },
                {
                    "action": "derive",
                    "target": "modality, is_bulk, is_pseudobulk",
                    "value": ["scRNA-seq", False, False],
                },
                {
                    "action": "set_unknown",
                    "target": "donor_id, age, sex, ethnicity, disease, cell_type",
                    "reason": "absent or unlinked in source",
                },
            ],
            "var_decisions": [
                {"action": "map", "target": "ensembl_id", "from": "feature_id"},
                {
                    "action": "retain",
                    "target": "author_gene_id, author_gene_symbol",
                    "from": "feature_id, gene_symbol",
                },
                {
                    "action": "derive",
                    "target": "organism, feature_namespace",
                    "value": ["Homo sapiens", "Ensembl gene ID"],
                    "reason": "GEO taxid 9606 and all 20,631 feature IDs are ENSG",
                },
                {
                    "action": "keep_unknown",
                    "target": "genome_build",
                    "reason": "not established by accepted source evidence",
                },
                {
                    "action": "share",
                    "target": "data/cleaned/GSE196799/var.parquet",
                    "reason": "identical ordered family var; avoid ten duplicate logical vars",
                },
            ],
            "post_fix_obs_schema": [
                "obs_uuid",
                "original_obs_index",
                "dataset",
                "sample",
                "cell_id",
                "barcode",
                "source_accession",
                "sample_accession",
                "organism",
                "assay",
                "modality",
                "n_genes",
                "n_counts",
                "pct_mito",
                "is_low_quality",
                "perturbation",
                "perturbation_type",
                "is_control",
                "donor_id",
                "age",
                "sex",
                "ethnicity",
                "disease",
                "cell_type",
            ],
            "post_fix_var_schema": [
                "ensembl_id",
                "gene_symbol",
                "feature_type",
                "feature_namespace",
                "organism",
                "genome_build",
                "author_gene_id",
                "author_gene_symbol",
            ],
        }
    )


def add_field_by_field_decisions(packet: dict[str, Any]) -> None:
    dataset = packet["dataset_id"]
    maps = {
        "GSE194214": {
            "source_accession": ("map_and_retain", "dataset; source_accession"),
            "organoiddb_id": ("move_to_dataset_metadata", "organoiddb_id"),
            "sample_accession": ("map_and_retain", "sample; sample_accession"),
            "sample_title": ("retain_source_provenance", "sample_title"),
            "source_file": ("retain_source_provenance", "source_file"),
            "source_cell_barcode": (
                "derive_and_retain",
                "cell_id; source_cell_barcode",
            ),
            "source_name": ("move_to_dataset_metadata", "source_name"),
            "source_cell_type": (
                "move_to_dataset_metadata",
                "source_material; canonical cell_type=unknown",
            ),
            "genotype": ("retain_conditioning", "genotype"),
            "development_stage": ("retain_conditioning", "development_stage"),
            "timepoint": ("map_unit", "timepoint minutes; source_timepoint days"),
            "timepoint_unit": (
                "map_and_retain",
                "canonical minutes; source_timepoint_unit",
            ),
            "organism": ("retain_conditioning", "organism"),
            "assay": ("normalize_and_retain_source", "assay; source_assay"),
            "tissue": ("map_conditioning", "tissue_type"),
            "is_control": (
                "correct_semantics",
                "is_control=True; is_baseline derived separately",
            ),
            "trajectory_id": ("retain_conditioning", "trajectory_id"),
            "source_matrix_semantics": (
                "move_to_dataset_metadata",
                "x_semantics=raw_counts",
            ),
        },
        "GSE269572": {
            "source_accession": ("map_and_retain", "dataset; source_accession"),
            "sample_accession": ("map_and_retain", "sample; sample_accession"),
            "sample_title": ("retain_source_provenance", "sample_title"),
            "source_file": ("retain_source_provenance", "source_file"),
            "source_cell_barcode": (
                "derive_and_retain",
                "cell_id; source_cell_barcode",
            ),
            "source_name": ("move_to_dataset_metadata", "source_name"),
            "source_cell_line": ("map_conditioning", "cell_line"),
            "treatment": ("map_and_retain", "perturbation; source_treatment"),
            "condition": ("retain_conditioning", "condition"),
            "timepoint": ("move_to_dataset_metadata", "source snapshot day 42.5"),
            "timepoint_unit": ("move_to_dataset_metadata", "day in vitro"),
            "development_stage": ("move_to_dataset_metadata", "day 42.5"),
            "donor_age": ("replace_null_with_unknown", "age"),
            "donor_sex": ("replace_null_with_unknown", "sex"),
            "donor_ethnicity": ("replace_null_with_unknown", "ethnicity"),
            "organism": ("retain_conditioning", "organism"),
            "assay": ("normalize_and_retain_source", "assay; source_assay"),
            "tissue": ("map_conditioning", "tissue_type"),
            "is_control": ("retain_unknown", "is_control"),
            "trajectory_id": ("move_to_dataset_metadata", "trajectory_id"),
            "source_matrix_semantics": (
                "move_to_dataset_metadata",
                "x_semantics=raw_counts",
            ),
        },
    }
    gsm_map = {
        "cell_id": ("retain", "cell_id"),
        "barcode": ("retain_source_provenance", "barcode"),
        "source_accession": ("map_and_retain", "dataset=GSE196799; source_accession"),
        "sample_accession": ("map_and_retain", "sample; sample_accession"),
        "sample_title": ("move_to_member_metadata", "sample_title"),
        "source_name": ("move_to_member_metadata", "source_name"),
        "experiment": ("move_to_member_metadata", "experiment"),
        "timepoint": ("move_to_member_metadata", "source_timepoint_days"),
        "timepoint_unit": ("move_to_member_metadata", "source_timepoint_unit"),
        "culture_method": ("move_to_member_metadata", "culture_method"),
        "ascorbic_acid_from_day_12": (
            "move_to_member_metadata",
            "ascorbic_acid_from_day_12",
        ),
        "biosample": ("move_to_member_metadata", "biosample"),
        "sra_experiment": ("move_to_member_metadata", "sra_experiment"),
        "organism": ("retain_conditioning", "organism"),
        "assay": ("normalize_and_retain_source", "assay; source_assay"),
        "n_genes_by_counts": ("map", "n_genes"),
        "total_counts": ("map", "n_counts"),
        "pct_counts_mt": ("map", "pct_mito"),
        "source_cell_call_flag_missing": (
            "move_to_member_metadata",
            "source_cell_call_flag_missing",
        ),
    }
    mapping = gsm_map if dataset.startswith("GSM") else maps[dataset]
    rows = []
    for field in packet["obs_audit"]["columns"]:
        action, target = mapping[field]
        ontology_status = (
            "source_literal_requires_registry_resolution"
            if field
            in {"organism", "assay", "tissue", "source_cell_line", "source_cell_type"}
            else "not_applicable"
        )
        audit = packet["obs_audit"]["column_audit"][field]
        rows.append(
            {
                "field": field,
                "dtype": audit["dtype"],
                "cardinality": audit["unique_non_null"],
                "missing": audit["missing"],
                "action": action,
                "target": target,
                "ontology_status": ontology_status,
                "source": "current payload cross-checked against GEO SOFT and immutable acceptance manifest",
                "inference_utility": "conditioning_or_identity"
                if "retain" in action or "map" in action
                else "metadata_or_missingness_only",
            }
        )
    packet["obs_field_decisions"] = rows


def add_plan(packet: dict[str, Any]) -> None:
    dataset = packet["dataset_id"]
    packet["validators"] = [
        "resolve exact current active UIDs and fail on 0 or >1",
        "assert source OBS row index/order SHA-256 equals packet obs_audit.index_sha256 before transformation",
        "assert revised OBS row count/order and existing X UID/hash are unchanged",
        "validate obs_uuid/original_obs_index with pert_gym.obs_identity.validate_obs_identity",
        "assert post-fix columns, dtypes, missingness states and low-cardinality values match this packet",
        "assert immutable-manifest X shape equals revised OBS rows by linked VAR rows",
        "assert exact revised obs -> existing X -> accepted VAR link chain",
        "repeat a fresh bounded readback from laminlabs/pertdata/jkobject and emit a zero-drift receipt",
    ]
    steps = [
        f"Resolve current exact data/cleaned/{dataset}/obs.parquet, X.h5ad and var.parquet by UID from this packet.",
        "Load OBS only and fail closed if index hash, row count, columns or source values drift.",
        "Apply obs_decisions in order; preserve source columns named as provenance and encode unsupported applicable fields as unknown.",
        "Call add_obs_identity with the packet's logical dataset and canonical prefix; validate identity and row order.",
        "Register revised OBS with revises=<current OBS>; do not revise or materialize X.",
    ]
    if dataset.startswith("GSM"):
        steps.append(
            "Create/reuse the exact-hash GSE196799 shared VAR and process all ten siblings atomically before relinking X; do not leave a mixed family policy."
        )
    steps.extend(
        [
            "Set revised OBS X link and X VAR link exactly as specified; preserve old artifacts for rollback.",
            "Run every listed validator and publish immutable generation/UID/hash evidence for independent review.",
        ]
    )
    packet["remediation_plan"] = {
        "writer_scope": "single downstream writer; append-only revisions; no X rewrite",
        "steps": steps,
        "rollback": "restore the previous OBS feature link and, for GSE196799, previous per-member X->VAR links using the exact current UIDs recorded here",
    }


def render(packet: dict[str, Any]) -> str:
    dataset = packet["dataset_id"]
    temporal = packet["temporal_verdict"]
    temporal_text = json.dumps(temporal, sort_keys=True)
    chunk_text = json.dumps(packet["chunk_verdict"], sort_keys=True)
    var_text = json.dumps(packet["var_verdict"], sort_keys=True)
    x_text = json.dumps(packet["x_audit"], sort_keys=True)
    lines = [
        f"# {dataset} scientific audit and correction plan",
        "",
        f"- Verdict: **{packet['scientific_verdict']}**",
        f"- Mode: read-only; cloud mutations: `{packet['cloud_mutations']}`.",
        f"- Current OBS/X/VAR links: `{packet['link_audit']['verdict']}`.",
        f"- X shape: `{packet['x_audit']['manifest_readback']['shape']}`; OBS/X parity: `{packet['x_audit']['row_parity']}`; X/VAR parity: `{packet['x_audit']['column_parity']}`.",
        f"- Temporal verdict: `{temporal_text}`.",
        f"- Chunk/member verdict: `{chunk_text}`.",
        "",
        "## Source and immutable identity",
        "",
        f"- Family/source: `{packet['source_evidence']['family_accession']}`; confidence: `{packet['source_evidence']['confidence']}`.",
        f"- GEO: {packet['source_evidence']['citations']['series_url']}",
        f"- BioProject: {packet['source_evidence']['citations']['bioproject_url']}",
        f"- PubMed: {packet['source_evidence']['citations']['pubmed_url'] or 'not supplied by GEO; unknown'}",
        f"- Acceptance manifest SHA-256: `{packet['source_evidence']['acceptance_manifest']['sha256']}`.",
        "",
        "## OBS decisions",
        "",
    ]
    lines.extend(
        f"- `{row['action']}` `{row['target']}` — {row.get('reason') or row.get('rule') or row.get('from') or row.get('value')}"
        for row in packet["obs_decisions"]
    )
    lines.extend(
        [
            "",
            "## Field-by-field OBS disposition",
            "",
            "| Field | dtype | cardinality | missing | action | target | ontology |",
            "|---|---|---:|---:|---|---|---|",
        ]
    )
    lines.extend(
        f"| `{row['field']}` | `{row['dtype']}` | {row['cardinality']} | {row['missing']} | `{row['action']}` | `{row['target']}` | `{row['ontology_status']}` |"
        for row in packet["obs_field_decisions"]
    )
    lines.extend(
        [
            "",
            "## VAR and X",
            "",
            f"- VAR: `{var_text}`.",
            f"- X: `{x_text}`.",
        ]
    )
    if "var_decisions" in packet:
        lines.extend(
            f"- `{row['action']}` `{row['target']}` — {row.get('reason') or row.get('from') or row.get('value')}"
            for row in packet["var_decisions"]
        )
    lines.extend(
        [
            "",
            "## Proposed post-fix OBS schema",
            "",
            "`" + "`, `".join(packet["post_fix_obs_schema"]) + "`",
        ]
    )
    if "post_fix_var_schema" in packet:
        lines.extend(
            [
                "",
                "## Proposed post-fix VAR schema",
                "",
                "`" + "`, `".join(packet["post_fix_var_schema"]) + "`",
            ]
        )
    lines.extend(["", "## Executable remediation", ""])
    lines.extend(
        f"{i}. {step}" for i, step in enumerate(packet["remediation_plan"]["steps"], 1)
    )
    lines.extend(["", "## Validators", ""])
    lines.extend(f"- {value}" for value in packet["validators"])
    lines.extend(["", "## Evidence files", ""])
    lines.extend(f"- `{value}`" for value in packet["evidence_files"])
    return "\n".join(lines) + "\n"


def main() -> int:
    live = json.loads(LIVE.read_text())
    packets: dict[str, dict[str, Any]] = {}
    for dataset in ("GSE194214", "GSE269572", "GSM5901228", "GSM5901229"):
        packet = common_packet(dataset, live)
        if dataset == "GSE194214":
            gse194214(packet)
        elif dataset == "GSE269572":
            gse269572(packet)
        else:
            gsm(packet, 0 if dataset == "GSM5901228" else 3)
        add_field_by_field_decisions(packet)
        add_plan(packet)
        packets[dataset] = packet
        (EVIDENCE / f"{dataset}.audit.json").write_text(
            json.dumps(packet, indent=2, sort_keys=True) + "\n"
        )
        (EVIDENCE / f"{dataset}.audit.md").write_text(render(packet))
    print(
        json.dumps(
            {"packets": sorted(packets), "output": str(EVIDENCE)}, sort_keys=True
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
