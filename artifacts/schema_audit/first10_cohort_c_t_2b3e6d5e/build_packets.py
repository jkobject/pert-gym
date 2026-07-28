#!/usr/bin/env python3
"""Build deterministic per-dataset audit packets from frozen live/source evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
LIVE_PATH = ROOT / "live_audit.json"
SOURCE_PATH = ROOT / "source_evidence.json"
VAR_EVIDENCE_PATH = ROOT / "var_all_rows_evidence.json"
GENERATION_RECHECK_PATH = ROOT / "generation_recheck.json"
MANIFEST_SHA256 = "3f73fdc9e405279ebbd5e5a4d67ee8b6d32cd0a031b73d5297184f95b6bb7eb3"

COLUMN_DECISIONS: dict[str, dict[str, str]] = {
    "cell_id": {
        "action": "keep",
        "target": "cell_id",
        "reason": "unique source-backed cell identity",
    },
    "barcode": {
        "action": "keep",
        "target": "barcode",
        "reason": "unique source 10x barcode; useful for replay",
    },
    "source_accession": {
        "action": "move_to_dataset_metadata",
        "target": "source_accession",
        "reason": "exact but dataset-wide provenance",
    },
    "sample_accession": {
        "action": "move_to_dataset_metadata",
        "target": "sample_accession",
        "reason": "exact but dataset-wide provenance",
    },
    "sample_title": {
        "action": "move_to_dataset_metadata",
        "target": "sample_title",
        "reason": "descriptive source constant",
    },
    "source_name": {
        "action": "map",
        "target": "source_material",
        "reason": "scientific source material; not a current cell-type annotation",
    },
    "experiment": {
        "action": "map",
        "target": "source_experiment,batch",
        "reason": "source-backed experimental stratum",
    },
    "timepoint": {
        "action": "move_to_dataset_metadata",
        "target": "timepoint_days,timepoint_minutes",
        "reason": "single constant snapshot in this GSM child; preserve family time-course relation separately",
    },
    "timepoint_unit": {
        "action": "move_to_dataset_metadata",
        "target": "timepoint unit/day",
        "reason": "paired with child-level constant timepoint",
    },
    "culture_method": {
        "action": "keep",
        "target": "culture_method",
        "reason": "inference-relevant experimental conditioning field",
    },
    "ascorbic_acid_from_day_12": {
        "action": "keep",
        "target": "ascorbic_acid_from_day_12",
        "reason": "inference-relevant family condition; false for this child",
    },
    "biosample": {
        "action": "move_to_dataset_metadata",
        "target": "biosample",
        "reason": "exact sample-level provenance",
    },
    "sra_experiment": {
        "action": "move_to_dataset_metadata",
        "target": "sra_experiment",
        "reason": "exact sample-level provenance",
    },
    "organism": {
        "action": "keep_and_map",
        "target": "organism,organism_ontology_id",
        "reason": "scientific conditioning field; GEO taxid 9606",
    },
    "assay": {
        "action": "map",
        "target": "assay,assay_ontology_id,technology,technology_ontology_id",
        "reason": "source protocol specifies 10x 3' v3.1",
    },
    "n_genes_by_counts": {
        "action": "map",
        "target": "n_genes",
        "reason": "cell-level QC covariate",
    },
    "total_counts": {
        "action": "map",
        "target": "n_counts",
        "reason": "cell-level QC covariate",
    },
    "pct_counts_mt": {
        "action": "map",
        "target": "pct_mito",
        "reason": "cell-level QC covariate",
    },
    "source_cell_call_flag_missing": {
        "action": "move_to_dataset_metadata",
        "target": "source_cell_call_flag_missing",
        "reason": "ingestion/source-quality flag, not a biological cell variable",
    },
}

DERIVED_FIELDS = {
    "dataset": "GSM child accession",
    "sample": "GSM child accession",
    "original_obs_index": "exact predecessor OBS index",
    "obs_uuid": "pert-gym.obs.v1 UUID5 from dataset, canonical prefix, original index",
    "modality": "scRNA-seq (EFO:0008913) from GEO library strategy/source",
    "sequencer": "Illumina NovaSeq 6000 from GEO instrument model",
    "is_bulk": False,
    "is_pseudobulk": False,
    "perturbation": "none",
    "perturbation_type": "none",
    "is_control": "true in the perturbation-control sense; not a claim of developmental baseline equivalence",
    "cell_type": "unknown; applicable but no cell-level labels in bounded source/current OBS",
    "cell_line": "unknown; no exact line identifier in bounded source",
    "donor_id": "unknown",
    "disease": "unknown",
    "tissue_type": "not_applicable for in-vitro 3D suspension culture",
    "sex": "unknown",
    "age": "unknown",
    "ethnicity": "unknown",
}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_packet(
    dataset: str,
    live: dict[str, Any],
    source: dict[str, Any],
    var_evidence: dict[str, Any],
    generation_recheck: dict[str, Any],
) -> dict[str, Any]:
    current = live["datasets"][dataset]
    sample = source["datasets"][dataset]
    observed_columns = current["obs"]["columns"]
    if set(observed_columns) != set(COLUMN_DECISIONS):
        missing = sorted(set(observed_columns) - set(COLUMN_DECISIONS))
        stale = sorted(set(COLUMN_DECISIONS) - set(observed_columns))
        raise AssertionError(
            f"OBS decision coverage drift for {dataset}: unreviewed={missing}, absent={stale}"
        )
    obs_audit = []
    for column in current["obs"]["column_order"]:
        stats = observed_columns[column]
        obs_audit.append({"column": column, **stats, **COLUMN_DECISIONS[column]})

    expected = current["expected"]
    x = current["x"]
    var_all_rows = var_evidence["datasets"][dataset]
    payloads = current["payload_sha256"]
    if not current["links"]["obs_to_x"] or not current["links"]["x_to_var"]:
        raise AssertionError(f"broken feature links for {dataset}")
    if not all(current["axis_parity"].values()):
        raise AssertionError(f"axis mismatch for {dataset}")
    if (
        x["storage"]["sum"] != expected["expected_sum"]
        or x["storage"]["stored_nnz"] != expected["expected_nnz"]
    ):
        raise AssertionError(f"matrix invariant drift for {dataset}")
    if var_all_rows["payload_sha256"] != payloads["var"]:
        raise AssertionError(f"VAR all-row evidence hash drift for {dataset}")
    if not (
        var_all_rows["rows"] == 20631
        and var_all_rows["index_unique"]
        and var_all_rows["index_equals_feature_id"]
        and var_all_rows["feature_id_unique"]
        and var_all_rows["feature_id_exact_ensg_count"] == 20631
        and var_all_rows["feature_id_control_character_rows"] == 0
    ):
        raise AssertionError(f"VAR feature identity drift for {dataset}")
    current_generation_recheck = {}
    for role in ("obs", "x", "var"):
        checked = generation_recheck["objects"][f"{dataset}/{role}"]
        frozen = current["object_metadata"][role]
        if (
            checked["generation"] != frozen["generation"]
            or checked["size"] != frozen["size"]
        ):
            raise AssertionError(f"GCS generation/size drift for {dataset}/{role}")
        current_generation_recheck[role] = checked

    dataset_metadata = {
        "dataset_id": dataset,
        "source_accession": "GSE196799",
        "sample_accession": dataset,
        "sample_title": sample["sample_title"],
        "biosample": sample["biosample"],
        "sra_experiment": sample["sra_experiment"],
        "source_relation": "GEO series child sample",
        "source_relation_confidence": "exact",
        "timepoint_days": sample["day"],
        "timepoint_minutes": sample["day"] * 1440,
        "child_temporal_status": "non_temporal_single_snapshot",
        "family_temporal_status": "temporal_time_course",
        "temporal_family": "GSE196799",
        "family_observed_days": source["family"]["observed_family_days_from_manifest"],
        "source_cell_call_flag_missing": True,
        "x_semantics": "raw_counts_after_deterministic_cell_qc",
        "cell_qc": {
            "n_genes_strictly_greater_than": 2000,
            "mitochondrial_fraction_strictly_less_than": 0.1,
        },
        "doublet_filter_applied": False,
    }
    return {
        "format": "pert-gym.first10-cohort-c-dataset-audit/v1",
        "task_id": "t_2b3e6d5e",
        "dataset": dataset,
        "verdict": "correction_plan_ready_no_mutation",
        "evidence_binding": {
            "live_audit_sha256": sha256_file(LIVE_PATH),
            "source_evidence_sha256": sha256_file(SOURCE_PATH),
            "var_all_rows_evidence_sha256": sha256_file(VAR_EVIDENCE_PATH),
            "generation_recheck_sha256": sha256_file(GENERATION_RECHECK_PATH),
            "generation_rechecked_at": generation_recheck["checked_at"],
        },
        "source_provenance": {
            "relation": "GSM child of GSE196799; nested raw archive member converted under the accepted family manifest",
            "confidence": "exact",
            "geo_family": source["family"],
            "geo_sample": sample,
            "ontology_evidence": source["ontology_evidence"],
        },
        "current_artifacts": current["artifacts"],
        "current_object_metadata": current["object_metadata"],
        "current_object_generation_recheck": current_generation_recheck,
        "current_payload_sha256": payloads,
        "obs_audit": {
            "rows": current["obs"]["rows"],
            "index_unique": current["obs"]["index_unique"],
            "index_sha256": current["obs"]["index_sha256"],
            "missing_required_identity": ["obs_uuid", "original_obs_index"],
            "column_decisions": obs_audit,
            "derived_fields": DERIVED_FIELDS,
            "dataset_metadata_proposal": dataset_metadata,
        },
        "var_audit": {
            "rows": var_all_rows["rows"],
            "namespace": "Ensembl human stable gene ID",
            "species": "Homo sapiens",
            "species_ontology_id": "NCBITaxon:9606",
            "index_unique": var_all_rows["index_unique"],
            "index_equals_feature_id": var_all_rows["index_equals_feature_id"],
            "feature_id_unique": var_all_rows["feature_id_unique"],
            "feature_id_exact_ensg_count": var_all_rows["feature_id_exact_ensg_count"],
            "control_character_rows": var_all_rows["feature_id_control_character_rows"],
            "gene_symbol_non_null": var_all_rows["gene_symbol_non_null"],
            "gene_symbol_unique": var_all_rows["gene_symbol_unique"],
            "duplicate_gene_symbol_rows": var_all_rows["duplicate_gene_symbol_rows"],
            "duplicate_symbol_policy": "retain; feature identity is the unique ordered Ensembl ID, not display symbol",
            "payload_sha256": payloads["var"],
            "sibling_vars_byte_identical": live["family_checks"][
                "all_var_payloads_byte_identical"
            ],
            "decision": "revise metadata columns only; retain feature order and X axis",
            "proposed_columns": [
                "feature_id",
                "ensembl_id",
                "gene_id",
                "gene_symbol",
                "feature_type",
                "organism",
                "organism_ontology_id",
                "author_gene_id",
                "author_gene_symbol",
            ],
        },
        "x_audit": {
            "shape": x["shape"],
            "encoding": x["storage"]["encoding_type"],
            "dtype": x["storage"]["data_dtype"],
            "nnz": x["storage"]["stored_nnz"],
            "sum": x["storage"]["sum"],
            "minimum_stored_value": x["storage"]["minimum"],
            "maximum_stored_value": x["storage"]["maximum"],
            "all_values_integer": x["storage"]["all_values_integer"],
            "payload_sha256": payloads["x"],
            "axis_parity": current["axis_parity"],
            "semantics": "deposited raw integer UMI counts after deterministic cell QC",
            "decision": "retain exact X bytes",
        },
        "feature_links": current["links"],
        "feature_link_targets": {
            "obs.X": current["artifacts"]["x"]["uid"],
            "X.var": current["artifacts"]["var"]["uid"],
        },
        "layout_audit": {
            "canonical_prefix": f"data/cleaned/{dataset}",
            "chunking": "not_chunked",
            "chunk_verdict": "appropriate: one 4.4k-6.4k-cell CSR member of 41-60 MB; rechunking would add overhead without a failed invariant",
            "same_prefix_var": True,
            "var_members_for_dataset": 1,
            "var_verdict": "exactly one same-prefix VAR for this unchunked child; identical sibling hashes prove family axis parity but do not justify violating same-prefix contract",
        },
        "remediation": {
            "executable": "remediate_local.py",
            "publication_owner": "downstream single writer t_d6fc80f0",
            "steps": [
                "freeze exact predecessor UIDs, keys, GCS generations and SHA-256 from this packet",
                "materialize revised OBS with exhaustive keep/drop/map/derive decisions and deterministic identity",
                "materialize metadata-enriched VAR without changing feature order",
                "retain X exact bytes and restore obs->X->var links",
                "publish append-only revisions, then successor Collection with no unrelated drift",
                "run bounded EU readback and zero-write replay before review",
            ],
            "validators": [
                "source/current hashes and artifact UIDs unchanged before write",
                "OBS row count/order/index conserved; obs_uuid present, valid and unique",
                "all current OBS columns have an explicit decision",
                "VAR exactly 20,631 unique ENSG IDs, no controls, exact X feature order",
                "X shape/CSR integer sum/nnz/hash unchanged",
                "obs->X and X->var exact UID links resolve",
                "one same-prefix VAR; no chunks introduced",
                "child temporal status non_temporal_single_snapshot and GSE196799 family relation retained",
                "unknown and not_applicable states remain distinct",
            ],
        },
        "residual_unknowns": [
            "cell-level differentiated cell_type labels are absent from bounded GEO/current OBS evidence",
            "exact cell-line/donor/sex/age/ethnicity/disease metadata are absent from bounded evidence",
            "GEO text contains a processing inconsistency: alignment says GRCh38/Ensembl 99 while a separate Genome_build line says hg19; the deposited ordered ENSG feature axis is accepted, but the writer must preserve this provenance note rather than relabel the build",
        ],
        "writes": 0,
    }


def render_markdown(packet: dict[str, Any]) -> str:
    dataset = packet["dataset"]
    obs = packet["obs_audit"]
    var = packet["var_audit"]
    x = packet["x_audit"]
    lines = [
        f"# {dataset} scientific audit and correction plan",
        "",
        f"Verdict: **{packet['verdict']}**. This packet is read-only; publication belongs to the downstream single writer.",
        "",
        "## Exact source and current identity",
        "",
        f"- GEO child: [{dataset}]({packet['source_provenance']['geo_sample']['geo_url']}) of [GSE196799]({packet['source_provenance']['geo_family']['geo_url']}).",
        f"- Source relation confidence: `{packet['source_provenance']['confidence']}`.",
        f"- Accepted family manifest: `{packet['source_provenance']['geo_family']['accepted_manifest']['uri']}#{packet['source_provenance']['geo_family']['accepted_manifest']['generation']}`; SHA-256 `{MANIFEST_SHA256}`.",
        f"- Frozen evidence is hash-bound and all nine OBS/X/VAR GCS generations were freshly rechecked at `{packet['evidence_binding']['generation_rechecked_at']}` with no generation or size drift.",
    ]
    for role in ("obs", "x", "var"):
        artifact = packet["current_artifacts"][role]
        obj = packet["current_object_metadata"][role]
        lines.append(
            f"- {role.upper()}: Artifact `{artifact['uid']}` / `{artifact['key']}` / Lamin hash `{artifact['hash']}` / GCS generation `{obj['generation']}` / payload SHA-256 `{packet['current_payload_sha256'][role]}`."
        )
    lines += [
        "",
        "## OBS column audit",
        "",
        f"Current OBS has {obs['rows']:,} rows, unique ordered index SHA-256 `{obs['index_sha256']}`, and no nulls in any current column. It lacks required `obs_uuid` and `original_obs_index`.",
        "",
        "| column | dtype | cardinality | missing | decision | target | rationale |",
        "|---|---|---:|---:|---|---|---|",
    ]
    for item in obs["column_decisions"]:
        lines.append(
            f"| `{item['column']}` | `{item['dtype']}` | {item['nunique_non_null']} | {item['null']} | `{item['action']}` | `{item['target']}` | {item['reason']} |"
        )
    metadata = obs["dataset_metadata_proposal"]
    lines += [
        "",
        "### Temporal and metadata verdict",
        "",
        f"This child is one snapshot at day {metadata['timepoint_days']:g} ({metadata['timepoint_minutes']:g} minutes), so its child verdict is `{metadata['child_temporal_status']}` and the redundant per-cell time fields move to dataset metadata. GSE196799 remains `{metadata['family_temporal_status']}` across days {metadata['family_observed_days']}; the family relation is not erased.",
        "",
        "Scientifically useful constants remain conditioning columns (organism, culture method, ascorbic-acid condition, assay/technology, source material). Pure source/descriptive constants move to dataset metadata. `hiPSCs` maps only to source material EFO:0004905, not to current cell type. Unsupported fields remain `unknown`; in-vitro tissue is `not_applicable`.",
        "",
        "## VAR, X, links, and layout",
        "",
        f"- VAR: {var['rows']:,} rows; all {var['feature_id_exact_ensg_count']:,} IDs are unique unversioned human ENSG identifiers; zero control-character rows. Gene symbols are complete but only {var['gene_symbol_unique']:,} unique ({var['duplicate_gene_symbol_rows']} rows participate in duplicate symbols), so symbol duplicates are retained and Ensembl IDs remain identity.",
        f"- X: `{x['encoding']}` `{x['dtype']}` raw integer counts, shape {x['shape']}, nnz {x['nnz']:,}, sum {x['sum']:,}; exact OBS/X and X/VAR axis parity passes.",
        f"- Links: OBS→X `{packet['feature_links']['obs_to_x']}` targeting `{packet['feature_link_targets']['obs.X']}`; X→VAR `{packet['feature_links']['x_to_var']}` targeting `{packet['feature_link_targets']['X.var']}`.",
        f"- Layout: {packet['layout_audit']['chunk_verdict']}",
        f"- VAR policy: {packet['layout_audit']['var_verdict']}",
        "",
        "## Executable remediation and validators",
        "",
        "Run `remediate_local.py` against the exact current local payloads. It emits revised local OBS/VAR plus dataset metadata and a receipt; it never connects to LaminDB or writes GCS. The downstream writer must publish append-only revisions, restore exact links, build a successor Collection, perform bounded readback, and prove zero-write replay.",
        "",
    ]
    lines.extend(f"- {item}" for item in packet["remediation"]["validators"])
    lines += ["", "## Residual unknowns", ""]
    lines.extend(f"- {item}" for item in packet["residual_unknowns"])
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    live = load_json(LIVE_PATH)
    source = load_json(SOURCE_PATH)
    var_evidence = load_json(VAR_EVIDENCE_PATH)
    generation_recheck = load_json(GENERATION_RECHECK_PATH)
    if live["writes"] != 0 or live["task_id"] != "t_2b3e6d5e":
        raise AssertionError("live evidence task/write boundary mismatch")
    for dataset in sorted(source["datasets"]):
        packet = build_packet(dataset, live, source, var_evidence, generation_recheck)
        (ROOT / f"{dataset}_audit.json").write_text(
            json.dumps(packet, indent=2, sort_keys=True) + "\n"
        )
        (ROOT / f"{dataset}_audit.md").write_text(render_markdown(packet))
    print(
        json.dumps(
            {"datasets": sorted(source["datasets"]), "packet_count": 6, "writes": 0},
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
