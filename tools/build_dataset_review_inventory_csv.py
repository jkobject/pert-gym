#!/usr/bin/env python3
"""Build one-row-per-dataset review inventory with orthogonal acceptance gates.

This intentionally does not count Lamin records, triplet members, chunks, or
catalogue rows as datasets. It combines the frozen 70-real-dataset scientific
crosswalk with the 22 genuinely-new logical families from the accepted newness
reconciliation. Every gate is fail-closed and retains its evidence source.
The default build starts from the tracked, hash-pinned pre-reconciliation CSV
instead of reading its own generated output as input.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data/pert_gym_dataset_review_inventory.csv"
BASELINE_INPUT = ROOT / "data/pert_gym_dataset_review_inventory_baseline_20260729.csv"
REAL_DATASETS = ROOT / "artifacts/schema_audit/final_real_dataset_obs_var_20260717.tsv"
NEWNESS = (
    ROOT / "artifacts/orchestration/accepted_28_newness_reconciliation_20260717.json"
)
BRANCH_SNAPSHOT = (
    ROOT / "artifacts/notebook_decisions/jkobject_vs_main_dataset_inventory.json"
)
PUBLICATION_LEDGER = (
    ROOT
    / "artifacts/orchestration/publication_queue/accepted_component_identities_v1.progress.snapshot.json"
)
ACCEPTED_WAVE = (
    ROOT / "artifacts/dataset_completion/accepted_10_dataset_review_snapshot.json"
)
INTEGRATION_MANIFEST = (
    ROOT / "artifacts/dataset_completion/accepted_10_of_10_integration_manifest.json"
)
INTEGRATION_COMMIT = "36c7e02fdf0421f7844918ed646e49a6322ed30f"
INTEGRATION_BASE = "5e27972bd09e0f931f6cd0f7ee4cd8df01726a5f"
INTEGRATION_MANIFEST_SHA256 = (
    "f0149596fd06ea61766499b812e0669ca08f6fce7440bad7af171d8885c40bd4"
)
ACCEPTED_EVIDENCE_DIGESTS = ROOT / "data/accepted_10_evidence_digests.json"
ACCEPTED_EVIDENCE_DIGESTS_SHA256 = (
    "42926969b40e717e44b7474d7ae75677db61b5931e216406d19cf6b3128dbd69"
)

EVIDENCE_LABEL = "dataset-review-inventory@2026-08-11"

# Strict independently accepted real-dataset identities from the current TODO/current-status
# ledger. These are dataset identities, never physical triplet/member counts.
OBS_ACCEPTED = {
    "drug-seq/GSE120222",
    "ginkgo/vcpi",
    "lincs/phase2",
    "scperturb/chang22",
    "scperturb/datlinger17",
    "depmap_ccle/26q1",
    "scperturb/adamson16",
    "SchiebingerLander2019",
    "geo/GSE132080",
    "geo/GSE197452",
}
VAR_ACCEPTED = {
    "drug-seq/GSE120222",
    "scperturb/chang22",
    "scperturb/datlinger17",
    "depmap_ccle/26q1",
    "scperturb/adamson16",
    "SchiebingerLander2019",
    "geo/GSE132080",
    "geo/GSE197452",
}

# Fresh bounded live main readback on 2026-07-29 found exact active keys for these
# fully accepted biological datasets. Their accepted revisions may be on jkobject;
# this flag means the dataset/source representation already existed on main.
MAIN_LIVE_VERIFIED = {
    "drug-seq/GSE120222",
    "scperturb/adamson16",
    "scperturb/chang22",
    "scperturb/datlinger17",
    "SchiebingerLander2019",
}
JKOBJECT_ONLY_FULLY_ACCEPTED = {
    "depmap_ccle/26q1",
    "geo/GSE132080",
    "geo/GSE197452",
}

# The ten accepted 10/22 registration+Collection deltas. This is deliberately
# separate from strict OBS/VAR acceptance.
REGISTERED_NEW_RECORD_IDS = {
    "temporal_v4_057_c_elegans_embryogenesis",
    "temporal_v4_059_drosophila_embryo_dorsal_ventral_patterning_scrna_seq",
    "temporal_v4_089_organoiddb_odd001155_gse196799",
    "temporal_v4_092_organoiddb_odd001154_gse194214",
    "temporal_v4_094_organoiddb_odd001099_gse138002",
    "temporal_v4_097_organoiddb_odd001111_gse130238",
    "temporal_v4_098_an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation",
    "temporal_v4_109_stable_chambered_cardioids_from_human_pluripotent_stem_cells_scrna_seq",
    "temporal_v4_116_perturbase_gse107185",
    "temporal_v4_133_scrnaseq_unravels_the_transcriptional_network_underlying_zebrafish_retina_regene",
}

# Immutable accepted-receipt predicates. These are intentionally code-owned rather
# than supplied by the mutable reconciliation snapshot.
ACCEPTED_RECEIPT_ASSERTIONS: dict[str, list[tuple[tuple[str, ...], Any]]] = {
    "GSE228110": [
        (("status",), "PASS"),
        (("obs_validation", "status"), "PASS"),
        (("var_validation", "status"), "PASS"),
        (("source_exhaustive",), True),
        (("collections", "status"), "PASS"),
        (("duplicate_gate_main", "status"), "PASS"),
    ],
    "C. elegans embryogenesis": [
        (("obs", "status"), "pass"),
        (("var", "status"), "pass"),
        (("x", "status"), "pass_reused_accepted_payload"),
        (("scientific_equivalence_gate", "status"), "pass"),
        (("collection", "status"), "pass_append_only_successor"),
    ],
    "Drosophila E-MTAB-9304": [
        (("complete",), True),
        (("gates", "strict_obs_pass"), True),
        (("gates", "species_correct_var_pass"), True),
        (("gates", "structure_pass"), True),
        (("gates", "cleaning_pass"), True),
        (("gates", "canonical_lamin_pass"), True),
        (("gates", "collection_readback_pass"), True),
    ],
    "GSE138002": [],
    "GSE130238": [],
    "GSE194214 / ODD001154": [
        (("status",), "PASS"),
        (("invariants", "OBS_SCHEMA_COMPLETED"), True),
        (("invariants", "VAR_COMPLETED"), True),
        (("invariants", "accepted_component_status"), "include"),
        (("collection", "successor", "uid"), "GvFuzrWQeKB6Pd8t0001"),
        (("collection", "successor", "target_obs_uid"), "GtDvEO1BsANR8VKR0002"),
    ],
    "GSE196799 / ODD001155": [],
    "GSE107185": [],
    "SCP1973 / GSE226373": [
        (("complete",), True),
        (("gates", "strict_obs_pass"), True),
        (("gates", "species_correct_var_pass"), True),
        (("gates", "chunks_structure_pass"), True),
        (("gates", "cleaning_pass"), True),
        (("gates", "canonical_storage_lamin_pass"), True),
        (("gates", "collection_pass"), True),
        (("gates", "accepted_X_logical_matrix_recovery_pass"), True),
    ],
    "GSE269572": [],
}
for _dataset in [
    "GSE138002",
    "GSE130238",
    "GSE196799 / ODD001155",
    "GSE107185",
    "GSE269572",
]:
    ACCEPTED_RECEIPT_ASSERTIONS[_dataset] = [
        (("status",), "PASS"),
        (("gates", "OBS"), "PASS"),
        (("gates", "VAR"), "PASS"),
        (("gates", "chunks"), "PASS"),
        (("gates", "cleaning"), "PASS"),
        (("gates", "lamin_jkobject"), "PASS"),
        (("gates", "collection"), "PASS"),
    ]

OBSERVATION_EVIDENCE: dict[str, tuple[str, tuple[str, ...]]] = {
    "GSE228110": (
        "artifacts/dataset_completion/temporal_an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation/completion_receipt.json",
        ("obs_validation", "rows"),
    ),
    "C. elegans embryogenesis": (
        "artifacts/dataset_completion/temporal__c_elegans_embryogenesis/verification_receipt.json",
        ("counts", "observations"),
    ),
    "Drosophila E-MTAB-9304": (
        "artifacts/dataset_completion/temporal__drosophila_dv_patterning/latest_receipt.json",
        ("final", "obs", "rows"),
    ),
    "GSE138002": (
        "artifacts/dataset_completion/temporal__organoiddb_odd001099_gse138002/completion_receipt.json",
        ("member_after", "obs_receipt", "rows"),
    ),
    "GSE130238": (
        "artifacts/dataset_completion/temporal__organoiddb_odd001111_gse130238/verify_receipt.json",
        ("member_after", "obs_receipt", "rows"),
    ),
    "GSE194214 / ODD001154": (
        "artifacts/dataset_completion/temporal__organoiddb_odd001154_gse194214/replay_receipt.json",
        ("prepared", "obs_receipt", "rows"),
    ),
    "GSE196799 / ODD001155": (
        "artifacts/dataset_completion/temporal__organoiddb_odd001155_gse196799/verification_receipt.json",
        ("counts", "observations"),
    ),
    "GSE107185": (
        "artifacts/dataset_completion/temporal__perturbase_gse107185/completion_receipt.json",
        ("member_after", "obs_receipt", "rows"),
    ),
    "SCP1973 / GSE226373": (
        "artifacts/dataset_completion/temporal__zebrafish_retina_regeneration/completion_receipt.json",
        ("final", "obs", "rows"),
    ),
    "GSE269572": (
        "artifacts/dataset_completion/temporal__stable_chambered_cardioids/completion_receipt.json",
        ("counts", "observations"),
    ),
}

EXPECTED_ACCEPTED_LINEAGE = {
    "GSE228110": (
        "8d97b16affb53564199044508ed3f709008fdfd7",
        "t_b626108c",
        "t_f8df2f65",
    ),
    "C. elegans embryogenesis": (
        "92111fbdf860828df71a039e5d2ccf9ffe4cbabb",
        "t_a62edaa7",
        "t_fd4e1a1a",
    ),
    "Drosophila E-MTAB-9304": (
        "fdd88db96616408d7d15689b49cd0ed4f90f52bd",
        "t_851469bf",
        "t_5049b6c0",
    ),
    "GSE138002": (
        "0ec69b86568665a0f34446b764b980afdaa5e844",
        "t_9b0cc7c2",
        "t_57c56cea",
    ),
    "GSE130238": (
        "323a0e16fdad15938eb6f99794192649ddb4887f",
        "t_9b5c70a6",
        "t_57c56cea",
    ),
    "GSE194214 / ODD001154": (
        "6b34d5cfe06765f01e27e190cf20be55b8186e2f",
        "t_56a7b7cf",
        "t_f8df2f65",
    ),
    "GSE196799 / ODD001155": (
        "923a0ae17c193481d68ad784e01de5a6c520ccdb",
        "t_a2ff6038",
        "t_f8df2f65",
    ),
    "GSE107185": (
        "83bdf3e8686453c6d8578047d005d60df13d95ca",
        "t_ab37edf6",
        "t_99c68302",
    ),
    "SCP1973 / GSE226373": (
        "2c0f23bafb282a6200aaacf4dc52c2740a8df835",
        "t_bb2f5bc0",
        "t_246e50fa",
    ),
    "GSE269572": (
        "44b934c168243dd1fcdad2918091ce793c8cedf5",
        "t_363b9754",
        "t_246e50fa",
    ),
}

LEGACY_COLUMNS = [
    "dataset_id",
    "review_scope",
    "logical_families",
    "physical_member_count",
    "observations",
    "main_baseline_crosswalk",
    "main_dataset_or_similar_visible",
    "jkobject_dataset_visible",
    "branch_disposition",
    "duplicate_newness_status",
    "strict_obs_validated",
    "strict_var_validated",
    "chunks_or_structure_validated",
    "cleaning_validated",
    "lamin_registered",
    "in_versioned_collection",
    "entirely_validated",
    "entirely_validated_main_existing_dataset",
    "entirely_validated_jkobject_addition",
    "missing_requirements",
    "next_review_focus",
    "source_completion_state",
    "evidence",
]
COLUMNS = [
    *LEGACY_COLUMNS,
    "aliases",
    "scoped_scientific_validation_accepted",
    "accepted_wave",
    "accepted_wave_scoped_validation",
    "accepted_wave_head",
    "producer_task_id",
    "reviewer_task_id",
    "scientific_modality",
    "experimental_axes",
    "outcomes_endpoints",
    "annotation_level",
    "source_evidence",
    "scientific_contract_documented",
    "scientific_contract_bound",
    "payload_prefixes",
    "structured_payload_evidence",
    "processing_decision_notebook_present",
    "processing_decision_notebook",
    "processing_decision_notebook_path",
    "canonical_data_cleaned_payload",
    "accepted_head_integrated",
    "staging_decommissioned_with_receipt",
    "inventory_docs_same_snapshot_accepted",
    "exact_head_inventory_pr_merged",
]


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "/", value.lower()).strip("/")


def _json_list(value: str) -> list[str]:
    parsed = json.loads(value)
    return [str(item) for item in parsed]


def _snapshot_main_visibility(snapshot: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for row in snapshot["rows"]:
        if row.get("classification") != "branch_revision":
            continue
        for value in [
            row.get("dataset_id", ""),
            row.get("source", ""),
            row.get("accession", ""),
            *row.get("logical_prefixes", []),
        ]:
            if value and value != "unknown":
                values.add(_norm(str(value)))
    return values


def _matches_main(logical_families: list[str], main_values: set[str]) -> bool:
    for family in logical_families:
        token = _norm(family)
        if token in main_values:
            return True
    return False


def _missing(row: dict[str, Any]) -> list[str]:
    required = [
        ("strict_obs_validated", "full_obs_validation"),
        ("strict_var_validated", "var_validation_or_NA_disposition"),
        ("chunks_or_structure_validated", "chunks_or_structure_validation"),
        ("cleaning_validated", "cleaning_acceptance"),
        ("lamin_registered", "add_to_lamin"),
        ("in_versioned_collection", "add_to_versioned_collection"),
        ("scientific_contract_bound", "scientific_contract_evidence"),
        ("processing_decision_notebook", "executable_processing_decision_notebook"),
        ("canonical_data_cleaned_payload", "canonical_data_cleaned_payload"),
        ("accepted_head_integrated", "accepted_head_integration"),
        (
            "staging_decommissioned_with_receipt",
            "staging_decommission_receipt",
        ),
        (
            "inventory_docs_same_snapshot_accepted",
            "accepted_inventory_docs_snapshot",
        ),
        ("exact_head_inventory_pr_merged", "merged_exact_head_inventory_pr"),
    ]
    return [label for key, label in required if not bool(row[key])]


def _focus(missing: list[str]) -> str:
    order = [
        "full_obs_validation",
        "var_validation_or_NA_disposition",
        "chunks_or_structure_validation",
        "cleaning_acceptance",
        "add_to_lamin",
        "add_to_versioned_collection",
        "scientific_contract_evidence",
        "executable_processing_decision_notebook",
        "canonical_data_cleaned_payload",
        "accepted_head_integration",
        "staging_decommission_receipt",
        "accepted_inventory_docs_snapshot",
        "merged_exact_head_inventory_pr",
    ]
    for item in order:
        if item in missing:
            return item
    return "complete"


def _default_reconciliation_fields() -> dict[str, Any]:
    return {
        "aliases": "[]",
        "scoped_scientific_validation_accepted": False,
        "accepted_wave": False,
        "accepted_wave_scoped_validation": False,
        "accepted_wave_head": "",
        "producer_task_id": "",
        "reviewer_task_id": "",
        "scientific_modality": "",
        "experimental_axes": "",
        "outcomes_endpoints": "",
        "annotation_level": "",
        "source_evidence": "[]",
        "scientific_contract_documented": False,
        "scientific_contract_bound": False,
        "payload_prefixes": "[]",
        "structured_payload_evidence": False,
        "processing_decision_notebook_present": False,
        "processing_decision_notebook": False,
        "processing_decision_notebook_path": "",
        "canonical_data_cleaned_payload": False,
        "accepted_head_integrated": False,
        "staging_decommissioned_with_receipt": False,
        "inventory_docs_same_snapshot_accepted": False,
        "exact_head_inventory_pr_merged": False,
    }


def _finalize_row(row: dict[str, Any]) -> None:
    strict_scoped = all(
        bool(row[key])
        for key in [
            "strict_obs_validated",
            "strict_var_validated",
            "chunks_or_structure_validated",
            "cleaning_validated",
            "lamin_registered",
            "in_versioned_collection",
        ]
    )
    scoped = strict_scoped or bool(row["accepted_wave_scoped_validation"])
    row["scoped_scientific_validation_accepted"] = scoped
    missing = _missing(row)
    entire = not missing
    row["entirely_validated"] = entire
    row["entirely_validated_main_existing_dataset"] = entire and bool(
        row["main_dataset_or_similar_visible"]
    )
    row["entirely_validated_jkobject_addition"] = entire and not bool(
        row["main_dataset_or_similar_visible"]
    )
    row["missing_requirements"] = ";".join(missing)
    row["next_review_focus"] = _focus(missing)


def _immutable_accepted_file(
    integration_item: dict[str, Any], relative_path: str
) -> bytes:
    if relative_path not in integration_item["transplanted_paths"]:
        raise RuntimeError(
            f"accepted-wave evidence is outside transplanted paths: {relative_path}"
        )
    local_path = ROOT / relative_path
    if not local_path.is_file():
        raise RuntimeError(f"accepted-wave evidence is missing: {relative_path}")
    current = local_path.read_bytes()
    index = _accepted_evidence_index()
    dataset = integration_item["dataset"]
    indexed = index.get(dataset)
    if indexed is None or indexed["accepted_head"] != integration_item["accepted_sha"]:
        raise RuntimeError(f"accepted-wave evidence head is not pinned: {dataset}")
    expected_digest = indexed["files"].get(relative_path)
    if (
        expected_digest is None
        or hashlib.sha256(current).hexdigest() != expected_digest
    ):
        raise RuntimeError(
            f"accepted-wave evidence differs from its accepted head: {relative_path}"
        )
    return current


def _accepted_evidence_index() -> dict[str, dict[str, Any]]:
    raw = ACCEPTED_EVIDENCE_DIGESTS.read_bytes()
    if hashlib.sha256(raw).hexdigest() != ACCEPTED_EVIDENCE_DIGESTS_SHA256:
        raise RuntimeError("accepted evidence digest index has drifted")
    document = json.loads(raw)
    if (
        document.get("schema_version") != 1
        or document.get("integration_commit") != INTEGRATION_COMMIT
        or document.get("integration_manifest_sha256") != INTEGRATION_MANIFEST_SHA256
    ):
        raise RuntimeError("accepted evidence digest index contract is invalid")
    index: dict[str, dict[str, Any]] = {}
    for record in document.get("datasets", []):
        dataset = record.get("dataset")
        files = record.get("files")
        if (
            not isinstance(dataset, str)
            or dataset in index
            or not isinstance(files, list)
        ):
            raise RuntimeError("accepted evidence digest index is malformed")
        file_index: dict[str, str] = {}
        for item in files:
            path = item.get("path") if isinstance(item, dict) else None
            digest = item.get("sha256") if isinstance(item, dict) else None
            if (
                not isinstance(path, str)
                or path in file_index
                or not isinstance(digest, str)
                or len(digest) != 64
            ):
                raise RuntimeError("accepted evidence digest index has an invalid file")
            file_index[path] = digest
        index[dataset] = {
            "accepted_head": record.get("accepted_head"),
            "files": file_index,
        }
    if set(index) != set(EXPECTED_ACCEPTED_LINEAGE):
        raise RuntimeError("accepted evidence digest index dataset set has drifted")
    return index


def _immutable_integration_manifest() -> dict[str, Any]:
    current = INTEGRATION_MANIFEST.read_bytes()
    if hashlib.sha256(current).hexdigest() != INTEGRATION_MANIFEST_SHA256:
        raise RuntimeError("integration manifest differs from its accepted commit")
    document = json.loads(current)
    if document["integration_base"]["sha"] != INTEGRATION_BASE:
        raise RuntimeError("integration commit is not based on the declared base")
    return document


def _resolve_json_path(document: dict[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = document
    for part in path:
        if not isinstance(value, dict) or part not in value:
            raise RuntimeError(f"accepted receipt is missing {'.'.join(path)}")
        value = value[part]
    return value


def _key_uid_index(mappings: list[dict[str, Any]]) -> dict[str, set[str]]:
    index: dict[str, set[str]] = defaultdict(set)
    for mapping in mappings:
        key = mapping.get("key")
        uid = mapping.get("uid")
        artifact_hash = mapping.get("hash")
        size = mapping.get("size")
        hash_bound = isinstance(artifact_hash, str) and bool(artifact_hash.strip())
        size_bound = isinstance(size, int) and size > 0
        if (
            isinstance(key, str)
            and isinstance(uid, str)
            and bool(uid.strip())
            and (hash_bound or size_bound)
            and mapping.get("is_latest") is not False
        ):
            index[key].add(uid)
    return index


PAYLOAD_RECEIPT_CONTAINERS: dict[str, tuple[tuple[str, ...], dict[str, str]]] = {
    "C. elegans embryogenesis": (("after",), {"obs": "obs", "X": "X", "var": "var"}),
    "Drosophila E-MTAB-9304": (
        ("final", "artifacts"),
        {"obs": "obs", "X": "X", "var": "var"},
    ),
    "GSE138002": (("member_after",), {"obs": "obs", "X": "X", "var": "var"}),
    "GSE130238": (("member_after",), {"obs": "obs", "X": "X", "var": "var"}),
    "GSE194214 / ODD001154": (
        ("prepared",),
        {"obs": "latest_obs", "X": "X", "var": "var"},
    ),
    "GSE107185": (("member_after",), {"obs": "obs", "X": "X", "var": "var"}),
    "SCP1973 / GSE226373": (
        ("final", "artifacts"),
        {"obs": "obs", "X": "X", "var": "var"},
    ),
    "GSE269572": (("links",), {"obs": "obs", "X": "X", "var": "var"}),
}


def _assert_role_key(dataset: str, role: str, mapping: dict[str, Any]) -> None:
    key = mapping.get("key")
    suffixes = {
        "obs": ("/obs.parquet",),
        "X": ("/X.h5ad", "/X.zarr.zip", "/X.sparse-zarr.zip"),
        "var": ("/var.parquet",),
    }
    if not isinstance(key, str) or not key.endswith(suffixes[role]):
        raise RuntimeError(f"{dataset} receipt {role} identity has the wrong key role")


def _payload_receipt_mappings(
    dataset: str, receipt: dict[str, Any]
) -> list[dict[str, Any]]:
    """Extract identities only from code-owned accepted-receipt locations."""
    if dataset == "GSE228110":
        members = _resolve_json_path(receipt, ("members",))
        if not isinstance(members, list):
            raise RuntimeError("GSE228110 receipt members are malformed")
        mappings = []
        for member in members:
            if not isinstance(member, dict):
                raise RuntimeError("GSE228110 receipt member is malformed")
            for role in ("obs", "x", "var"):
                mapping = member.get(role)
                if not isinstance(mapping, dict):
                    raise RuntimeError(f"GSE228110 receipt is missing member {role}")
                _assert_role_key(dataset, "X" if role == "x" else role, mapping)
                mappings.append(mapping)
        return mappings
    if dataset == "GSE196799 / ODD001155":
        members = _resolve_json_path(receipt, ("chunks", "members"))
        shared_var = _resolve_json_path(receipt, ("links", "shared_var"))
        if not isinstance(members, dict) or not isinstance(shared_var, dict):
            raise RuntimeError("GSE196799 receipt identities are malformed")
        mappings = []
        for member in members.values():
            if not isinstance(member, dict) or not isinstance(
                member.get("identity"), dict
            ):
                raise RuntimeError("GSE196799 member identity is malformed")
            _assert_role_key(dataset, "X", member["identity"])
            mappings.append(member["identity"])
        _assert_role_key(dataset, "var", shared_var)
        mappings.append(shared_var)
        return mappings
    path, roles = PAYLOAD_RECEIPT_CONTAINERS[dataset]
    container = _resolve_json_path(receipt, path)
    if not isinstance(container, dict):
        raise RuntimeError(f"{dataset} receipt identity container is malformed")
    mappings = []
    for role, key in roles.items():
        mapping = container.get(key)
        if not isinstance(mapping, dict):
            raise RuntimeError(f"{dataset} receipt is missing {role} identity")
        _assert_role_key(dataset, role, mapping)
        mappings.append(mapping)
    return mappings


def _receipt_contract_valid(dataset: str, receipt: dict[str, Any]) -> bool:
    assertions = ACCEPTED_RECEIPT_ASSERTIONS[dataset]
    if not assertions:
        raise RuntimeError(f"accepted receipt has no assertions: {dataset}")
    for path, expected in assertions:
        observed = _resolve_json_path(receipt, path)
        if observed != expected:
            raise RuntimeError(
                f"accepted receipt assertion failed: {dataset} {'.'.join(path)}"
            )
    return True


def _structured_payload_complete(
    record: dict[str, Any],
    receipt: dict[str, Any],
    evidence_documents: list[dict[str, Any]],
) -> tuple[bool, bool]:
    prefixes = record["payload_prefixes"]
    if len(prefixes) != len(set(prefixes)):
        raise RuntimeError(
            f"accepted-wave payload prefixes are not unique: "
            f"{record['canonical_dataset_id']}"
        )
    index = _key_uid_index(
        _payload_receipt_mappings(record["integration_dataset"], receipt)
    )
    shared_var_prefix = record.get("shared_var_prefix")
    all_triplets_bound = True
    for prefix in prefixes:
        obs_key = f"{prefix}/obs.parquet"
        var_key = f"{shared_var_prefix or prefix}/var.parquet"
        x_keys = [
            f"{prefix}/X.h5ad",
            f"{prefix}/X.zarr.zip",
            f"{prefix}/X.sparse-zarr.zip",
        ]
        obs_uids = index.get(obs_key, set())
        var_uids = index.get(var_key, set())
        bound_x_keys = {key: index[key] for key in x_keys if index.get(key)}
        x_uids = set().union(*bound_x_keys.values()) if bound_x_keys else set()
        if not shared_var_prefix and (
            len(obs_uids) == 1
            and len(var_uids) == 1
            and len(bound_x_keys) == 1
            and len(x_uids) == 1
            and obs_uids.isdisjoint(var_uids)
            and obs_uids.isdisjoint(x_uids)
            and var_uids.isdisjoint(x_uids)
        ):
            continue
        if not shared_var_prefix:
            return False, False

        # Shared-var families can bind the per-sample prefix and role UIDs through
        # a source manifest plus explicit receipt link rows.
        sample = prefix.rsplit("/", 1)[-1]
        sample_contracts = []
        for document in evidence_documents:
            samples = document.get("samples")
            if isinstance(samples, dict) and sample in samples:
                candidate = samples[sample]
                if (
                    isinstance(candidate, dict)
                    and candidate.get("prefix") == prefix
                    and set(candidate.get("accepted_uids", {})) == {"obs", "X", "var"}
                ):
                    sample_contracts.append(candidate)
        link_rows = _resolve_json_path(receipt, ("links", "rows"))
        if not isinstance(link_rows, list):
            return False, False
        link_contracts = [
            candidate
            for candidate in link_rows
            if isinstance(candidate, dict) and candidate.get("sample") == sample
        ]
        if (
            len(sample_contracts) != 1
            or len(link_contracts) != 1
            or not shared_var_prefix
        ):
            return False, False
        sample_contract = sample_contracts[0]
        link_contract = link_contracts[0]
        if set(link_contract) != {"sample", "obs_uid", "x_uid", "var_uid"}:
            return False, False
        if not all(
            str(link_contract[key]).strip() for key in ["obs_uid", "x_uid", "var_uid"]
        ):
            return False, False
        x_uid = str(link_contract["x_uid"])
        var_uid = str(link_contract["var_uid"])
        if sum(x_uid in index.get(key, set()) for key in x_keys) != 1:
            return False, False
        if index.get(var_key, set()) != {var_uid}:
            return False, False

        # Prefix and current link-row roles are evidence-bound, but this historical
        # source manifest predates corrected OBS/shared-VAR successors. Without an
        # immutable current OBS key→UID identity, do not claim a complete triplet.
        source_uids = sample_contract["accepted_uids"]
        obs_uid = str(link_contract["obs_uid"])
        linked_uids = {
            "obs": obs_uid,
            "X": x_uid,
            "var": var_uid,
        }
        if source_uids != linked_uids or obs_uid not in index.get(obs_key, set()):
            all_triplets_bound = False
    return True, all_triplets_bound


def _apply_accepted_wave(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    snapshot = json.loads(ACCEPTED_WAVE.read_text())
    integration = _immutable_integration_manifest()
    records = snapshot["datasets"]
    if snapshot["dataset_count"] != 10 or len(records) != 10:
        raise RuntimeError("accepted-wave snapshot must contain exactly ten datasets")
    if len({record["canonical_dataset_id"] for record in records}) != 10:
        raise RuntimeError("accepted-wave canonical dataset IDs must be unique")
    integration_by_dataset = {item["dataset"]: item for item in integration["datasets"]}
    if len(integration_by_dataset) != 10:
        raise RuntimeError("integration manifest must contain ten unique datasets")
    if {record["integration_dataset"] for record in records} != set(
        integration_by_dataset
    ):
        raise RuntimeError("accepted-wave integration dataset identities differ")

    by_id = {row["dataset_id"]: row for row in rows}
    accepted_ids = {record["canonical_dataset_id"] for record in records}
    prior_scoped = {
        dataset_id
        for dataset_id, row in by_id.items()
        if all(
            bool(row[key])
            for key in [
                "strict_obs_validated",
                "strict_var_validated",
                "chunks_or_structure_validated",
                "cleaning_validated",
                "lamin_registered",
                "in_versioned_collection",
            ]
        )
    }
    strict_70_ids = {
        row["dataset_id"]
        for row in rows
        if row["review_scope"] == "strict_real_dataset_curation_70"
    }
    registered_new_ids = {
        row["dataset_id"]
        for row in rows
        if row["review_scope"] == "genuinely_new_family_22"
        and bool(row["lamin_registered"])
        and bool(row["in_versioned_collection"])
    }
    reconciliation = snapshot["denominator_reconciliation"]
    observed = {
        "inventory_rows": len(rows),
        "strict_ledger_rows": len(strict_70_ids),
        "genuinely_new_family_rows": sum(
            row["review_scope"] == "genuinely_new_family_22" for row in rows
        ),
        "accepted_wave_rows": len(accepted_ids),
        "accepted_wave_overlap_with_prior_scoped_complete": len(
            accepted_ids & prior_scoped
        ),
        "accepted_wave_overlap_with_strict_70_exact_ids": len(
            accepted_ids & strict_70_ids
        ),
        "scoped_validation_before": len(prior_scoped),
        "scoped_validation_after": len(prior_scoped | accepted_ids),
    }
    for key, value in observed.items():
        if reconciliation[key] != value:
            raise RuntimeError(
                f"accepted-wave denominator reconciliation drift: {key}={value}"
            )
    if registered_new_ids != accepted_ids:
        raise RuntimeError(
            "accepted-wave IDs must equal the ten registered new-family rows"
        )

    for record in records:
        dataset_id = record["canonical_dataset_id"]
        integration_item = integration_by_dataset[record["integration_dataset"]]
        if (
            record["accepted_head"],
            record["producer_task_id"],
            record["reviewer_task_id"],
        ) != EXPECTED_ACCEPTED_LINEAGE[record["integration_dataset"]]:
            raise RuntimeError(f"accepted-wave lineage drift: {dataset_id}")
        if record["producer_task_id"] == record["reviewer_task_id"]:
            raise RuntimeError(f"accepted-wave review is not independent: {dataset_id}")
        if record["accepted_head"] != integration_item["accepted_sha"]:
            raise RuntimeError(
                f"accepted-wave head is bound to the wrong dataset: {dataset_id}"
            )
        row = by_id.get(dataset_id)
        if row is None or row["review_scope"] != "genuinely_new_family_22":
            raise RuntimeError(
                f"accepted-wave row is missing or mis-scoped: {dataset_id}"
            )
        notebook_path = record["processing_decision_notebook_path"]
        if notebook_path and not (ROOT / notebook_path).is_file():
            raise RuntimeError(f"accepted-wave notebook is missing: {notebook_path}")
        evidence_paths = {
            record["payload_evidence_path"],
            *record["scientific_evidence_paths"],
        }
        if notebook_path:
            evidence_paths.add(notebook_path)
        evidence_bytes = {
            path: _immutable_accepted_file(integration_item, path)
            for path in evidence_paths
        }
        if record["payload_evidence_path"] not in integration_item["receipt_paths"]:
            raise RuntimeError(
                f"accepted-wave payload evidence is not a receipt: {dataset_id}"
            )
        document_paths = {
            record["payload_evidence_path"],
            *record["scientific_evidence_paths"],
        }
        documents = {path: json.loads(evidence_bytes[path]) for path in document_paths}
        receipt = documents[record["payload_evidence_path"]]
        receipt_contract_valid = _receipt_contract_valid(
            record["integration_dataset"], receipt
        )
        payload_prefixes = record["payload_prefixes"]
        if len(payload_prefixes) != int(record["physical_member_count"]):
            raise RuntimeError(
                f"accepted-wave physical-member denominator drift: {dataset_id}"
            )
        if not all(
            prefix.startswith(("data/cleaned/", "pert-gym/logical/"))
            for prefix in payload_prefixes
        ):
            raise RuntimeError(f"accepted-wave payload prefix is invalid: {dataset_id}")
        observation_document_path, observation_path = OBSERVATION_EVIDENCE[
            record["integration_dataset"]
        ]
        if observation_document_path not in documents:
            raise RuntimeError(
                f"accepted-wave observation evidence is unbound: {dataset_id}"
            )
        observations = _resolve_json_path(
            documents[observation_document_path], observation_path
        )
        if not isinstance(observations, int) or observations <= 0:
            raise RuntimeError(
                f"accepted-wave observation evidence is invalid: {dataset_id}"
            )
        if observations != int(record["observations"]):
            raise RuntimeError(
                f"accepted-wave observation denominator drift: {dataset_id}"
            )
        shared_var_prefix = record.get("shared_var_prefix")
        payload_prefixes_bound, explicit_triplet_evidence = (
            _structured_payload_complete(record, receipt, list(documents.values()))
        )
        if not payload_prefixes_bound:
            raise RuntimeError(
                f"accepted-wave payload-prefix evidence is incomplete: {dataset_id}"
            )
        canonical_data_cleaned_payload = (
            explicit_triplet_evidence
            and all(prefix.startswith("data/cleaned/") for prefix in payload_prefixes)
            and (not shared_var_prefix or shared_var_prefix.startswith("data/cleaned/"))
        )
        if bool(record["canonical_data_cleaned_payload"]) != (
            canonical_data_cleaned_payload
        ):
            raise RuntimeError(
                f"accepted-wave canonical data/cleaned disposition drift: {dataset_id}"
            )
        scientific_contract_documented = all(
            [
                record["aliases"],
                record["accepted_head"],
                record["producer_task_id"],
                record["reviewer_task_id"],
                record["scientific_modality"],
                record["experimental_axes"],
                record["outcomes_endpoints"],
                record["annotation_level"],
                record["source_evidence"],
                all(path in documents for path in record["scientific_evidence_paths"]),
            ]
        )
        notebook_present = False
        if notebook_path:
            notebook = json.loads(evidence_bytes[notebook_path])
            notebook_present = (
                notebook.get("nbformat") == 4
                and isinstance(notebook.get("cells"), list)
                and any(cell.get("cell_type") == "code" for cell in notebook["cells"])
            )
        accepted_head_integrated = all(
            path in integration_item["transplanted_paths"] for path in evidence_paths
        )
        accepted_wave_scoped_validation = all(
            [
                receipt_contract_valid,
                accepted_head_integrated,
            ]
        )
        row.update(
            {
                "aliases": json.dumps(record["aliases"], separators=(",", ":")),
                "physical_member_count": len(payload_prefixes),
                "observations": observations,
                "jkobject_dataset_visible": True,
                # These columns belong to the frozen strict-70 ledger. Accepted-wave
                # scope is recorded separately and receives no anticipatory /70 credit.
                "strict_obs_validated": False,
                "strict_var_validated": False,
                "chunks_or_structure_validated": False,
                "cleaning_validated": False,
                "lamin_registered": receipt_contract_valid,
                "in_versioned_collection": receipt_contract_valid,
                "accepted_wave": True,
                "accepted_wave_scoped_validation": accepted_wave_scoped_validation,
                "accepted_wave_head": record["accepted_head"],
                "producer_task_id": record["producer_task_id"],
                "reviewer_task_id": record["reviewer_task_id"],
                "scientific_modality": record["scientific_modality"],
                "experimental_axes": json.dumps(
                    record["experimental_axes"], sort_keys=True, separators=(",", ":")
                ),
                "outcomes_endpoints": json.dumps(
                    record["outcomes_endpoints"], sort_keys=True, separators=(",", ":")
                ),
                "annotation_level": record["annotation_level"],
                "source_evidence": json.dumps(
                    record["source_evidence"], separators=(",", ":")
                ),
                "scientific_contract_documented": scientific_contract_documented,
                # Semantic acceptance belongs to this snapshot's independent review;
                # it is deliberately false while the exact-head inventory PR is open.
                "scientific_contract_bound": False,
                "payload_prefixes": json.dumps(payload_prefixes, separators=(",", ":")),
                "structured_payload_evidence": explicit_triplet_evidence,
                "processing_decision_notebook_present": notebook_present,
                # Presence plus valid nbformat is not execution/replay evidence.
                "processing_decision_notebook": False,
                "processing_decision_notebook_path": notebook_path or "",
                "canonical_data_cleaned_payload": canonical_data_cleaned_payload,
                "accepted_head_integrated": accepted_head_integrated,
                "staging_decommissioned_with_receipt": False,
                "inventory_docs_same_snapshot_accepted": False,
                "exact_head_inventory_pr_merged": False,
                "source_completion_state": (
                    "accepted_wave_scoped_scientific_validation; "
                    "stronger_full_dod_pending"
                ),
                "evidence": (
                    "accepted_10_dataset_review_snapshot.json; "
                    "accepted_10_of_10_integration_manifest.json; exact producer and "
                    "independent reviewer heads"
                ),
            }
        )

    for row in rows:
        _finalize_row(row)
    if sum(bool(row["scoped_scientific_validation_accepted"]) for row in rows) != 18:
        raise RuntimeError("expected exactly 18 scoped scientific validations")
    if any(bool(row["entirely_validated"]) for row in rows):
        raise RuntimeError("stronger full DoD must remain fail-closed in this snapshot")
    return sorted(rows, key=lambda row: row["dataset_id"].lower())


def _build_rows_from_evidence() -> list[dict[str, Any]]:
    snapshot = json.loads(BRANCH_SNAPSHOT.read_text())
    main_values = _snapshot_main_visibility(snapshot)
    rows: list[dict[str, Any]] = []

    with REAL_DATASETS.open(newline="") as handle:
        for source in csv.DictReader(handle, delimiter="\t"):
            dataset_id = source["real_dataset_id"]
            families = _json_list(source["logical_families"])
            categories = set(_json_list(source["collection_categories"]))
            main_baseline = "base_public" in categories
            main_visible = (
                main_baseline
                or dataset_id in MAIN_LIVE_VERIFIED
                or _matches_main(families, main_values)
            )
            # The dated snapshot can contain both added_dataset and branch_revision
            # rows for aliases/helpers. Fresh exact-key branch readback wins.
            if dataset_id in JKOBJECT_ONLY_FULLY_ACCEPTED:
                main_visible = False
            obs_ok = dataset_id in OBS_ACCEPTED
            var_ok = dataset_id in VAR_ACCEPTED
            # Exact accepted OBS and VAR reviews include current OBS→X→VAR identity,
            # row/feature parity, and link readback. Therefore their intersection is
            # sufficient structural and cleaning evidence for this review surface.
            structure_ok = obs_ok and var_ok
            cleaning_ok = obs_ok and var_ok
            # All 70 rows are drawn from the live branch curation crosswalk; main
            # baseline rows are inherited and addition rows have branch catalog evidence.
            lamin_ok = True
            collection_ok = main_baseline or bool(
                categories & {"additions", "base_public"}
            )
            entire = all(
                [obs_ok, var_ok, structure_ok, cleaning_ok, lamin_ok, collection_ok]
            )
            if main_visible:
                branch_disposition = "main_existing_with_jkobject_review_or_revision"
            else:
                branch_disposition = (
                    "jkobject_addition_no_main_match_in_available_evidence"
                )
            row: dict[str, Any] = {
                "dataset_id": dataset_id,
                "review_scope": "strict_real_dataset_curation_70",
                "logical_families": json.dumps(families, separators=(",", ":")),
                "physical_member_count": int(source["physical_member_count"]),
                "observations": int(source["observations"]),
                "main_baseline_crosswalk": main_baseline,
                "main_dataset_or_similar_visible": main_visible,
                "jkobject_dataset_visible": True,
                "branch_disposition": branch_disposition,
                "duplicate_newness_status": (
                    "already_on_or_similar_to_main"
                    if main_visible
                    else "no_main_match_in_available_evidence"
                ),
                "strict_obs_validated": obs_ok,
                "strict_var_validated": var_ok,
                "chunks_or_structure_validated": structure_ok,
                "cleaning_validated": cleaning_ok,
                "lamin_registered": lamin_ok,
                "in_versioned_collection": collection_ok,
                "entirely_validated": entire,
                "entirely_validated_main_existing_dataset": entire and main_visible,
                "entirely_validated_jkobject_addition": entire and not main_visible,
                "source_completion_state": "current_strict_ledger_override_of_2026-07-17_crosswalk",
                "evidence": (
                    "TODO.md/current-status strict OBS+VAR ledgers; "
                    "2026-07-29 bounded live main/jkobject key readback; "
                    "final_real_dataset_obs_var_20260717.tsv"
                ),
            }
            row.update(_default_reconciliation_fields())
            missing = _missing(row)
            row["missing_requirements"] = ";".join(missing)
            row["next_review_focus"] = _focus(missing)
            rows.append(row)

    newness = json.loads(NEWNESS.read_text())
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in newness["events"]:
        if event["newness_class"] == "GENUINELY_NEW":
            grouped[event["target_logical_key"]].append(event)
    if len(grouped) != 22:
        raise RuntimeError(f"expected 22 genuinely-new families, found {len(grouped)}")

    for target_key, events in sorted(grouped.items()):
        record_ids = {event["record_id"] for event in events}
        registered = bool(record_ids & REGISTERED_NEW_RECORD_IDS)
        if bool(record_ids <= REGISTERED_NEW_RECORD_IDS) != registered:
            raise RuntimeError(f"partial registration classification for {target_key}")
        row = {
            "dataset_id": target_key.removeprefix("pert-gym/logical/"),
            "review_scope": "genuinely_new_family_22",
            "logical_families": json.dumps([target_key], separators=(",", ":")),
            "physical_member_count": len(events),
            "observations": "",
            "main_baseline_crosswalk": False,
            "main_dataset_or_similar_visible": False,
            "jkobject_dataset_visible": registered,
            "branch_disposition": "genuinely_new_jkobject_family",
            "duplicate_newness_status": "independently_checked_genuinely_new",
            # Component acceptance proves source/matrix/chunk payload parity, not the
            # separate strict real-dataset OBS/VAR curation gates.
            "strict_obs_validated": False,
            "strict_var_validated": False,
            "chunks_or_structure_validated": True,
            "cleaning_validated": False,
            "lamin_registered": registered,
            "in_versioned_collection": registered,
            "entirely_validated": False,
            "entirely_validated_main_existing_dataset": False,
            "entirely_validated_jkobject_addition": False,
            "source_completion_state": "accepted_component_payload; strict_obs_var_not_yet_accepted",
            "evidence": (
                "accepted_28_newness_reconciliation_20260717.json; "
                "accepted_component_identities_v1.progress.snapshot.json"
            ),
        }
        row.update(_default_reconciliation_fields())
        missing = _missing(row)
        row["missing_requirements"] = ";".join(missing)
        row["next_review_focus"] = _focus(missing)
        rows.append(row)

    if len(rows) != 92 or len({row["dataset_id"] for row in rows}) != 92:
        raise RuntimeError(
            "dataset inventory must contain exactly 92 unique dataset rows"
        )
    return _apply_accepted_wave(rows)


BOOLEAN_COLUMNS = {
    "main_baseline_crosswalk",
    "main_dataset_or_similar_visible",
    "jkobject_dataset_visible",
    "strict_obs_validated",
    "strict_var_validated",
    "chunks_or_structure_validated",
    "cleaning_validated",
    "lamin_registered",
    "in_versioned_collection",
    "entirely_validated",
    "entirely_validated_main_existing_dataset",
    "entirely_validated_jkobject_addition",
    "scoped_scientific_validation_accepted",
    "accepted_wave",
    "accepted_wave_scoped_validation",
    "scientific_contract_documented",
    "scientific_contract_bound",
    "structured_payload_evidence",
    "processing_decision_notebook_present",
    "processing_decision_notebook",
    "canonical_data_cleaned_payload",
    "accepted_head_integrated",
    "staging_decommissioned_with_receipt",
    "inventory_docs_same_snapshot_accepted",
    "exact_head_inventory_pr_merged",
}
INTEGER_COLUMNS = {"physical_member_count", "observations"}


def _read_frozen_rows(path: Path) -> list[dict[str, Any]]:
    """Read the committed review snapshot without needing ignored evidence files."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
        if (
            fieldnames is None
            or fieldnames[: len(LEGACY_COLUMNS)] != LEGACY_COLUMNS
            or not set(fieldnames).issubset(COLUMNS)
        ):
            raise RuntimeError(f"unexpected columns in frozen inventory: {path}")
        rows: list[dict[str, Any]] = []
        for source in reader:
            row: dict[str, Any] = dict(source)
            for column, default in _default_reconciliation_fields().items():
                row.setdefault(column, default)
            for column in BOOLEAN_COLUMNS & row.keys():
                if isinstance(row[column], bool):
                    continue
                value = row[column].strip().lower()
                if value not in {"true", "false"}:
                    raise RuntimeError(f"invalid boolean {column}={row[column]!r}")
                row[column] = value == "true"
            for column in INTEGER_COLUMNS:
                row[column] = int(row[column]) if row[column] else ""
            rows.append(row)
    if len(rows) != 92 or len({row["dataset_id"] for row in rows}) != 92:
        raise RuntimeError(
            "frozen inventory must contain exactly 92 unique dataset rows"
        )
    return _apply_accepted_wave(rows)


def build_rows(
    *, base: Path = BASELINE_INPUT, refresh_from_evidence: bool = False
) -> list[dict[str, Any]]:
    """Canonicalize the frozen snapshot or explicitly rebuild from local evidence."""
    if refresh_from_evidence:
        return _build_rows_from_evidence()
    return _read_frozen_rows(base)


def summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "unique_datasets": len(rows),
        "main_baseline_datasets": sum(bool(r["main_baseline_crosswalk"]) for r in rows),
        "scoped_scientific_validation_accepted": sum(
            bool(r["scoped_scientific_validation_accepted"]) for r in rows
        ),
        "accepted_wave_scoped_validation": sum(
            bool(r["accepted_wave"])
            and bool(r["scoped_scientific_validation_accepted"])
            for r in rows
        ),
        "entirely_validated": sum(bool(r["entirely_validated"]) for r in rows),
        "entirely_validated_main_existing": sum(
            bool(r["entirely_validated_main_existing_dataset"]) for r in rows
        ),
        "entirely_validated_jkobject_additions": sum(
            bool(r["entirely_validated_jkobject_addition"]) for r in rows
        ),
        "new_families_registered_and_in_collection": sum(
            r["review_scope"] == "genuinely_new_family_22"
            and bool(r["lamin_registered"])
            and bool(r["in_versioned_collection"])
            for r in rows
        ),
        "new_families_entirely_validated": sum(
            r["review_scope"] == "genuinely_new_family_22"
            and bool(r["entirely_validated"])
            for r in rows
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, default=BASELINE_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--refresh-from-evidence", action="store_true")
    args = parser.parse_args()
    rows = build_rows(
        base=args.base,
        refresh_from_evidence=args.refresh_from_evidence,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({**summary(rows), "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
