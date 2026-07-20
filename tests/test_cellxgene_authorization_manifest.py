from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).parents[1]
REVIEW = ROOT / "artifacts/review/temporal-v4-099-parquet-parity-v1"
CONTRACT_PATH = REVIEW / "writer_contract.py"
MANIFEST_PATH = REVIEW / "writer-authorization-manifest.json"
MANIFEST_DIGEST_PATH = REVIEW / "writer-authorization-manifest.sha256"
WRITER = REVIEW / "write_component.py"
HELPER = REVIEW / "parquet_frame_parity.py"
LEDGER_HELPER = REVIEW / "live_ledger_control_plane.py"


def load_contract_module():
    spec = importlib.util.spec_from_file_location(
        "cellxgene_manifest_writer_contract", CONTRACT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_writer_module():
    sys.path.insert(0, str(REVIEW))
    try:
        spec = importlib.util.spec_from_file_location(
            "cellxgene_manifest_writer", WRITER
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(REVIEW))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(tmp_path: Path, manifest: dict[str, Any]) -> tuple[Path, Path]:
    path = tmp_path / "manifest.json"
    digest_path = tmp_path / "manifest.sha256"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    digest_path.write_text(f"{sha256(path)}  {path.name}\n")
    return path, digest_path


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def test_manifest_migrates_existing_authorized_rows_as_data_entries() -> None:
    contract = load_contract_module()

    validated = contract.load_authorization_manifest(
        MANIFEST_PATH,
        MANIFEST_DIGEST_PATH,
        writer_path=WRITER,
        helper_path=HELPER,
        ledger_helper_path=LEDGER_HELPER,
        now="2026-07-16T12:00:00Z",
    )

    assert validated.manifest_sha256 == sha256(MANIFEST_PATH)
    assert [entry["record_id"] for entry in validated.entries] == [
        "temporal_v4_007_a_novel_human_fetal_lung_derived_alveolar_organoid_model_reveals_mechanisms_of_s",
        "temporal_v4_055_type_i_interferon_responsive_microglia_shape_cortical_development_and_behavior",
        "temporal_v4_111_transcriptomic_analysis_of_air_liquid_interface_culture_in_human_lung_organoids",
    ]
    assert all(
        entry["review_provenance"]["status"] == "completed"
        for entry in validated.entries
    )
    assert all(
        entry["missingness_policy"]["mode"] == "explicit-only/v1"
        for entry in validated.entries
    )


def test_writer_accepts_only_the_reviewed_in_tree_manifest_paths(
    tmp_path: Path,
) -> None:
    writer = load_writer_module()

    assert writer.require_reviewed_manifest_paths(
        MANIFEST_PATH, MANIFEST_DIGEST_PATH
    ) == (MANIFEST_PATH.resolve(), MANIFEST_DIGEST_PATH.resolve())

    copied_manifest = tmp_path / MANIFEST_PATH.name
    copied_digest = tmp_path / MANIFEST_DIGEST_PATH.name
    copied_manifest.write_bytes(MANIFEST_PATH.read_bytes())
    copied_digest.write_bytes(MANIFEST_DIGEST_PATH.read_bytes())
    with pytest.raises(RuntimeError, match="reviewed in-tree authorization manifest"):
        writer.require_reviewed_manifest_paths(copied_manifest, copied_digest)


@pytest.mark.parametrize("record", [7, 55, 111])
def test_manifest_exact_membership_authorizes_each_migrated_config(record: int) -> None:
    contract = load_contract_module()
    config_path = REVIEW / f"row-{record}-config.json"

    validated = contract.load_manifest_contract(
        config_path,
        MANIFEST_PATH,
        MANIFEST_DIGEST_PATH,
        writer_path=WRITER,
        helper_path=HELPER,
        require_execution=True,
        now="2026-07-16T12:00:00Z",
    )

    assert validated.config["catalogue_record"] == validated.manifest_entry["record_id"]
    assert validated.manifest_entry["config_sha256"] == sha256(config_path)
    assert validated.manifest_entry["source"] == validated.config["source"]
    assert validated.manifest_entry["http_identity"] == validated.config["source_head"]
    assert validated.manifest_entry["shape"] == validated.config["shape"]
    assert (
        validated.manifest_entry["species"]
        == validated.config["api_identity"]["organism"]
    )
    assert (
        validated.manifest_entry["assays"] == validated.config["api_identity"]["assays"]
    )
    assert validated.manifest_entry["family_lease"] == validated.config["logical_key"]
    assert validated.manifest_entry["config_identity"][
        "metadata_completeness_findings"
    ] == validated.config.get("metadata_completeness_findings", [])


def test_row_7_manifest_explicitly_authorizes_documented_age_missingness() -> None:
    manifest = load_manifest()
    entry = manifest["entries"][0]
    config = json.loads((REVIEW / "row-7-config.json").read_text())

    assert entry["missingness_policy"] == {
        "mode": "explicit-only/v1",
        "allowed_unknown_fields": ["age"],
        "invent_values": False,
    }
    assert (
        entry["config_identity"]["metadata_completeness_findings"]
        == config["metadata_completeness_findings"]
    )


def test_manifest_digest_stale_writer_and_stale_config_fail_closed(
    tmp_path: Path,
) -> None:
    contract = load_contract_module()
    manifest = load_manifest()
    path, digest_path = write_manifest(tmp_path, manifest)

    digest_path.write_text(f"{'0' * 64}  {path.name}\n")
    with pytest.raises(RuntimeError, match="manifest SHA-256"):
        contract.load_authorization_manifest(
            path,
            digest_path,
            writer_path=WRITER,
            helper_path=HELPER,
            ledger_helper_path=LEDGER_HELPER,
            now="2026-07-16T12:00:00Z",
        )

    path, digest_path = write_manifest(tmp_path, manifest)
    stale_writer = tmp_path / "writer.py"
    stale_writer.write_bytes(WRITER.read_bytes() + b"\n")
    with pytest.raises(RuntimeError, match="writer SHA-256"):
        contract.load_authorization_manifest(
            path,
            digest_path,
            writer_path=stale_writer,
            helper_path=HELPER,
            ledger_helper_path=LEDGER_HELPER,
            now="2026-07-16T12:00:00Z",
        )

    stale_config = tmp_path / "row-7-config.json"
    config = json.loads((REVIEW / "row-7-config.json").read_text())
    config["shape"][0] += 1
    stale_config.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    with pytest.raises(RuntimeError, match="config SHA-256|exact manifest entry"):
        contract.load_manifest_contract(
            stale_config,
            path,
            digest_path,
            writer_path=WRITER,
            helper_path=HELPER,
            require_execution=True,
            now="2026-07-16T12:00:00Z",
        )


@pytest.mark.parametrize(
    ("issued_at", "expires_at"),
    [
        ("2027-07-16T00:00:00Z", "2028-07-16T00:00:00Z"),
        ("2025-07-16T00:00:00Z", "2026-07-15T00:00:00Z"),
    ],
)
def test_manifest_rejects_not_yet_valid_or_expired_windows(
    tmp_path: Path, issued_at: str, expires_at: str
) -> None:
    contract = load_contract_module()
    manifest = load_manifest()
    manifest["issued_at"] = issued_at
    manifest["expires_at"] = expires_at
    path, digest_path = write_manifest(tmp_path, manifest)

    with pytest.raises(RuntimeError, match="expired|validity window|not yet valid"):
        contract.load_authorization_manifest(
            path,
            digest_path,
            writer_path=WRITER,
            helper_path=HELPER,
            ledger_helper_path=LEDGER_HELPER,
            now="2026-07-16T12:00:00Z",
        )


@pytest.mark.parametrize("conflict", [False, True])
def test_manifest_rejects_duplicate_or_conflicting_record_entries(
    tmp_path: Path, conflict: bool
) -> None:
    contract = load_contract_module()
    manifest = load_manifest()
    duplicate = json.loads(json.dumps(manifest["entries"][0]))
    if conflict:
        duplicate["source"]["dataset_id"] = "00000000-0000-0000-0000-000000000000"
    manifest["entries"].append(duplicate)
    path, digest_path = write_manifest(tmp_path, manifest)

    with pytest.raises(ValueError, match="duplicate|conflicting"):
        contract.load_authorization_manifest(
            path,
            digest_path,
            writer_path=WRITER,
            helper_path=HELPER,
            ledger_helper_path=LEDGER_HELPER,
            now="2026-07-16T12:00:00Z",
        )


def test_missingness_is_explicit_authorization_policy_not_invented_metadata(
    tmp_path: Path,
) -> None:
    contract = load_contract_module()
    manifest = load_manifest()
    entry = manifest["entries"][0]
    entry["missingness_policy"] = {
        "mode": "explicit-only/v1",
        "allowed_unknown_fields": ["development_stage"],
        "invent_values": False,
    }
    path, digest_path = write_manifest(tmp_path, manifest)

    validated = contract.load_authorization_manifest(
        path,
        digest_path,
        writer_path=WRITER,
        helper_path=HELPER,
        ledger_helper_path=LEDGER_HELPER,
        now="2026-07-16T12:00:00Z",
    )

    assert validated.entries[0]["missingness_policy"]["allowed_unknown_fields"] == [
        "development_stage"
    ]
    assert validated.entries[0]["missingness_policy"]["invent_values"] is False


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("reviewer", "", "non-empty"),
        ("parent_task_id", "", "non-empty"),
        ("correction_task_id", "", "non-empty"),
        ("scope", "", "non-empty"),
        ("scope", "partial-review", "exact-head independent review"),
        ("reviewed_at", "2026-07-16T00:00:01Z", "reviewed_at"),
        ("reviewed_at", "2026-07-17T00:00:00Z", "reviewed_at"),
    ],
)
def test_manifest_rejects_incomplete_or_incoherent_entry_review_provenance(
    tmp_path: Path, field: str, value: str, match: str
) -> None:
    contract = load_contract_module()
    manifest = load_manifest()
    manifest["entries"][0]["review_provenance"][field] = value
    path, digest_path = write_manifest(tmp_path, manifest)

    with pytest.raises((RuntimeError, ValueError), match=match):
        contract.load_authorization_manifest(
            path,
            digest_path,
            writer_path=WRITER,
            helper_path=HELPER,
            ledger_helper_path=LEDGER_HELPER,
            now="2026-07-16T12:00:00Z",
        )


def test_new_mouse_manifest_entry_needs_no_row_specific_validator_change(
    tmp_path: Path,
) -> None:
    contract = load_contract_module()
    manifest = load_manifest()
    config = json.loads((REVIEW / "row-55-config.json").read_text())
    entry = copy.deepcopy(manifest["entries"][1])
    record_id = "temporal_v4_056_synthetic_mouse_dataset"
    task_id = "t_synthetic_mouse"
    logical_key = "pert-gym/logical/temporal/synthetic_mouse_dataset"
    config["catalogue_record"] = record_id
    config["task_id"] = task_id
    config["authorization_binding"]["approved_parent_protocol"] = (
        "temporal-v4-056-parquet-parity-parent/v1"
    )
    config["authorization_binding"]["correction_task_id"] = task_id
    config["logical_key"] = logical_key
    config["obs"]["assignments"][0]["value"] = logical_key
    config["revision"]["prefix"] = "temporal-v4-056"
    config["execution"]["output_directory"] = f"/tmp/temporal-v4-056-{task_id}"
    config_path = tmp_path / "row-56-config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    entry["record_id"] = record_id
    entry["config_sha256"] = sha256(config_path)
    entry["source_packet_sha256"] = sha256(config_path)
    entry["task_id"] = task_id
    entry["family_lease"] = logical_key
    entry["revision_prefix"] = "temporal-v4-056"
    entry["review_provenance"]["correction_task_id"] = task_id
    entry["config_identity"]["obs"] = copy.deepcopy(config["obs"])
    entry["config_identity"]["execution"] = copy.deepcopy(config["execution"])
    manifest["entries"].append(entry)
    path, digest_path = write_manifest(tmp_path, manifest)

    validated = contract.load_manifest_contract(
        config_path,
        path,
        digest_path,
        writer_path=WRITER,
        helper_path=HELPER,
        require_execution=True,
        now="2026-07-16T12:00:00Z",
    )
    assert validated.manifest_entry["species"] == {
        "label": "Mus musculus",
        "ontology_term_id": "NCBITaxon:10090",
    }
