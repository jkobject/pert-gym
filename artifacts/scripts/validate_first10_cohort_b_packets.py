#!/usr/bin/env python3
"""Validate first-10 cohort B evidence packets and immutable source snapshots."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "artifacts/evidence/first10-cohort-b-t_890a73de"
SNAPSHOTS = EVIDENCE / "source_snapshots"
DATASETS = ("GSE194214", "GSE269572", "GSM5901228", "GSM5901229")
EXPECTED_LIVE_SHA256 = (
    "ae453c247b0cee77854a16c9981daf31bcc46d285a1917694397a2d2944da2ce"
)
EXPECTED_PACKET_HASHES = {
    "GSE194214": "24d529b8afc268cd836c0bd8031f64d02240f2b92d858bf8c0323cc76e0bbbb9",
    "GSE269572": "4e48a27b93cb25a54e8e7e43b18ea0456ff46c766cf11d4b4e8dccd038b93c89",
    "GSM5901228": "87cbfa1f1396273a02231ea030944429153ba935b61c90e67d777bf8a6c2bb8b",
    "GSM5901229": "42da284e5dd2c730ac380ea1ac2b0f66a7bb95e799ea9c246835b674a56671d8",
}
EXPECTED_HASHES = {
    "GSE194214_manifest.json": "f1e5d44fd0dc621728c1c6b37bf150650f6dd5cd7d58530b7f52b0f93eaddd1f",
    "GSE269572_manifest.json": "ab3286741d1b41fcdc11003e61e42ad3f598f11cf6c9d65b8ee48161b2866f8c",
    "GSE196799_manifest.json": "3f73fdc9e405279ebbd5e5a4d67ee8b6d32cd0a031b73d5297184f95b6bb7eb3",
    "GSE194214_README.md": "1a6a9709a63dbf5053fe0036508c7a48f9ec733d3bf64a8bfa1fbeeedfde31f2",
    "GSE269572_README.md": "7f45fe08fcac135b766985e93a7250f4185038ac9f65f41358ca88e3e7a51071",
    "GSM5901228_README.md": "0e2d3bf2cd3813823e9c9bb889aef139b09538286c93506a42c801eb317ddc68",
    "GSM5901229_README.md": "b8df0a04225e4a50e7932b523667a0ac68fdd7b5caf5efb772dde30bf7614fb2",
    "GSE194214_family.soft": "4b22135f699519adffb642b101c64a0ea83495e0cf1cbe7a732a2d3d5fa7c5ea",
    "GSE269572_family.soft": "39b44bd33ab4182e640a6a75887931799b3a20f6785def7b1c305cf6344c12dc",
    "GSE196799_family.soft": "175c85af650b2aba25599290ea0d39f1385a957acfa79a07e2ac25a7910084f5",
}
MANIFEST_BY_DATASET = {
    "GSE194214": "GSE194214_manifest.json",
    "GSE269572": "GSE269572_manifest.json",
    "GSM5901228": "GSE196799_manifest.json",
    "GSM5901229": "GSE196799_manifest.json",
}
REQUIRED = {
    "schema_version",
    "task_id",
    "dataset_id",
    "current_identity",
    "source_evidence",
    "obs_audit",
    "obs_decisions",
    "var_audit",
    "var_verdict",
    "x_audit",
    "link_audit",
    "temporal_verdict",
    "chunk_verdict",
    "post_fix_obs_schema",
    "validators",
    "remediation_plan",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_identity(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        role: data["metadata"] | {"feature_links": data["feature_links"]}
        for role, data in entry["artifacts"].items()
    }


def accepted_inventory(manifest: dict[str, Any], dataset: str) -> dict[str, Any]:
    marker = (
        f"/samples/{dataset}/" if dataset.startswith("GSM") else f"/datasets/{dataset}/"
    )
    selected = [
        row for row in manifest["actual_artifact_inventory"] if marker in row["key"]
    ]
    assert len(selected) == 3, (dataset, [row["key"] for row in selected])
    by_role = {Path(row["key"]).name: row for row in selected}
    assert set(by_role) == {"obs.parquet", "X.h5ad", "var.parquet"}
    return {
        "obs": by_role["obs.parquet"],
        "X": by_role["X.h5ad"],
        "var": by_role["var.parquet"],
    }


def load_builder() -> Any:
    path = ROOT / "artifacts/scripts/build_first10_cohort_b_packets.py"
    spec = importlib.util.spec_from_file_location("first10_cohort_b_builder", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_read_only_source(path: Path) -> None:
    source = path.read_text()
    tree = ast.parse(source)
    forbidden_calls = {
        "save",
        "delete",
        "set_values",
        "from_dataframe",
        "from_anndata",
        "track",
        "unlink",
    }
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not (called & forbidden_calls), called & forbidden_calls
    forbidden_text = (
        "revises=",
        "gcloud ",
        "gsutil ",
        "subprocess",
        "os.system",
        "shell=True",
    )
    for value in forbidden_text:
        assert value not in source, value


def main() -> int:
    if not __debug__:
        raise RuntimeError(
            "validation must run without Python optimization; assertions are security gates"
        )
    for name, expected in EXPECTED_HASHES.items():
        actual = digest(SNAPSHOTS / name)
        assert actual == expected, (name, actual, expected)

    live_path = EVIDENCE / "live_lamin_readback.json"
    assert digest(live_path) == EXPECTED_LIVE_SHA256
    live = json.loads(live_path.read_text())
    assert live["instance"] == "laminlabs/pertdata"
    assert live["branch"] == {"name": "jkobject", "uid": "GCjqQtGwPzkY"}
    assert set(live["datasets"]) == set(DATASETS)
    assert live["read_boundary"] == {
        "datasets": list(DATASETS),
        "payloads_loaded": "exactly four obs.parquet and four var.parquet dataframes",
        "X_policy": "registry metadata and feature links only; no X payload load",
        "writes": "none",
    }
    builder = load_builder()
    fetch_receipt = json.loads((SNAPSHOTS / "fetch_receipt.json").read_text())
    soft_receipt = json.loads((SNAPSHOTS / "geo_soft_fetch_receipt.json").read_text())

    for dataset in DATASETS:
        packet_path = EVIDENCE / f"{dataset}.audit.json"
        assert digest(packet_path) == EXPECTED_PACKET_HASHES[dataset]
        packet = json.loads(packet_path.read_text())
        markdown = (EVIDENCE / f"{dataset}.audit.md").read_text()
        entry = live["datasets"][dataset]
        manifest_name = MANIFEST_BY_DATASET[dataset]
        manifest = json.loads((SNAPSHOTS / manifest_name).read_text())
        matrix = (
            manifest["readback"][dataset]
            if dataset.startswith("GSM")
            else manifest["dataset"]["readback"]["matrix"]
        )
        inventory = accepted_inventory(manifest, dataset)
        assert REQUIRED <= packet.keys(), (dataset, REQUIRED - packet.keys())
        assert packet["dataset_id"] == dataset
        assert packet["task_id"] == "t_890a73de"
        assert packet["audit_mode"] == "read_only"
        assert packet["cloud_mutations"] == 0
        expected_current = {
            "instance": live["instance"],
            "branch": live["branch"],
            "artifacts": artifact_identity(entry),
            "bounded_key_candidates": entry["bounded_key_candidates"],
        }
        assert packet["current_identity"] == expected_current
        assert packet["obs_audit"] == entry["obs"]
        assert packet["var_audit"] == entry["var"]
        assert packet["link_audit"] == entry["parity"] | {
            "verdict": "PASS_exact_obs_to_X_to_same_prefix_var"
        }
        expected_obs_key = f"data/cleaned/{dataset}/obs.parquet"
        expected_x_key = f"data/cleaned/{dataset}/X.h5ad"
        expected_var_key = f"data/cleaned/{dataset}/var.parquet"
        assert entry["artifacts"]["obs"]["feature_links"] == {"X": expected_x_key}
        assert entry["artifacts"]["X"]["feature_links"] == {"var": expected_var_key}
        assert entry["artifacts"]["obs"]["metadata"]["key"] == expected_obs_key
        assert entry["artifacts"]["X"]["metadata"]["key"] == expected_x_key
        assert entry["artifacts"]["var"]["metadata"]["key"] == expected_var_key
        assert packet["x_audit"]["manifest_readback"] == matrix
        assert (
            packet["x_audit"]["row_parity"]
            == (entry["obs"]["shape"][0] == matrix["shape"][0])
            is True
        )
        assert (
            packet["x_audit"]["column_parity"]
            == (entry["var"]["shape"][0] == matrix["shape"][1])
            is True
        )
        expected_var_verdict = {
            "species": "Homo sapiens",
            "namespace": "Ensembl gene ID",
            "index_unique": entry["var"]["index_unique"],
            "control_characters": entry["var"]["index_control_character_count"],
            "dimension_matches_X": entry["var"]["shape"][0] == matrix["shape"][1],
        }
        assert packet["var_verdict"] == expected_var_verdict
        assert (
            packet["source_evidence"]["source_identity"] == manifest["source_identity"]
        )
        assert packet["source_evidence"]["acceptance_manifest"] == {
            "local_snapshot": f"source_snapshots/{manifest_name}",
            "sha256": EXPECTED_HASHES[manifest_name],
        }
        assert fetch_receipt[manifest_name]["sha256"] == EXPECTED_HASHES[manifest_name]
        assert (
            fetch_receipt[manifest_name]["size_bytes"]
            == (SNAPSHOTS / manifest_name).stat().st_size
        )
        family = "GSE196799" if dataset.startswith("GSM") else dataset
        soft_name = f"{family}_family.soft.gz"
        assert (
            packet["source_evidence"]["citations"]["soft_sha256"]
            == soft_receipt[soft_name]["sha256"]
        )
        assert (
            soft_receipt[soft_name]["url"]
            == packet["source_evidence"]["citations"]["soft_url"]
        )
        assert (
            f"^SERIES = {family}" in (SNAPSHOTS / f"{family}_family.soft").read_text()
        )
        for role, row in inventory.items():
            readback = row["producer_generation_readback"]
            assert readback["sha256"] == row["sha256"]
            assert readback["size_bytes"] == row["size_bytes"]
            assert entry["artifacts"][role]["metadata"]["size"] == row["size_bytes"]
        assert len(packet["obs_decisions"]) >= 8
        assert len(packet["obs_field_decisions"]) == len(packet["obs_audit"]["columns"])
        assert {row["field"] for row in packet["obs_field_decisions"]} == set(
            packet["obs_audit"]["columns"]
        )
        assert "post_fix_var_schema" in packet
        assert "var_decisions" in packet
        assert len(packet["validators"]) >= 8
        assert len(packet["remediation_plan"]["steps"]) >= 7
        assert dataset in markdown
        assert "Executable remediation" in markdown
        assert "Validators" in markdown
        assert markdown == builder.render(packet)

    assert (
        json.loads((EVIDENCE / "GSE194214.audit.json").read_text())["temporal_verdict"][
            "status"
        ]
        == "temporal_multi_snapshot"
    )
    assert (
        json.loads((EVIDENCE / "GSE269572.audit.json").read_text())["temporal_verdict"][
            "status"
        ]
        == "non_temporal_single_snapshot"
    )
    for dataset in ("GSM5901228", "GSM5901229"):
        packet = json.loads((EVIDENCE / f"{dataset}.audit.json").read_text())
        assert packet["logical_family"] == "GSE196799"
        assert (
            packet["chunk_verdict"]["shared_var_target"]
            == "data/cleaned/GSE196799/var.parquet"
        )
        assert (
            packet["temporal_verdict"]["member_status"]
            == "non_temporal_single_snapshot"
        )
        assert packet["temporal_verdict"]["family_status"] == "temporal_multi_snapshot"

    gse194 = json.loads((EVIDENCE / "GSE194214.audit.json").read_text())
    assert gse194["obs_audit"]["column_audit"]["timepoint"][
        "value_counts_if_at_most_20"
    ] == {"1": 2930, "2": 4977, "3": 5968, "5": 4841}
    gse269 = json.loads((EVIDENCE / "GSE269572.audit.json").read_text())
    assert gse269["obs_audit"]["column_audit"]["timepoint"]["values_if_at_most_20"] == [
        42.5
    ]
    gsm_packets = [
        json.loads((EVIDENCE / f"{dataset}.audit.json").read_text())
        for dataset in ("GSM5901228", "GSM5901229")
    ]
    assert (
        gsm_packets[0]["current_identity"]["artifacts"]["var"]["hash"]
        == gsm_packets[1]["current_identity"]["artifacts"]["var"]["hash"]
    )
    family_manifest = json.loads((SNAPSHOTS / "GSE196799_manifest.json").read_text())
    assert (
        family_manifest["shared_var_identity_sha256"]
        == "4f796e2e5212467c2b54b9a5ff30fbb1ba020686b7edac3df76b996c2a687dd7"
    )
    assert_read_only_source(
        ROOT / "artifacts/scripts/audit_first10_cohort_b_readonly.py"
    )
    print(
        "PASS: 4 JSON + 4 Markdown packets, live readback, and 10 immutable snapshots validated"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
