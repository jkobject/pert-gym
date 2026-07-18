#!/usr/bin/env python3
"""Verify the sealed local remediation packet without reading remote payloads."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).parent
OLD_REVISION = "temporal-v4-095-wave10-b1dee01a49f4c78c"
NEW_REVISION = "temporal-v4-095-wave10-c1f63c6ec90e4c24"
SEALED_BUILDER_SHA = "c50fdc6927d9ad20e120979a2200c4f51d7ba264458f0d3f39dc9a9bb2434ee7"
EXPECTED_COUNTS = {
    "observations": 30_496,
    "variables": 23_961,
    "nnz_stored": 159_634_893,
    "finite_value_sum": 1_054_824_189,
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_inventory(root: Path, inventory_path: Path) -> None:
    """Verify the sealed packet is exact, complete, and path-safe."""
    inventory = json.loads(inventory_path.read_text())
    entries = inventory.get("files")
    if not isinstance(entries, list):
        raise RuntimeError("evidence inventory files must be a list")

    sealed_paths: set[str] = set()
    resolved_root = root.resolve()
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise RuntimeError("evidence inventory entry is malformed")
        relative = entry["path"]
        if relative in sealed_paths:
            raise RuntimeError(f"duplicate inventory path: {relative}")
        sealed_paths.add(relative)
        path = root / relative
        try:
            path.resolve().relative_to(resolved_root)
        except ValueError as exc:
            raise RuntimeError(f"inventory path escapes packet: {relative}") from exc
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"missing inventory path: {relative}")
        size = path.stat().st_size
        digest = sha256(path)
        if size != entry.get("size_bytes") or digest != entry.get("sha256"):
            raise RuntimeError(f"inventory identity mismatch: {relative}")

    inventory_relative = inventory_path.relative_to(root).as_posix()
    actual_paths = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
        and not path.is_symlink()
        and path.relative_to(root).as_posix() != inventory_relative
        and "__pycache__" not in path.parts
        and path.suffix != ".pyc"
    }
    extra = sorted(actual_paths.difference(sealed_paths))
    if extra:
        raise RuntimeError(f"extra packet entries absent from inventory: {extra}")


def main() -> int:
    verify_inventory(ROOT, ROOT / "evidence-sha256.json")
    output = ROOT / "remote-output" / "output"
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest_object = json.loads((output / "manifest-object.json").read_text())
    ledger = json.loads((output / "ledger.json").read_text())
    readback = json.loads((ROOT / "independent-readback.json").read_text())
    immutable = json.loads((ROOT / "immutability-readback.json").read_text())

    assert manifest_path.read_bytes() == (output / "manifest-readback.json").read_bytes()
    assert sha256(manifest_path) == manifest_object["sha256"] == readback["manifest_sha256"]
    assert manifest_object["generation"] == "1784286707609127"
    assert manifest["revision"] == NEW_REVISION
    assert manifest["supersedes_revision"] == OLD_REVISION
    assert manifest["immutability"]["superseded_revision_mutated"] is False
    assert manifest["provenance"]["builder_script_sha256"] == SEALED_BUILDER_SHA
    assert readback["verdict"] == "PASS"
    assert readback["counts"] == EXPECTED_COUNTS
    assert readback["physical_sparse_encoding"] == {
        "format": "csc",
        "physical_encoding_type": "csc_matrix",
        "shape": [30_496, 23_961],
        "indptr_length": 23_962,
        "expected_indptr_length": 23_962,
    }
    assert manifest["dataset"]["readback"]["matrix"]["format"] == "csc"
    assert manifest["dataset"]["readback"]["matrix"]["physical_encoding_type"] == "csc_matrix"
    assert manifest["dataset"]["readback"]["matrix"]["indptr_length"] == 23_962
    assert manifest["missingness"]["excluded_records"] == 0
    assert manifest["missingness"]["excluded_observations"] == 0
    assert manifest["missingness"]["dependency_created"] is False
    assert ledger["accepted_delta_at_build"] == 0
    assert ledger["debits"] == {"dropped_observations": 0, "exclusions": 0, "product_credit": 0}
    assert ledger["input_accounting"]["expected_inputs"] == ledger["input_accounting"]["accounted_inputs"] == 8
    assert ledger["output_accounting"]["expected_outputs"] == ledger["output_accounting"]["accounted_outputs"] == 5
    assert immutable["revision_count"] == 2
    assert immutable["revisions"][0]["revision"] == OLD_REVISION
    assert immutable["revisions"][0]["manifest_generation"] == "1784259356995184"
    assert immutable["revisions"][0]["mutated"] is False
    print(
        "REMEDIATION_EVIDENCE_PASS "
        f"revision={NEW_REVISION} manifest={readback['manifest_sha256']} "
        "encoding=csc_matrix indptr=23962 product_credit=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
