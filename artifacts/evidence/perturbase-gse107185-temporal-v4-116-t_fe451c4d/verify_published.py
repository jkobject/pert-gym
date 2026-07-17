#!/usr/bin/env python3
"""Fresh-process generation-pinned verification of the GSE107185 component."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import zarr
from scipy import sparse

RECORD_ID = "temporal_v4_116_perturbase_gse107185"
LOGICAL_KEY = "pert-gym/logical/temporal/perturbase_gse107185"
SOURCE_SHA256 = "e5d2fa6f7a3c3faced2649e53a5226a41e4b93b6278b375e5f09346739e0bbfa"
EXPECTED_SHAPE = [8428, 2000]
EXPECTED_NNZ = 6377510
EXPECTED_WAVE = 11
BILLING_PROJECT = "jkobject-1549353370965"


def run(command: list[str]) -> str:
    return subprocess.run(command, check=True, text=True, capture_output=True).stdout


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_array(value: np.ndarray) -> str:
    value = np.ascontiguousarray(value)
    digest = hashlib.sha256()
    digest.update(str(value.dtype).encode())
    digest.update(json.dumps(list(value.shape)).encode())
    digest.update(value.tobytes())
    return digest.hexdigest()


def frame_semantic_sha256(frame: pd.DataFrame) -> str:
    hashed = pd.util.hash_pandas_object(frame, index=True, categorize=True).to_numpy(dtype=np.uint64)
    digest = hashlib.sha256()
    digest.update(json.dumps(list(map(str, frame.columns))).encode())
    digest.update(json.dumps([str(dtype) for dtype in frame.dtypes]).encode())
    digest.update(hashed.tobytes())
    return digest.hexdigest()


def read_matrix(path: Path) -> sparse.csr_matrix:
    store = zarr.storage.ZipStore(str(path), mode="r")
    try:
        group = zarr.open_group(store=store, mode="r")
        return sparse.csr_matrix((np.asarray(group["data"]), np.asarray(group["indices"]), np.asarray(group["indptr"])), shape=tuple(group.attrs["shape"]))
    finally:
        store.close()


def matrix_identity(matrix: sparse.csr_matrix) -> dict[str, Any]:
    return {
        "format": "csr_matrix",
        "shape": list(matrix.shape),
        "nnz": int(matrix.nnz),
        "dtype": str(matrix.dtype),
        "sum": float(matrix.sum(dtype=np.float64)),
        "minimum": float(matrix.data.min()),
        "maximum": float(matrix.data.max()),
        "negative_values": int((matrix.data < 0).sum()),
        "data_sha256": sha256_array(matrix.data),
        "indices_sha256": sha256_array(matrix.indices),
        "indptr_sha256": sha256_array(matrix.indptr),
    }


def copy_generation(identity: dict[str, Any], path: Path) -> None:
    run(["gcloud", "storage", "cp", f"--billing-project={BILLING_PROJECT}", identity["generation_uri"], str(path)])
    if path.stat().st_size != identity["size_bytes"] or sha256(path) != identity["sha256"]:
        raise RuntimeError(f"generation identity mismatch: {identity['generation_uri']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-generation-uri", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="verify-gse107185-") as tmp_name:
        tmp = Path(tmp_name)
        manifest_path = tmp / "manifest.json"
        run(["gcloud", "storage", "cp", f"--billing-project={BILLING_PROJECT}", args.manifest_generation_uri, str(manifest_path)])
        if sha256(manifest_path) != args.manifest_sha256:
            raise RuntimeError("manifest generation checksum mismatch")
        manifest = json.loads(manifest_path.read_text())
        if manifest["record_id"] != RECORD_ID or manifest["target_logical_key"] != LOGICAL_KEY:
            raise RuntimeError("manifest logical identity mismatch")
        if manifest["bounded_wave"]["number"] != EXPECTED_WAVE or manifest["bounded_wave_duplicate_check"]["assignment_count"] != 1:
            raise RuntimeError("bounded-wave identity mismatch")
        if manifest["source_identity"]["archive"]["sha256"] != SOURCE_SHA256:
            raise RuntimeError("source archive identity mismatch")
        if manifest["observation_count"] != EXPECTED_SHAPE[0] or manifest["variable_count"] != EXPECTED_SHAPE[1]:
            raise RuntimeError("manifest denominator mismatch")
        if manifest["missingness"]["excluded_observations"] != 0 or not manifest["missingness"]["non_excluding"]:
            raise RuntimeError("missingness was misclassified")
        if manifest["ledger"]["accepted_delta_at_build"] != 0 or manifest["ledger"]["observation_accounting"]["materialized_n_obs"] != EXPECTED_SHAPE[0]:
            raise RuntimeError("proposed ledger is unbalanced")

        source_path = tmp / "source.tar.gz"
        copy_generation(manifest["source_object"], source_path)
        if sha256(source_path) != SOURCE_SHA256:
            raise RuntimeError("source immutable copy mismatch")

        by_role = {item["role"]: item for item in manifest["actual_artifact_inventory"]}
        expected_roles = {"canonical_obs", "canonical_X_processed_mixscape_hvg", "canonical_var"}
        if set(by_role) != expected_roles:
            raise RuntimeError(f"artifact role mismatch: {set(by_role)}")
        obs_path = tmp / "obs.parquet"
        x_path = tmp / "X.zarr.zip"
        var_path = tmp / "var.parquet"
        copy_generation(by_role["canonical_obs"], obs_path)
        copy_generation(by_role["canonical_X_processed_mixscape_hvg"], x_path)
        copy_generation(by_role["canonical_var"], var_path)
        obs = pd.read_parquet(obs_path)
        var = pd.read_parquet(var_path)
        matrix = read_matrix(x_path)
        observed_matrix = matrix_identity(matrix)
        if list(matrix.shape) != EXPECTED_SHAPE or matrix.nnz != EXPECTED_NNZ:
            raise RuntimeError("matrix shape/nnz mismatch")
        if frame_semantic_sha256(obs) != manifest["dataset"]["obs"]["semantic_sha256"]:
            raise RuntimeError("obs semantic mismatch")
        if frame_semantic_sha256(var) != manifest["dataset"]["var"]["semantic_sha256"]:
            raise RuntimeError("var semantic mismatch")
        if observed_matrix != manifest["dataset"]["X"]:
            raise RuntimeError("matrix semantic mismatch")
        if len(obs) != matrix.shape[0] or len(var) != matrix.shape[1]:
            raise RuntimeError("triplet axis parity mismatch")
        if obs.index.duplicated().any() or var.index.duplicated().any():
            raise RuntimeError("triplet axes are not unique")

        revision_prefix = manifest["revision_prefix"]
        listed = sorted(line for line in run(["gcloud", "storage", "ls", f"--billing-project={BILLING_PROJECT}", f"{revision_prefix}/**"]).splitlines() if line.strip())
        expected_suffixes = ["/datasets/extend_61/X.zarr.zip", "/datasets/extend_61/obs.parquet", "/datasets/extend_61/var.parquet", "/ledger.json", "/manifest.json", "/source/extend_61.filter.tar.gz"]
        if len(listed) != 6 or not all(any(item.endswith(suffix) for item in listed) for suffix in expected_suffixes):
            raise RuntimeError(f"revision object inventory mismatch: {listed}")

        result = {
            "schema_version": "pert-gym.independent-generation-readback/v1",
            "verdict": "PASS",
            "record_id": RECORD_ID,
            "target_logical_key": LOGICAL_KEY,
            "manifest_generation_uri": args.manifest_generation_uri,
            "manifest_sha256": args.manifest_sha256,
            "source_sha256": SOURCE_SHA256,
            "scope": {"catalogue_rows": [116], "datasets": 1, "triplets": 1, "payload_objects": 3, "revision_objects": len(listed)},
            "counts": {"observations": len(obs), "variables": len(var), "nnz": int(matrix.nnz), "perturbations": int(obs["perturbation"].nunique()), "media": {str(k): int(v) for k, v in obs["media"].value_counts().items()}},
            "checks": {
                "generation_checksums": True,
                "ordered_axis_parity": True,
                "unique_axes": True,
                "obs_semantic_hash": True,
                "var_semantic_hash": True,
                "matrix_semantic_hashes": True,
                "bounded_wave_identity": True,
                "missingness_non_excluding": True,
                "proposed_ledger_balanced_zero_credit": True,
                "manifest_last_declared": manifest["manifest_last"],
                "complete_revision_inventory": True,
            },
            "matrix_identity": observed_matrix,
            "revision_objects": listed,
            "product_credit": 0,
        }
        args.output.write_bytes((json.dumps(result, indent=2, sort_keys=True) + "\n").encode())
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
