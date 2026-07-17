#!/usr/bin/env python3
"""Generation-pinned, read-only verification of a CELLxGENE GCS triplet."""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import tempfile
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
from google.cloud import storage

BUFFER_BYTES = 8 * 1024**2


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(BUFFER_BYTES):
            digest.update(block)
    return digest.hexdigest()


def ordered_index_sha256(index: pd.Index) -> str:
    digest = hashlib.sha256()
    for value in index.astype(str):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def download_generation(
    bucket: storage.Bucket, identity: dict[str, Any], destination: Path
) -> None:
    generation = int(identity["generation"])
    blob = bucket.blob(identity["name"], generation=generation)
    blob.download_to_filename(
        destination, if_generation_match=generation, timeout=3600, checksum="crc32c"
    )
    if destination.stat().st_size != identity["size_bytes"]:
        raise RuntimeError(f"size mismatch for {identity['generation_uri']}")
    if sha256_file(destination) != identity["sha256"]:
        raise RuntimeError(f"SHA-256 mismatch for {identity['generation_uri']}")


def frame_schema(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "ordered_index_sha256": ordered_index_sha256(frame.index),
        "columns": [
            {
                "name": str(column),
                "dtype": str(frame[column].dtype),
                "null_count": int(frame[column].isna().sum()),
            }
            for column in frame.columns
        ],
        "total_null_count": int(frame.isna().sum().sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-generation", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    plan = json.loads(args.plan.read_text())
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    if socket.gethostname().split(".")[0] != plan["execution"]["host"]:
        raise RuntimeError("verification must run on the exact EU worker")
    if manifest["execution_plan_sha256"] != sha256_file(args.plan):
        raise RuntimeError("execution plan hash mismatch")
    if manifest["writer_sha256"] != plan["writer_sha256"]:
        raise RuntimeError("writer hash mismatch")

    client = storage.Client(project=plan["execution"]["billing_project"])
    bucket = client.bucket(
        plan["storage"]["bucket"], user_project=plan["execution"]["billing_project"]
    )
    manifest_identity = {
        "name": plan["storage"]["manifest_name"],
        "generation": args.manifest_generation,
        "generation_uri": (
            f"gs://{plan['storage']['bucket']}/{plan['storage']['manifest_name']}"
            f"#{args.manifest_generation}"
        ),
        "size_bytes": len(manifest_bytes),
        "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }

    logical_root = f"{plan['storage']['root'].rstrip('/')}/{plan['logical_key']}"
    workload_name = f"{logical_root}/workloads/temporal-v4-025-wave05-ab1405c531952b7a/manifest.json"
    expected_names = {workload_name, manifest_identity["name"]}
    record = manifest["datasets"][0]
    for identity in record["objects"].values():
        expected_names.add(identity["name"])
    actual_names = {
        blob.name for blob in client.list_blobs(bucket, prefix=logical_root)
    }
    if actual_names != expected_names:
        raise RuntimeError(
            f"logical-key inventory mismatch: {sorted(actual_names)} != {sorted(expected_names)}"
        )

    payload_generations = [
        int(identity["generation"]) for identity in record["objects"].values()
    ]
    if int(args.manifest_generation) <= max(payload_generations):
        raise RuntimeError("manifest was not written after every payload")

    with tempfile.TemporaryDirectory(prefix="row25-independent-readback-") as raw_temp:
        temp = Path(raw_temp)
        remote_manifest = temp / "materialization-manifest.json"
        download_generation(bucket, manifest_identity, remote_manifest)
        if remote_manifest.read_bytes() != manifest_bytes:
            raise RuntimeError("generation-pinned manifest differs from evidence bytes")

        paths: dict[str, Path] = {}
        for role, identity in record["objects"].items():
            path = temp / role
            download_generation(bucket, identity, path)
            paths[role] = path

        obs = pd.read_parquet(paths["obs"])
        var = pd.read_parquet(paths["var"])
        matrix = ad.read_h5ad(paths["X"], backed="r")
        try:
            shape = list(matrix.shape)
            obs_axis_equal = matrix.obs_names.astype(str).equals(obs.index.astype(str))
            var_axis_equal = matrix.var_names.astype(str).equals(var.index.astype(str))
            sample_rows = sorted({0, shape[0] // 2, shape[0] - 1})
            sample_nnz = {
                str(row): int(
                    matrix.X[row].nnz
                    if hasattr(matrix.X[row], "nnz")
                    else (matrix.X[row] != 0).sum()
                )
                for row in sample_rows
            }
        finally:
            matrix.file.close()

    if shape != [108_838, 32_055]:
        raise RuntimeError(f"matrix shape mismatch: {shape}")
    if not obs_axis_equal or not var_axis_equal:
        raise RuntimeError("obs -> X -> var axis linkage mismatch")
    if frame_schema(obs) != {
        key: record["obs"][key]
        for key in ("rows", "ordered_index_sha256", "columns", "total_null_count")
    }:
        raise RuntimeError("obs schema/null inventory mismatch")
    if frame_schema(var) != {
        key: record["var"][key]
        for key in ("rows", "ordered_index_sha256", "columns", "total_null_count")
    }:
        raise RuntimeError("var schema/null inventory mismatch")
    if (
        record["source"]["sha256"]
        != "c2528b8c1eaea03979b367b86744e5aa2e13de0ac1c9c82f433229e082e9411f"
    ):
        raise RuntimeError("source payload SHA-256 mismatch")
    if (
        record["shared_var_identity"]["sha256"]
        != "2e38a37f56c4e91819f8bbe098118221e2cb1bcd7160d86df0f61101a72ee246"
    ):
        raise RuntimeError("shared-var identity mismatch")

    result = {
        "schema_version": "pert-gym.cellxgene-triplet-independent-readback/v1",
        "status": "PASS",
        "host": socket.gethostname().split(".")[0],
        "logical_key": plan["logical_key"],
        "inventory_exact": sorted(actual_names),
        "manifest": manifest_identity,
        "opened_members": ["obs.parquet", "X.h5ad", "var.parquet"],
        "generation_pinned_hashes_equal": True,
        "shape": shape,
        "obs_schema": frame_schema(obs),
        "var_schema": frame_schema(var),
        "obs_to_X_axis_equal": obs_axis_equal,
        "X_to_var_axis_equal": var_axis_equal,
        "sample_nnz": sample_nnz,
        "source_payload_sha256": record["source"]["sha256"],
        "shared_var_identity": record["shared_var_identity"],
        "manifest_last": True,
        "excluded_records": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": "PASS", "shape": shape, "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
