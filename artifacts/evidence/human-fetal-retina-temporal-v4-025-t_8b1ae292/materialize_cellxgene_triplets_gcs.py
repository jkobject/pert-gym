#!/usr/bin/env python3
"""Materialize immutable CELLxGENE obs/X/var triplets on the EU worker.

The source is streamed to a temporary local file, then read through h5py-backed
objects. X is copied dataset-by-dataset in bounded first-axis chunks. Every GCS
payload is uploaded create-only and read back at its recorded generation before
the materialization manifest is uploaded last.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import resource
import socket
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import numpy as np
import pandas as pd
from anndata._io.specs import read_elem, write_elem
from google.cloud import storage

BUFFER_BYTES = 8 * 1024**2
COPY_CHUNK_BYTES = 32 * 1024**2


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode()


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


def frame_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "index_name": frame.index.name,
        "index_dtype": str(frame.index.dtype),
        "index_unique": bool(frame.index.is_unique),
        "index_null_count": int(frame.index.isna().sum()),
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


def source_head(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request, timeout=60) as response:
        return {
            "status": int(response.status),
            "final_url": response.url,
            "content_length": int(response.headers["Content-Length"]),
            "etag": response.headers["ETag"],
            "last_modified": response.headers["Last-Modified"],
            "version_id": response.headers.get("x-amz-version-id"),
        }


def stream_source(url: str, destination: Path, expected_size: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    with (
        urllib.request.urlopen(url, timeout=180) as response,
        destination.open("xb") as output,
    ):
        while block := response.read(BUFFER_BYTES):
            output.write(block)
            digest.update(block)
            count += len(block)
    if count != expected_size:
        raise RuntimeError(f"source size mismatch: {count} != {expected_size}")
    return digest.hexdigest(), count


def _dataset_create_kwargs(source: h5py.Dataset) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if source.chunks is not None:
        kwargs["chunks"] = source.chunks
    if source.compression is not None:
        kwargs["compression"] = source.compression
        kwargs["compression_opts"] = source.compression_opts
    if source.shuffle:
        kwargs["shuffle"] = True
    if source.fletcher32:
        kwargs["fletcher32"] = True
    if source.scaleoffset is not None:
        kwargs["scaleoffset"] = source.scaleoffset
    return kwargs


def copy_dataset_chunked(
    source: h5py.Dataset, destination_group: h5py.Group, name: str
) -> None:
    target = destination_group.create_dataset(
        name, shape=source.shape, dtype=source.dtype, **_dataset_create_kwargs(source)
    )
    for key, value in source.attrs.items():
        target.attrs[key] = value
    if source.shape == ():
        target[()] = source[()]
        return
    if source.shape[0] == 0:
        return
    trailing_values = 1
    for length in source.shape[1:]:
        trailing_values *= max(1, int(length))
    rows = max(1, COPY_CHUNK_BYTES // max(1, source.dtype.itemsize * trailing_values))
    if source.chunks:
        rows = max(
            int(source.chunks[0]), rows // int(source.chunks[0]) * int(source.chunks[0])
        )
    for start in range(0, source.shape[0], rows):
        stop = min(source.shape[0], start + rows)
        target[start:stop, ...] = source[start:stop, ...]


def copy_node_chunked(
    source: h5py.Group | h5py.Dataset, destination: h5py.Group, name: str
) -> None:
    if isinstance(source, h5py.Dataset):
        copy_dataset_chunked(source, destination, name)
        return
    group = destination.create_group(name)
    for key, value in source.attrs.items():
        group.attrs[key] = value
    for child_name, child in source.items():
        copy_node_chunked(child, group, child_name)


def write_x_only_h5ad(source_path: Path, destination: Path) -> tuple[list[int], int]:
    with h5py.File(source_path, "r") as source, h5py.File(destination, "x") as output:
        if "X" not in source or "obs" not in source or "var" not in source:
            raise RuntimeError("source H5AD lacks X/obs/var")
        source_obs = read_elem(source["obs"])
        source_var = read_elem(source["var"])
        obs_names = pd.Index(source_obs.index.astype(str), name=source_obs.index.name)
        var_names = pd.Index(source_var.index.astype(str), name=source_var.index.name)
        output.attrs["encoding-type"] = "anndata"
        output.attrs["encoding-version"] = "0.1.0"
        copy_node_chunked(source["X"], output, "X")
        write_elem(output, "obs", pd.DataFrame(index=obs_names))
        write_elem(output, "var", pd.DataFrame(index=var_names))
        shape = [len(obs_names), len(var_names)]
    matrix = ad.read_h5ad(destination, backed="r")
    try:
        if list(matrix.shape) != shape:
            raise RuntimeError(
                f"written X.h5ad shape mismatch: {matrix.shape} != {shape}"
            )
    finally:
        matrix.file.close()
    return shape, destination.stat().st_size


def object_identity(blob: storage.Blob, path: Path) -> dict[str, Any]:
    blob.reload()
    return {
        "bucket": blob.bucket.name,
        "name": blob.name,
        "uri": f"gs://{blob.bucket.name}/{blob.name}",
        "generation": str(blob.generation),
        "generation_uri": f"gs://{blob.bucket.name}/{blob.name}#{blob.generation}",
        "size_bytes": int(blob.size or 0),
        "sha256": sha256_file(path),
    }


def upload_create_only(bucket: storage.Bucket, name: str, path: Path) -> dict[str, Any]:
    blob = bucket.blob(name)
    blob.upload_from_filename(
        path, if_generation_match=0, timeout=3600, checksum="crc32c"
    )
    identity = object_identity(blob, path)
    if identity["size_bytes"] != path.stat().st_size:
        raise RuntimeError(f"uploaded size mismatch for {name}")
    return identity


def download_generation(
    bucket: storage.Bucket, identity: dict[str, Any], destination: Path
) -> None:
    generation = int(identity["generation"])
    blob = bucket.blob(identity["name"], generation=generation)
    blob.download_to_filename(
        destination, if_generation_match=generation, timeout=3600, checksum="crc32c"
    )
    if (
        destination.stat().st_size != identity["size_bytes"]
        or sha256_file(destination) != identity["sha256"]
    ):
        raise RuntimeError(
            f"generation-pinned physical mismatch: {identity['generation_uri']}"
        )


def assert_frame_value_null_parity(
    expected: pd.DataFrame, actual: pd.DataFrame, label: str
) -> None:
    if list(expected.columns) != list(actual.columns):
        raise RuntimeError(f"{label} parquet columns changed")
    if list(map(str, expected.index)) != list(map(str, actual.index)):
        raise RuntimeError(f"{label} parquet index changed")
    for column in expected.columns:
        left = expected[column]
        right = actual[column]
        if not left.isna().equals(right.isna()):
            raise RuntimeError(f"{label}.{column} parquet null mask changed")
        left_values = left[~left.isna()].astype(object).tolist()
        right_values = right[~right.isna()].astype(object).tolist()
        if left_values != right_values:
            raise RuntimeError(f"{label}.{column} parquet values changed")


def shared_var_identity(
    var: pd.DataFrame, *, organism: str, feature_namespace: str
) -> dict[str, str]:
    if not organism or not feature_namespace:
        raise RuntimeError("shared-var organism and feature namespace must be bound")
    contract = {
        "feature_namespace": feature_namespace,
        "ordered_var_identifiers_sha256": ordered_index_sha256(var.index),
        "organism": organism,
    }
    return {
        **contract,
        "sha256": hashlib.sha256(
            json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }


def check_plan(plan: dict[str, Any]) -> None:
    if plan["writer_sha256"] != sha256_file(Path(__file__)):
        raise RuntimeError("writer bytes do not match the execution plan")
    if plan["execution"]["host"] != socket.gethostname().split(".")[0]:
        raise RuntimeError("must execute on exact authorized EU worker")
    if plan["execution"]["manifest_last"] is not True:
        raise RuntimeError("manifest-last contract missing")
    datasets = plan["datasets"]
    if len(datasets) != 1 or sum(row["n_obs"] for row in datasets) != 108_838:
        raise RuntimeError(
            "plan must bind exactly the row-25 dataset and 108,838 observations"
        )
    dataset = datasets[0]
    if dataset["dataset_version_id"] != "3fdf264b-40f6-4f5a-b433-bddba1bbebf4":
        raise RuntimeError("plan does not bind the frozen row-25 source version")
    if dataset["n_vars"] != 32_055:
        raise RuntimeError("plan must bind exactly 32,055 variables")
    if dataset["organism"] != "NCBITaxon:9606":
        raise RuntimeError("plan must bind the human organism identity")
    if dataset["feature_namespace"] != "cellxgene_feature_id":
        raise RuntimeError("plan must bind the CELLxGENE feature namespace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    plan_bytes = args.plan.read_bytes()
    plan = json.loads(plan_bytes)
    plan_sha256 = hashlib.sha256(plan_bytes).hexdigest()
    check_plan(plan)
    args.evidence_dir.mkdir(parents=True, exist_ok=False)
    lock_path = Path(plan["execution"]["lock_path"])
    lock_handle = lock_path.open("w")
    fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    lock_handle.write(
        json.dumps(
            {"pid": os.getpid(), "task_id": plan["task_id"], "started_at": time.time()}
        )
    )
    lock_handle.flush()

    client = storage.Client(project=plan["execution"]["billing_project"])
    bucket = client.bucket(
        plan["storage"]["bucket"], user_project=plan["execution"]["billing_project"]
    )
    existing = []
    for dataset in plan["datasets"]:
        for member in ("obs.parquet", "X.h5ad", "var.parquet"):
            name = f"{plan['storage']['root'].rstrip('/')}/{dataset['prefix'].strip('/')}/{member}"
            blob = bucket.blob(name)
            if blob.exists():
                existing.append(f"gs://{bucket.name}/{name}")
    if existing:
        raise RuntimeError(f"create-only preflight found existing objects: {existing}")

    started = time.time()
    records: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="row25-triplet-") as temporary:
        temp = Path(temporary)
        for position, dataset in enumerate(plan["datasets"], start=1):
            version = dataset["dataset_version_id"]
            before = source_head(dataset["source_url"])
            if before != dataset["source_head"]:
                raise RuntimeError(f"source HEAD drift for {version}: {before}")
            source_path = temp / f"{version}.source.h5ad"
            source_sha256, source_bytes = stream_source(
                dataset["source_url"], source_path, before["content_length"]
            )
            after = source_head(dataset["source_url"])
            if after != before:
                raise RuntimeError(f"source changed while streaming {version}")
            with h5py.File(source_path, "r") as source:
                obs = read_elem(source["obs"])
                var = read_elem(source["var"])
            if [len(obs), len(var)] != [dataset["n_obs"], dataset["n_vars"]]:
                raise RuntimeError(f"source dimensions drift for {version}")
            if (
                not obs.index.is_unique
                or obs.index.isna().any()
                or not var.index.is_unique
                or var.index.isna().any()
            ):
                raise RuntimeError(f"axis identity invalid for {version}")
            obs_path = temp / f"{version}.obs.parquet"
            var_path = temp / f"{version}.var.parquet"
            x_path = temp / f"{version}.X.h5ad"
            obs.to_parquet(obs_path)
            var.to_parquet(var_path)
            x_shape, x_bytes = write_x_only_h5ad(source_path, x_path)
            physical_obs = pd.read_parquet(obs_path)
            physical_var = pd.read_parquet(var_path)
            assert_frame_value_null_parity(obs, physical_obs, "obs")
            assert_frame_value_null_parity(var, physical_var, "var")
            if ordered_index_sha256(physical_var.index) != ordered_index_sha256(
                var.index
            ):
                raise RuntimeError(f"ordered var identity changed for {version}")
            prefix = (
                f"{plan['storage']['root'].rstrip('/')}/{dataset['prefix'].strip('/')}"
            )
            objects = {
                "obs": upload_create_only(bucket, f"{prefix}/obs.parquet", obs_path),
                "X": upload_create_only(bucket, f"{prefix}/X.h5ad", x_path),
                "var": upload_create_only(bucket, f"{prefix}/var.parquet", var_path),
            }
            records.append(
                {
                    "position": position,
                    "dataset_id": dataset["dataset_id"],
                    "dataset_version_id": version,
                    "prefix": dataset["prefix"],
                    "source": {
                        **before,
                        "url": dataset["source_url"],
                        "sha256": source_sha256,
                        "size_bytes": source_bytes,
                    },
                    "shape": [len(obs), len(var)],
                    "source_obs": frame_inventory(obs),
                    "source_var": frame_inventory(var),
                    "obs": frame_inventory(physical_obs),
                    "var": frame_inventory(physical_var),
                    "shared_var_identity": shared_var_identity(
                        var,
                        organism=dataset["organism"],
                        feature_namespace=dataset["feature_namespace"],
                    ),
                    "x_h5ad_size_bytes": x_bytes,
                    "objects": objects,
                }
            )
            source_path.unlink()
            obs_path.unlink()
            var_path.unlink()
            x_path.unlink()

        consumer_results = []
        for record in records:
            paths: dict[str, Path] = {}
            for role, identity in record["objects"].items():
                path = temp / f"readback-{record['dataset_version_id']}-{role}"
                download_generation(bucket, identity, path)
                paths[role] = path
            obs = pd.read_parquet(paths["obs"])
            var = pd.read_parquet(paths["var"])
            matrix = ad.read_h5ad(paths["X"], backed="r")
            try:
                shape = list(matrix.shape)
                obs_names = pd.Index(matrix.obs_names.astype(str))
                var_names = pd.Index(matrix.var_names.astype(str))
                sample_rows = sorted({0, shape[0] // 2, shape[0] - 1})
                samples = []
                for row in sample_rows:
                    values = matrix.X[row]
                    nnz = (
                        values.nnz
                        if hasattr(values, "nnz")
                        else np.count_nonzero(values)
                    )
                    samples.append({"row": row, "nnz": int(nnz)})
            finally:
                matrix.file.close()
            if (
                shape != record["shape"]
                or not obs.index.astype(str).equals(obs_names)
                or not var.index.astype(str).equals(var_names)
            ):
                raise RuntimeError(
                    f"consumer axis linkage mismatch for {record['dataset_version_id']}"
                )
            if (
                frame_inventory(obs) != record["obs"]
                or frame_inventory(var) != record["var"]
            ):
                raise RuntimeError(
                    f"consumer schema/null inventory mismatch for {record['dataset_version_id']}"
                )
            consumer_results.append(
                {
                    "dataset_version_id": record["dataset_version_id"],
                    "opened_members": ["obs.parquet", "X.h5ad", "var.parquet"],
                    "shape": shape,
                    "obs_to_X_axis_equal": True,
                    "X_to_var_axis_equal": True,
                    "generation_pinned_hashes_equal": True,
                    "sample_rows": samples,
                }
            )
            for path in paths.values():
                path.unlink()

    payload_generations = [
        int(identity["generation"])
        for record in records
        for identity in record["objects"].values()
    ]
    manifest = {
        "schema_version": "pert-gym.cellxgene-triplet-materialization/v1",
        "task_id": plan["task_id"],
        "record_id": plan["record_id"],
        "logical_key": plan["logical_key"],
        "execution_plan_sha256": plan_sha256,
        "writer_sha256": plan["writer_sha256"],
        "controlling_packet": plan["controlling_packet"],
        "source_collection": plan["source_collection"],
        "dataset_count": len(records),
        "observation_count": sum(record["shape"][0] for record in records),
        "artifact_count": sum(len(record["objects"]) for record in records),
        "matrix_payload_bytes_written": sum(
            record["objects"]["X"]["size_bytes"] for record in records
        ),
        "preserve_all_rows_and_nulls": True,
        "missingness_is_non_excluding": True,
        "excluded_observations": 0,
        "datasets": records,
        "consumer_readback": consumer_results,
        "publication": {
            "payload_create_only": True,
            "manifest_last": True,
            "lamin_writes": 0,
            "product_credit": 0,
            "legacy_deletions": 0,
        },
        "runtime": {
            "host": socket.gethostname().split(".")[0],
            "seconds_before_manifest": time.time() - started,
            "max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        },
    }
    manifest_path = args.evidence_dir / "materialization-manifest.json"
    manifest_path.write_bytes(json_bytes(manifest))
    manifest_identity = upload_create_only(
        bucket, plan["storage"]["manifest_name"], manifest_path
    )
    if int(manifest_identity["generation"]) <= max(payload_generations):
        raise RuntimeError(
            "manifest generation is not newer than every payload generation"
        )
    pinned_manifest = (
        args.evidence_dir / "materialization-manifest.generation-pinned.json"
    )
    download_generation(bucket, manifest_identity, pinned_manifest)
    if pinned_manifest.read_bytes() != manifest_path.read_bytes():
        raise RuntimeError("generation-pinned manifest bytes differ")
    result = {
        "status": "PASS",
        "artifacts": 3,
        "datasets": 1,
        "observations": 108_838,
        "manifest": manifest_identity,
        "manifest_last": True,
        "manifest_newer_than_every_payload": True,
        "consumer_readback": "3/3 opened at recorded generations",
        "product_credit": 0,
    }
    (args.evidence_dir / "writer-result.json").write_bytes(json_bytes(result))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
