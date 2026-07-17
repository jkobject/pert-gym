#!/usr/bin/env python3
"""Independent generation-qualified verifier for temporal-v4 row 132."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import socket
import tempfile
from pathlib import Path
from typing import Any

import fsspec
import h5py
import numpy as np
import pandas as pd

EXPECTED_HOST = "pert-gym-worker-eu"
BILLING = "jkobject-1549353370965"
RECORD_ID = "temporal_v4_132_overlapping_transcriptional_programs_promote_survival_and_axonal_regeneration_of"
EXPECTED_SHAPE = [129_441, 23_308]
EXPECTED_NNZ = 528_649_855
EXPECTED_COUNT_SUM = 2_017_575_951
EXPECTED_MANIFEST_SHA = "dc33b1a1e1d24e96f3ce8efce8d052d1b01c2e7102d335a0ebf866ebc63e92a6"


def read_generation(fs: Any, key: str, generation: str) -> bytes:
    with fs.open(f"{key}#{generation}", "rb") as handle:
        return handle.read()


def verify_object(fs: Any, obj: dict[str, Any], target: Path | None = None) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with fs.open(f"{obj['key']}#{obj['generation']}", "rb") as source:
        if target is None:
            while block := source.read(8 * 1024**2):
                digest.update(block)
                size += len(block)
        else:
            with target.open("wb") as destination:
                while block := source.read(8 * 1024**2):
                    destination.write(block)
                    digest.update(block)
                    size += len(block)
    if size != obj["size_bytes"] or digest.hexdigest() != obj["sha256"]:
        raise RuntimeError(f"generation-qualified object mismatch: {obj['key']}")
    return {"key": obj["key"], "generation": obj["generation"], "size_bytes": size, "sha256": digest.hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-generation-uri", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if socket.gethostname().split(".")[0] != EXPECTED_HOST:
        raise RuntimeError("verification must run on pert-gym-worker-eu")
    uri, generation = args.manifest_generation_uri.rsplit("#", 1)
    if not uri.startswith("gs://"):
        raise RuntimeError("manifest must be generation-qualified GCS URI")
    key = uri.removeprefix("gs://")
    fs = fsspec.filesystem("gcs", version_aware=True, project=BILLING, requester_pays=BILLING)
    manifest_bytes = read_generation(fs, key, generation)
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha != EXPECTED_MANIFEST_SHA:
        raise RuntimeError("immutable manifest checksum mismatch")
    manifest = json.loads(manifest_bytes)
    if manifest.get("record_id") != RECORD_ID or manifest.get("shape") != EXPECTED_SHAPE or manifest.get("nnz") != EXPECTED_NNZ or manifest.get("raw_count_sum") != EXPECTED_COUNT_SUM:
        raise RuntimeError("manifest identity or denominator drift")
    payload = {obj["role"]: obj for obj in manifest["payload_objects"]}
    support = {obj["role"]: obj for obj in manifest["support_objects"]}
    if set(payload) != {"obs", "X", "var"} or len(support) != 5:
        raise RuntimeError("physical member inventory mismatch")
    if int(generation) <= max(int(obj["generation"]) for obj in [*payload.values(), *support.values()]):
        raise RuntimeError("manifest was not written last")
    checks: list[dict[str, Any]] = []
    for obj in support.values():
        checks.append(verify_object(fs, obj))
    with tempfile.TemporaryDirectory(prefix="temporal-v4-132-independent-") as temp:
        x_path = Path(temp) / "X.h5ad"
        checks.append(verify_object(fs, payload["X"], x_path))
        obs_bytes = read_generation(fs, payload["obs"]["key"], payload["obs"]["generation"])
        var_bytes = read_generation(fs, payload["var"]["key"], payload["var"]["generation"])
        if hashlib.sha256(obs_bytes).hexdigest() != payload["obs"]["sha256"] or len(obs_bytes) != payload["obs"]["size_bytes"]:
            raise RuntimeError("obs immutable readback mismatch")
        if hashlib.sha256(var_bytes).hexdigest() != payload["var"]["sha256"] or len(var_bytes) != payload["var"]["size_bytes"]:
            raise RuntimeError("var immutable readback mismatch")
        checks.extend([
            {"key": payload["obs"]["key"], "generation": payload["obs"]["generation"], "size_bytes": len(obs_bytes), "sha256": payload["obs"]["sha256"]},
            {"key": payload["var"]["key"], "generation": payload["var"]["generation"], "size_bytes": len(var_bytes), "sha256": payload["var"]["sha256"]},
        ])
        obs = pd.read_parquet(io.BytesIO(obs_bytes))
        var = pd.read_parquet(io.BytesIO(var_bytes))
        with h5py.File(x_path, "r") as h5:
            shape = list(map(int, h5["X"].attrs["shape"]))
            nnz = int(h5["X/data"].shape[0])
            count_sum = int(sum(np.asarray(h5["X/data"][start:start + 1_000_000], dtype=np.int64).sum() for start in range(0, nnz, 1_000_000)))
            obs_index_key = h5["obs"].attrs["_index"]
            var_index_key = h5["var"].attrs["_index"]
            obs_names = [x.decode() if isinstance(x, bytes) else str(x) for x in h5[f"obs/{obs_index_key}"][:]]
            var_names = [x.decode() if isinstance(x, bytes) else str(x) for x in h5[f"var/{var_index_key}"][:]]
        if shape != EXPECTED_SHAPE or nnz != EXPECTED_NNZ or count_sum != EXPECTED_COUNT_SUM:
            raise RuntimeError("independent matrix payload mismatch")
        if len(obs) != EXPECTED_SHAPE[0] or len(var) != EXPECTED_SHAPE[1] or obs_names != list(map(str, obs.index)) or var_names != list(map(str, var.index)):
            raise RuntimeError("independent ordered-axis parity failure")
    result = {"verdict": "PASS", "independent": True, "manifest_generation_uri": args.manifest_generation_uri, "manifest_sha256": manifest_sha, "shape": EXPECTED_SHAPE, "nnz": EXPECTED_NNZ, "raw_count_sum": EXPECTED_COUNT_SUM, "physical_member_count": 1 + len(payload) + len(support), "generation_qualified_member_checks": checks, "ordered_axis_parity": True, "manifest_written_last": True, "accepted_product_credit_pre": 0, "accepted_product_credit_post": 0, "global_accepted_components_control_plane_status": manifest["ledger"]["global_control_plane"]["status"]}
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
