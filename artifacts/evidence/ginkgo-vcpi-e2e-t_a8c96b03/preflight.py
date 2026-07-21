#!/usr/bin/env python3
"""Read-only EU-VM preflight for the integrated Ginkgo VCPI E2E task."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    legacy_lamin_writer_lock_paths,
    require_heavy_vm,
    vm_global_lamin_writer_lock_path,
)

TASK_ID = "t_a8c96b03"
PREFIX = "ginkgo-datapoints/vcpi"
LOGICAL_KEY = "pert-gym/logical/ginkgo-datapoints/vcpi"
EXPECTED = {
    "obs_uid": "Q7Qaj6dz0CzyQQ9i0002",
    "x_uid": "72CMoQ6GfgZuTNdL0000",
    "var_uid": "sDYMNbN7DkmFB7Dx0001",
    "n_obs": 11_808,
    "n_vars": 59_427,
}
SOURCE_KEYS = {
    "counts": f"{PREFIX}/vcpi_GDPx2_counts.parquet",
    "meta": f"{PREFIX}/vcpi_GDPx2_meta.csv",
    "compounds": f"{PREFIX}/compounds-GDPx2-2026-02-09.csv",
}
UPSTREAM_URLS = {
    "counts": "https://ginkgo-datapoints-public.s3.us-east-2.amazonaws.com/datasets/vcpi/vcpi_GDPx2_counts.parquet",
    "meta": "https://ginkgo-datapoints-public.s3.us-east-2.amazonaws.com/datasets/vcpi/vcpi_GDPx2_meta.csv",
}


def sha256_stream(path: Any) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_identity(record: Any, *, hash_payload: bool = True) -> dict[str, Any]:
    result = {
        "uid": str(record.uid),
        "key": str(record.key),
        "version": str(record.version),
        "lamin_hash": str(record.hash),
        "size": int(record.size),
        "n_observations": record.n_observations,
        "is_latest": bool(record.is_latest),
        "description": record.description,
        "path": str(record.path),
        "features": {
            str(key): str(getattr(value, "key", value))
            for key, value in record.features.get_values().items()
        },
    }
    if hash_payload:
        result["sha256"] = sha256_stream(record.path)
    return result


def latest(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).order_by("created_at"))
    if not records:
        raise RuntimeError(f"missing exact artifact key: {key}")
    return records[-1], records


def available_memory() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable absent")


def http_head(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return {
                "url": url,
                "status": int(response.status),
                "content_length": response.headers.get("Content-Length"),
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
            }
    except urllib.error.HTTPError as exc:
        return {"url": url, "status": int(exc.code), "error": str(exc)}


def process_conflicts() -> list[dict[str, Any]]:
    ancestors = {os.getpid()}
    cursor = os.getpid()
    while cursor > 1:
        try:
            cursor = int(
                Path(f"/proc/{cursor}/stat").read_text().rsplit(")", 1)[1].split()[1]
            )
        except (FileNotFoundError, IndexError, ValueError):
            break
        ancestors.add(cursor)
    matches = []
    needles = (
        "execute_recompaction.py",
        "migrate_logical_sparse_zarr",
        "publish_candidate",
        PREFIX,
    )
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal() or int(entry.name) in ancestors:
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if command and any(needle in command for needle in needles):
            matches.append({"pid": int(entry.name), "command": command})
    return matches


def probe_locks() -> list[str]:
    paths = [vm_global_lamin_writer_lock_path(), *legacy_lamin_writer_lock_paths()]
    handles = []
    try:
        for path in paths:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            handle = path.open("a+")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                handle.close()
                raise RuntimeError(f"writer lock held: {path}") from exc
            handles.append(handle)
        return [str(path) for path in paths]
    finally:
        for handle in reversed(handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


def collection_inventory(ln: Any, obs_uid: str) -> list[dict[str, Any]]:
    rows = []
    for collection in ln.Collection.filter(key__startswith="pert-gym/").order_by(
        "created_at"
    ):
        members = list(collection.artifacts.all())
        member_uids = [str(member.uid) for member in members]
        member_keys = [str(member.key) for member in members]
        rows.append(
            {
                "uid": str(collection.uid),
                "key": str(collection.key),
                "version": str(collection.version),
                "description": collection.description,
                "member_count": len(members),
                "member_uid_sha256": hashlib.sha256(
                    ("\n".join(member_uids) + "\n").encode()
                ).hexdigest(),
                "contains_current_obs_uid": obs_uid in member_uids,
                "contains_obs_key": f"{PREFIX}/obs.parquet" in member_keys,
            }
        )
    return rows


def main() -> int:
    host, project, zone, _ = require_heavy_vm()
    if host != "pert-gym-worker-eu" or socket.gethostname().split(".")[0] != host:
        raise RuntimeError("wrong heavy host")
    conflicts = process_conflicts()
    if conflicts:
        raise RuntimeError(f"conflicting writers: {conflicts}")
    lock_paths = probe_locks()
    tmux = subprocess.run(
        ["tmux", "list-sessions"], capture_output=True, text=True, check=False
    )
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    obs, obs_history = latest(ln, f"{PREFIX}/obs.parquet")
    x, x_history = latest(ln, f"{PREFIX}/X.h5ad")
    var, var_history = latest(ln, f"{PREFIX}/var.parquet")
    actual_uids = {
        "obs_uid": str(obs.uid),
        "x_uid": str(x.uid),
        "var_uid": str(var.uid),
    }
    if actual_uids != {key: EXPECTED[key] for key in actual_uids}:
        raise RuntimeError(f"exact current triplet drift: {actual_uids}")

    cache = Path("/tmp/pert-gym") / TASK_ID / "source-cache"
    cache.mkdir(parents=True, exist_ok=True)
    obs_path = Path(obs.cache())
    x_path = Path(x.cache())
    var_path = Path(var.cache())
    obs_df = pd.read_parquet(obs_path)
    var_df = pd.read_parquet(var_path)
    source = ad.read_h5ad(x_path, backed="r")
    try:
        shape = tuple(int(value) for value in source.shape)
        x_type = type(source.X).__name__
        x_dtype = str(source.X.dtype)
        x_format = str(getattr(source.X, "format", "unknown"))
    finally:
        source.file.close()
    with h5py.File(x_path, "r") as handle:
        x_group = handle["X"]
        encoding_type = x_group.attrs.get("encoding-type", "unknown")
        if isinstance(encoding_type, bytes):
            encoding_type = encoding_type.decode()
        nnz = (
            int(x_group["data"].shape[0])
            if isinstance(x_group, h5py.Group) and "data" in x_group
            else None
        )
        h5_layout = {
            "encoding_type": str(encoding_type),
            "data_dtype": str(x_group["data"].dtype)
            if nnz is not None
            else str(x_group.dtype),
            "nnz": nnz,
        }
    if shape != (EXPECTED["n_obs"], EXPECTED["n_vars"]):
        raise RuntimeError(f"shape drift: {shape}")
    if len(obs_df) != EXPECTED["n_obs"] or len(var_df) != EXPECTED["n_vars"]:
        raise RuntimeError("OBS/VAR denominator drift")
    if obs.features.get_values().get("X") not in (x, x.key):
        raise RuntimeError("OBS->X link drift")
    if x.features.get_values().get("var") not in (var, var.key):
        raise RuntimeError("X->VAR link drift")

    sources = {}
    for role, key in SOURCE_KEYS.items():
        record, history = latest(ln, key)
        sources[role] = {
            "current": artifact_identity(record),
            "history_uids": [str(item.uid) for item in history],
        }

    logical_existing = [
        {"uid": str(item.uid), "key": str(item.key), "size": int(item.size)}
        for item in ln.Artifact.filter(key__startswith=LOGICAL_KEY).all()
    ]
    collections = collection_inventory(ln, str(obs.uid))
    receipt = {
        "format": "pert-gym.ginkgo-vcpi-e2e-preflight/v1",
        "task_id": TASK_ID,
        "timestamp": time.time(),
        "product_execution": {
            "host": host,
            "pid": os.getpid(),
            "phase": "preflight",
            "payload_heartbeat_at": time.time(),
            "metric": "dataset_e2e_current",
            "current": 0,
            "denominator": 1,
            "unit": "logical_dataset",
        },
        "host": {
            "project": project,
            "zone": zone,
            "mem_available_bytes": available_memory(),
            "disk_free_bytes": shutil.disk_usage(cache).free,
        },
        "repo": {
            "head": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            "status": subprocess.check_output(
                ["git", "status", "--short"], text=True
            ).splitlines(),
        },
        "writer_exclusivity": {
            "conflicts": conflicts,
            "lock_paths_probed_free": lock_paths,
            "tmux": tmux.stdout.splitlines(),
            "mismatch_count": 0,
        },
        "triplet": {
            "obs": artifact_identity(obs),
            "x": artifact_identity(x),
            "var": artifact_identity(var),
            "obs_history_uids": [str(item.uid) for item in obs_history],
            "x_history_uids": [str(item.uid) for item in x_history],
            "var_history_uids": [str(item.uid) for item in var_history],
            "shape": list(shape),
            "x_type": x_type,
            "x_dtype": x_dtype,
            "x_format": x_format,
            "h5_layout": h5_layout,
            "obs_columns": list(map(str, obs_df.columns)),
            "var_columns": list(map(str, var_df.columns)),
            "obs_index_sha256": hashlib.sha256(
                ("\n".join(map(str, obs_df.index)) + "\n").encode()
            ).hexdigest(),
            "var_index_sha256": hashlib.sha256(
                ("\n".join(map(str, var_df.index)) + "\n").encode()
            ).hexdigest(),
            "link_mismatch_count": 0,
        },
        "source_artifacts": sources,
        "upstream_head": {name: http_head(url) for name, url in UPSTREAM_URLS.items()},
        "logical_target": {
            "key": LOGICAL_KEY,
            "existing_artifacts": logical_existing,
            "conflict_count": len(logical_existing),
        },
        "pert_gym_collections": collections,
        "collection_memberships_for_current_obs": [
            item
            for item in collections
            if item["contains_current_obs_uid"] or item["contains_obs_key"]
        ],
        "mismatch_count": 0,
    }
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
