#!/usr/bin/env python3
"""Read-only, bounded live preflight for DRUG-seq/GSE120222 E2E publication."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    legacy_lamin_writer_lock_paths,
    require_heavy_vm,
    vm_global_lamin_writer_lock_path,
)

TASK_ID = "t_3d9bf0d8"
DATASET_ID = "DRUG-seq/GSE120222"
OBS_UID = "mKSaEEcH4jyes43Z0002"
X_UID = "hfeVCMInQu1UKhwp0000"
VAR_UID = "vmqp94W72a1Tl2Xw0002"
PREDECESSOR_UID = "qoTeH7T78kjbmIWA0000"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def available_memory() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("MemAvailable absent")


def feature_key(value: Any) -> str | None:
    return None if value is None else str(getattr(value, "key", value))


def artifact_snapshot(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "n_observations": artifact.n_observations,
        "is_latest": bool(artifact.is_latest),
        "created_at": artifact.created_at.isoformat(),
        "created_by_id": artifact.created_by_id,
    }


def collection_snapshot(collection: Any) -> tuple[list[Any], dict[str, Any]]:
    members = list(collection.artifacts.all().only("uid", "key"))
    uids = [str(member.uid) for member in members]
    keys = [str(member.key) for member in members]
    membership = hashlib.sha256(canonical(sorted(uids)).encode()).hexdigest()
    return members, {
        "uid": str(collection.uid),
        "key": str(collection.key),
        "hash": str(collection.hash),
        "description": str(collection.description),
        "member_count": len(members),
        "unique_uid_count": len(set(uids)),
        "unique_key_count": len(set(keys)),
        "obs_key_count": sum(key.endswith("/obs.parquet") for key in keys),
        "membership_sha256": membership,
    }


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
    needles = ("publish_collections.py", "run_e2e.py", DATASET_ID)
    matches = []
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


def probe_locks(predecessor_uid: str) -> list[str]:
    family = hashlib.sha256(DATASET_ID.encode()).hexdigest()
    predecessor = hashlib.sha256(predecessor_uid.encode()).hexdigest()
    paths = [
        vm_global_lamin_writer_lock_path(),
        *legacy_lamin_writer_lock_paths(),
        Path("/tmp/pert-gym/families") / f"{family}.lock",
        Path("/tmp/pert-gym/collections") / f"{predecessor}.lock",
    ]
    handles = []
    try:
        for path in paths:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            handle = path.open("a+")
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            handles.append(handle)
    finally:
        for handle in reversed(handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
    return [str(path) for path in paths]


def main() -> int:
    require_heavy_vm()
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("unexpected Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("unexpected Lamin branch")
    if available_memory() < 8 * 1024**3:
        raise AssertionError("MemAvailable below 8 GiB")
    conflicts = process_conflicts()
    if conflicts:
        raise AssertionError(f"conflicting writer processes: {conflicts}")

    artifacts = {
        "obs": ln.Artifact.get(uid=OBS_UID),
        "x": ln.Artifact.get(uid=X_UID),
        "var": ln.Artifact.get(uid=VAR_UID),
    }
    expected_keys = {
        "obs": f"{DATASET_ID}/obs.parquet",
        "x": f"{DATASET_ID}/X.h5ad",
        "var": f"{DATASET_ID}/var.parquet",
    }
    for role, artifact in artifacts.items():
        if str(artifact.key) != expected_keys[role] or not artifact.is_latest:
            raise AssertionError(f"current {role} identity drift")
    if (
        feature_key(artifacts["obs"].features.get_values().get("X"))
        != expected_keys["x"]
    ):
        raise AssertionError("OBS -> X link drift")
    if (
        feature_key(artifacts["x"].features.get_values().get("var"))
        != expected_keys["var"]
    ):
        raise AssertionError("X -> VAR link drift")

    paths = {role: Path(artifact.cache()) for role, artifact in artifacts.items()}
    obs = pd.read_parquet(paths["obs"])
    var = pd.read_parquet(paths["var"])
    x = ad.read_h5ad(paths["x"], backed="r")
    try:
        shape = [int(x.n_obs), int(x.n_vars)]
        matrix = x.X[:]
        nnz = int(matrix.nnz if hasattr(matrix, "nnz") else np.count_nonzero(matrix))
        x_dtype = str(x.X.dtype)
    finally:
        x.file.close()
    if shape != [len(obs), len(var)]:
        raise AssertionError("OBS/X/VAR shape mismatch")

    predecessor = ln.Collection.get(uid=PREDECESSOR_UID)
    predecessor_members, predecessor_snapshot = collection_snapshot(predecessor)
    predecessor_uids = {str(member.uid) for member in predecessor_members}
    predecessor_same_key = [
        str(member.uid)
        for member in predecessor_members
        if str(member.key) == expected_keys["obs"]
    ]
    successors = []
    for collection in ln.Collection.filter(key__startswith="pert-gym/additions/").all():
        try:
            description = json.loads(collection.description or "{}")
        except json.JSONDecodeError:
            continue
        if description.get("predecessor_uid") == PREDECESSOR_UID:
            successors.append({"uid": str(collection.uid), "key": str(collection.key)})

    receipt = {
        "format": "pert-gym.drug-seq-gse120222-live-preflight/v1",
        "task_id": TASK_ID,
        "host": os.uname().nodename,
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "mem_available_bytes": available_memory(),
        "conflicting_processes": conflicts,
        "lock_probe_paths": probe_locks(PREDECESSOR_UID),
        "artifacts": {
            role: artifact_snapshot(value) for role, value in artifacts.items()
        },
        "links": {"obs_to_x": expected_keys["x"], "x_to_var": expected_keys["var"]},
        "payload": {
            "shape": shape,
            "nnz": nnz,
            "x_dtype": x_dtype,
            "sha256": {role: sha256_file(path) for role, path in paths.items()},
            "obs_columns": list(obs.columns),
            "obs_non_null": {
                column: int(obs[column].notna().sum()) for column in obs.columns
            },
            "obs_unique": {
                column: sorted(map(str, obs[column].dropna().unique()))[:100]
                for column in obs.columns
            },
            "var_columns": list(var.columns),
            "var_non_null": {
                column: int(var[column].notna().sum()) for column in var.columns
            },
        },
        "predecessor": predecessor_snapshot,
        "predecessor_contains_target_uid": OBS_UID in predecessor_uids,
        "predecessor_same_key_uids": predecessor_same_key,
        "successors_from_predecessor": successors,
    }
    print("HERMES_PREFLIGHT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
