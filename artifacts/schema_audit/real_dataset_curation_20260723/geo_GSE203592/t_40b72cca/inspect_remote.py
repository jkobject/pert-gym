#!/usr/bin/env python3
"""Read-only bounded inspection of the current GSE203592 OBS/X/VAR triplet."""

from __future__ import annotations

import json
import platform
from pathlib import Path
from typing import Any

import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_40b72cca"
PREFIX = "prism_collection/GSE203592"
EXPECTED_N_OBS = 70_646
EXPECTED_N_VARS = 31_053
EXPECTED_OBS_UID = "DQDHXFkEpvdiCySB0002"
EXPECTED_X_UID = "PhpiVnUwNAeZ26m40000"
EXPECTED_VAR_UID = "N0CJ8e8f2rE4PjL10001"
REPORT = Path(__file__).with_name("inspection_report.json")


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "is_latest": bool(artifact.is_latest),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve linked artifact: {value}")
    return records[-1]


def column_summary(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column in frame.columns:
        values = frame[column]
        non_null = int(values.notna().sum())
        unique = int(values.nunique(dropna=True))
        top: dict[str, int] = {}
        if unique <= 100:
            top = {
                str(key): int(value)
                for key, value in values.astype("string")
                .value_counts(dropna=False)
                .head(100)
                .items()
            }
        result[str(column)] = {
            "dtype": str(values.dtype),
            "non_null": non_null,
            "unique": unique,
            "top_values": top,
        }
    return result


def collection_snapshot(ln: Any) -> dict[str, Any]:
    snapshots: dict[str, Any] = {}
    for key in ("pert-gym/additions/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [
            {"uid": str(item.uid), "key": str(item.key)}
            for item in members
            if str(item.key) == f"{PREFIX}/obs.parquet"
        ]
        if len(matches) != 1:
            raise AssertionError(f"target Collection membership drift: {key}")
        snapshots[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_key_matches": matches,
        }
    return snapshots


def main() -> int:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")

    obs_records = list(ln.Artifact.filter(key=f"{PREFIX}/obs.parquet").all())
    obs_records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    obs_artifact = obs_records[-1]
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()

    if str(obs_artifact.uid) != EXPECTED_OBS_UID:
        raise AssertionError("OBS identity drift")
    if str(x_artifact.uid) != EXPECTED_X_UID:
        raise AssertionError("X identity drift")
    if str(var_artifact.uid) != EXPECTED_VAR_UID:
        raise AssertionError("VAR identity drift")
    if len(obs) != EXPECTED_N_OBS or len(var) != EXPECTED_N_VARS:
        raise AssertionError("OBS/VAR denominator drift")

    report = {
        "format": "pert-gym.gse203592-read-only-inspection/v1",
        "task_id": TASK_ID,
        "triplet": {
            "obs": artifact_identity(obs_artifact),
            "x": artifact_identity(x_artifact),
            "var": artifact_identity(var_artifact),
            "obs_history": [artifact_identity(item) for item in obs_records],
        },
        "obs": {
            "rows": len(obs),
            "index_name": obs.index.name,
            "index_unique": bool(obs.index.is_unique),
            "columns": column_summary(obs),
        },
        "var": {
            "rows": len(var),
            "index_name": var.index.name,
            "index_unique": bool(var.index.is_unique),
            "index_sample": var.index.astype(str).tolist()[:100],
            "columns": column_summary(var),
        },
        "collections": collection_snapshot(ln),
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "writes": 0,
    }
    REPORT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("GSE203592_INSPECTION=" + json.dumps(report, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
