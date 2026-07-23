#!/usr/bin/env python3
"""Read-only live triplet inspection for GEO GSE197452 Perturb-seq."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_05cef992"
REAL_DATASET_ID = "geo/GSE197452"
PREFIX = "prism_collection/GSE197452_Perturb-seq"
EXPECTED_N_OBS = 20_811
EXPECTED_N_VARS = 33_694
EXPECTED = {
    "obs": {"uid": "6UsaktwOJjkXPM3L0002", "key": f"{PREFIX}/obs.parquet"},
    "x": {"uid": "GYQBTGssvyua7wmc0000", "key": f"{PREFIX}/X.h5ad"},
    "var": {"uid": "eJMdIf8H75RMWWK90001", "key": f"{PREFIX}/var.parquet"},
}
PERTURB_SAMPLES = {
    "GSM6297384": "Illumina expression",
    "GSM6297385": "Ultima expression",
    "GSM6297388": "HTO/ADT/guide feature matrices",
    "GSM6297389": "ADT logical sample (payload attached to GSM6297388)",
    "GSM6297390": "Perturb Guide logical sample (payload attached to GSM6297388)",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def ordered_sha256(values: pd.Index) -> str:
    return hashlib.sha256("\n".join(values.astype(str)).encode()).hexdigest()


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "columns": list(map(str, frame.columns)),
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(frame.index),
        "index_sample": frame.index.astype(str)[:12].tolist(),
        "dtypes": {str(column): str(frame[column].dtype) for column in frame.columns},
        "non_null": {
            str(column): int(frame[column].notna().sum()) for column in frame.columns
        },
        "nunique": {
            str(column): int(frame[column].dropna().astype(str).nunique())
            for column in frame.columns
        },
        "value_samples": {
            str(column): frame[column]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .head(12)
            .tolist()
            for column in frame.columns
        },
    }


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
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"newest Artifact is not latest: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve linked Artifact: {value}")
    return records[-1]


def collection_snapshot(ln: Any) -> dict[str, Any]:
    snapshots: dict[str, Any] = {
        "historical_manifest_identity": "jkobject:GCjqQtGwPzkY"
    }
    for key in ("pert-gym/additions/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}={len(records)}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [
            {"uid": str(item.uid), "key": str(item.key)}
            for item in members
            if str(item.key) == EXPECTED["obs"]["key"]
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


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")

    obs_artifact, obs_history = latest_artifact(ln, EXPECTED["obs"]["key"])
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    for role, artifact in (
        ("obs", obs_artifact),
        ("x", x_artifact),
        ("var", var_artifact),
    ):
        expected = EXPECTED[role]
        if str(artifact.uid) != expected["uid"] or str(artifact.key) != expected["key"]:
            raise AssertionError(
                f"accepted {role} identity drift: expected={expected}, actual={artifact_identity(artifact)}"
            )
    if len(obs) != EXPECTED_N_OBS or len(var) != EXPECTED_N_VARS:
        raise AssertionError("accepted triplet denominator drift")

    x_path = Path(x_artifact.cache())
    backed = ad.read_h5ad(x_path, backed="r")
    if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
        raise AssertionError("accepted X shape drift")
    x_obs_axis = backed.obs_names.astype(str).copy()
    x_var_axis = backed.var_names.astype(str).copy()
    backed.file.close()

    stable = (
        var["stable_feature_id"].astype("string")
        if "stable_feature_id" in var
        else pd.Series([], dtype="string")
    )
    report = {
        "format": "pert-gym.gse197452-live-inspection/v1",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "dataset_id": PREFIX,
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "source_allowlist": PERTURB_SAMPLES,
        "current": {
            "obs": artifact_identity(obs_artifact),
            "obs_history": [artifact_identity(item) for item in obs_history],
            "obs_frame": frame_summary(obs),
            "x": artifact_identity(x_artifact),
            "x_shape": [EXPECTED_N_OBS, EXPECTED_N_VARS],
            "x_obs_names_sha256": ordered_sha256(x_obs_axis),
            "x_var_names_sha256": ordered_sha256(x_var_axis),
            "var": artifact_identity(var_artifact),
            "var_frame": frame_summary(var),
            "var_uniqueness": {
                "index_unique": bool(var.index.is_unique),
                "stable_feature_id_present": "stable_feature_id" in var,
                "stable_feature_id_non_null": int(stable.notna().sum()),
                "stable_feature_id_unique": bool(stable.dropna().is_unique),
                "stable_feature_id_ensg": int(
                    stable.str.fullmatch(r"ENSG\d{11}", na=False).sum()
                ),
            },
            "axis": {
                "obs_index_equals_x_obs_names": obs.index.astype(str).equals(
                    x_obs_axis
                ),
                "original_obs_index_equals_x_obs_names": (
                    "original_obs_index" in obs
                    and pd.Index(obs["original_obs_index"].astype(str)).equals(
                        x_obs_axis
                    )
                ),
                "var_index_equals_x_var_names": var.index.astype(str).equals(
                    x_var_axis
                ),
            },
            "links": {"obs_to_x": True, "x_to_var": True},
        },
        "collection": collection_snapshot(ln),
        "invariants": {
            "writes": 0,
            "x_payload_materialized": False,
            "single_physical_member": True,
            "chunk_decision": "review no-rechunk unless live loadability or size evidence proves a defect",
        },
    }
    print("GSE197452_INSPECTION=" + canonical(report), flush=True)


if __name__ == "__main__":
    main()
