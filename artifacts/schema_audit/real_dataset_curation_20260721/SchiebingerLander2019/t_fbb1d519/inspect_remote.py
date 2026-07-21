#!/usr/bin/env python3
"""Source-exhaustive, metadata-only inspection for SchiebingerLander2019."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import platform
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_fbb1d519"
DATASET_ID = "SchiebingerLander2019"
PREFIX = "SchiebingerLander2019"
EXPECTED_BASE_OBS_UID = "trSdGyVkTDn5ZkaY0000"
EXPECTED_BASE_VAR_UID = "cw0Kr6j7qVyrDBP10000"
EXPECTED_N_OBS = 327_494
EXPECTED_N_VARS = 27_998
SOURCE_KEYS = (
    "scperturb/records/13350497/files/SchiebingerLander2019_GSE115943.h5ad",
    "scperturb/records/13350497/files/SchiebingerLander2019_GSE106340.h5ad",
)
SOURCE_SUFFIXES = (
    "SchiebingerLander2019_GSE115943",
    "SchiebingerLander2019_GSE106340",
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: pd.Index) -> str:
    return hashlib.sha256("\n".join(values.astype(str)).encode()).hexdigest()


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "columns": list(map(str, frame.columns)),
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(frame.index),
        "index_sample": frame.index.astype(str)[:25].tolist(),
        "dtypes": {str(column): str(frame[column].dtype) for column in frame.columns},
        "non_null": {str(column): int(frame[column].notna().sum()) for column in frame.columns},
        "nunique": {str(column): int(frame[column].dropna().astype(str).nunique()) for column in frame.columns},
        "value_samples": {
            str(column): frame[column].dropna().astype(str).drop_duplicates().head(25).tolist()
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
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    if by_uid:
        raise AssertionError(f"duplicate Artifact uid: {value}")
    records = list(ln.Artifact.filter(key=value).all())
    if not records:
        raise AssertionError(f"unresolved Artifact feature value: {value}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"ordered newest Artifact is not latest: {value}")
    return records[-1]


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"ordered newest Artifact is not latest: {key}")
    return records[-1], records


def download_source(ln: Any, key: str, destination: Path) -> dict[str, Any]:
    records = list(ln.Artifact.filter(key=key).all())
    if len(records) != 1:
        raise AssertionError(f"expected one exact source Artifact for {key}: {len(records)}")
    artifact = records[0]
    url = str(artifact.path)
    if not url.startswith("https://zenodo.org/records/13350497/files/"):
        raise AssertionError(f"unexpected immutable source URL: {url}")
    if not destination.exists() or destination.stat().st_size != int(artifact.size):
        subprocess.run(
            [
                "curl", "--location", "--fail", "--retry", "3", "--output",
                str(destination), url,
            ],
            check=True,
            timeout=7200,
        )
    if destination.stat().st_size != int(artifact.size):
        raise AssertionError(f"source size drift: {key}")
    return {
        "artifact": artifact_identity(artifact),
        "url": url,
        "size": destination.stat().st_size,
        "local_sha256": sha256_file(destination),
        "local_path": str(destination),
    }


def collection_membership(ln: Any, obs_uid: str) -> dict[str, Any]:
    snapshots = {}
    for key in ("pert-gym/additions/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        snapshots[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_uid_present": obs_uid in {str(member.uid) for member in members},
            "target_key_matches": [str(member.uid) for member in members if str(member.key) == f"{PREFIX}/obs.parquet"],
        }
    return snapshots


def main() -> None:
    capacity = preflight()
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-schiebinger-sources"
    root.mkdir(parents=True, exist_ok=True)

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("wrong Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin branch")

    sources: list[dict[str, Any]] = []
    source_obs: list[pd.DataFrame] = []
    source_var_indices: list[pd.Index] = []
    for position, key in enumerate(SOURCE_KEYS):
        path = root / f"source-{position}.h5ad"
        identity = download_source(ln, key, path)
        backed = ad.read_h5ad(path, backed="r")
        obs = backed.obs.copy()
        obs.index = pd.Index(obs.index.astype(str) + "-" + SOURCE_SUFFIXES[position])
        var = backed.var.copy()
        identity.update(
            {
                "shape": [int(backed.n_obs), int(backed.n_vars)],
                "obs": frame_summary(obs),
                "var": frame_summary(var),
                "uns_keys": sorted(map(str, backed.uns.keys())),
                "obsm_keys": sorted(map(str, backed.obsm.keys())),
                "layers": sorted(map(str, backed.layers.keys())),
                "x_dtype": str(backed.X.dtype),
            }
        )
        backed.file.close()
        sources.append(identity)
        source_obs.append(obs)
        source_var_indices.append(var.index.copy())

    obs_artifact, obs_history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    if EXPECTED_BASE_OBS_UID not in {str(item.uid) for item in obs_history}:
        raise AssertionError("frozen base OBS UID absent from history")
    current_obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    linked_var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    latest_var_artifact, var_history = latest_artifact(ln, f"{PREFIX}/var.parquet")
    if EXPECTED_BASE_VAR_UID not in {str(item.uid) for item in var_history}:
        raise AssertionError("frozen base VAR UID absent from history")
    linked_var = linked_var_artifact.load()
    latest_var = latest_var_artifact.load()

    concatenated_index = source_obs[0].index.append(source_obs[1].index)
    overlap = source_obs[0].index.intersection(source_obs[1].index)
    if sum(len(frame) for frame in source_obs) != EXPECTED_N_OBS:
        raise AssertionError("source OBS denominator drift")
    if (
        len(current_obs) != EXPECTED_N_OBS
        or len(linked_var) != EXPECTED_N_VARS
        or len(latest_var) != EXPECTED_N_VARS
    ):
        raise AssertionError("current triplet denominator drift")
    if len(source_var_indices[0]) != EXPECTED_N_VARS or not source_var_indices[0].equals(source_var_indices[1]):
        raise AssertionError("source VAR axes differ")

    source_all = pd.concat(source_obs, axis=0, join="outer")
    current_original = pd.Index(current_obs["original_obs_index"].astype(str))
    source_set_equals_current_original = set(source_all.index.astype(str)) == set(current_original)
    source_join_mismatches: dict[str, int] = {}
    if source_set_equals_current_original:
        source_all.index = source_all.index.astype(str)
        joined = source_all.reindex(current_original)
        for column in source_all.columns:
            left = joined[column].astype("string")
            right = current_obs[column].astype("string").reset_index(drop=True)
            left = left.reset_index(drop=True)
            equal = (left.isna() & right.isna()) | (left.fillna("") == right.fillna(""))
            source_join_mismatches[str(column)] = int((~equal).sum())

    report = {
        "format": "pert-gym.schiebinger-source-exhaustive-inspection/v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "sources": sources,
        "source_union": {
            "rows": len(concatenated_index),
            "index_unique": bool(concatenated_index.is_unique),
            "index_sha256": ordered_sha256(concatenated_index),
            "cross_source_index_overlap": len(overlap),
            "cross_source_index_overlap_sample": overlap.astype(str)[:25].tolist(),
            "var_axes_equal": source_var_indices[0].equals(source_var_indices[1]),
            "var_index_sha256": ordered_sha256(source_var_indices[0]),
            "set_equals_current_original_obs_index": source_set_equals_current_original,
            "source_join_mismatches": source_join_mismatches,
            "source_join_mismatch_count": sum(source_join_mismatches.values()),
        },
        "current": {
            "obs": artifact_identity(obs_artifact),
            "obs_history": [artifact_identity(item) for item in obs_history],
            "obs_frame": frame_summary(current_obs),
            "x": artifact_identity(x_artifact),
            "linked_var": artifact_identity(linked_var_artifact),
            "linked_var_frame": frame_summary(linked_var),
            "latest_var": artifact_identity(latest_var_artifact),
            "latest_var_history": [artifact_identity(item) for item in var_history],
            "latest_var_frame": frame_summary(latest_var),
            "links": {
                "obs_to_x": str(resolve_artifact(ln, obs_artifact.features.get_values()["X"]).uid) == str(x_artifact.uid),
                "x_to_linked_var": str(resolve_artifact(ln, x_artifact.features.get_values()["var"]).uid) == str(linked_var_artifact.uid),
                "x_links_latest_var": str(linked_var_artifact.uid) == str(latest_var_artifact.uid),
            },
        },
        "identity_hypotheses": {
            "source_concat_index_equals_current_index": concatenated_index.equals(current_obs.index.astype(str)),
            "source_concat_index_equals_original_obs_index": (
                "original_obs_index" in current_obs
                and concatenated_index.equals(pd.Index(current_obs["original_obs_index"].astype(str)))
            ),
            "source_index_set_equals_current_original_obs_index": source_set_equals_current_original,
            "current_index_equals_original_obs_index": current_obs.index.astype(str).equals(current_original),
            "source_var_index_equals_linked_var_index": source_var_indices[0].equals(linked_var.index.astype(str)),
            "source_var_index_equals_latest_var_index": source_var_indices[0].equals(latest_var.index.astype(str)),
        },
        "collections": collection_membership(ln, str(obs_artifact.uid)),
        "invariants": {
            "source_object_count": len(sources),
            "source_obs_rows": sum(len(frame) for frame in source_obs),
            "source_var_rows": len(source_var_indices[0]),
            "no_x_payload_materialized": True,
            "writes": 0,
        },
    }
    encoded = base64.b64encode(gzip.compress(canonical(report).encode(), mtime=0)).decode()
    print("SCHIEBINGER_REPORT_GZIP_BASE64_BEGIN")
    print(encoded)
    print("SCHIEBINGER_REPORT_END")


if __name__ == "__main__":
    main()
