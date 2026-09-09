#!/usr/bin/env python3
"""Read-only, remote-only live proof for the GEO GSE203592 triplet.

This verifier never creates or revises Lamin objects. It resolves live feature
links instead of deriving keys, loads OBS and VAR only, opens X backed-only for
axis metadata, and proves the selected collection memberships are unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import time
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

from tools.lamin_context import connect_pertdata

TASK_ID = "t_180f9956"
PREFIX = "prism_collection/GSE203592"
EXPECTED_N_OBS = 70_646
EXPECTED_N_VARS = 31_053
COLLECTION_KEYS = (
    "pert-gym/additions/20260621",
    "pert-gym/canonical/20260621",
)


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def ordered_values_sha256(values: pd.Index) -> str:
    return sha256_text("\n".join(values.astype(str)))


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "is_latest": bool(artifact.is_latest),
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "n_observations": getattr(artifact, "n_observations", None),
    }


def current_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    history = list(ln.Artifact.filter(key=key).all())
    if not history:
        raise AssertionError(f"missing artifact history for {key}")
    latest = [item for item in history if bool(item.is_latest)]
    if len(latest) != 1:
        raise AssertionError(f"expected exactly one latest artifact for {key}")
    history.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    return latest[0], history


def resolve_linked_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    return current_artifact(ln, value)[0]


def x_axis_receipt(x_artifact: Any) -> tuple[pd.Index, dict[str, Any]]:
    cached_path = Path(x_artifact.cache())
    if not cached_path.is_file():
        raise AssertionError(f"cached X payload is absent: {cached_path}")
    backed = ad.read_h5ad(cached_path, backed="r")
    try:
        if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
            raise AssertionError("X shape differs from frozen GSE203592 denominator")
        axis = backed.var_names.astype(str).copy()
        return axis, {
            "shape": [backed.n_obs, backed.n_vars],
            "var_names_sha256": ordered_values_sha256(axis),
            "backed_only": True,
            "x_payload_cached": True,
            "layers": sorted(backed.layers.keys()),
            "raw_present": backed.raw is not None,
        }
    finally:
        backed.file.close()


def collection_receipt(ln: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in COLLECTION_KEYS:
        collections = list(ln.Collection.filter(key=key).all())
        if len(collections) != 1:
            raise AssertionError(f"expected one Collection for {key}")
        collection = collections[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matching = [
            {"uid": str(member.uid), "key": str(member.key)}
            for member in members
            if str(member.key) == f"{PREFIX}/obs.parquet"
        ]
        if len(matching) != 1:
            raise AssertionError(f"target membership drift in {key}: {matching!r}")
        result[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_members": matching,
        }
    return result


def obs_receipt(obs: pd.DataFrame) -> dict[str, Any]:
    if len(obs) != EXPECTED_N_OBS or not obs.index.is_unique:
        raise AssertionError("OBS denominator or index uniqueness drift")
    required = {"cell_barcode", "original_obs_index", "is_control", "condition"}
    missing = sorted(required - set(obs.columns))
    if missing:
        raise AssertionError(f"OBS source-join fields absent: {missing}")
    barcode = obs["cell_barcode"].astype("string")
    original_index = obs["original_obs_index"].astype("string")
    controls = obs["is_control"].astype("boolean")
    condition_controls = obs["condition"].astype("string").eq("control")
    if not barcode.is_unique or not original_index.is_unique:
        raise AssertionError("OBS source identity is not unique")
    if not barcode.equals(original_index):
        raise AssertionError("cell_barcode/original_obs_index source join drift")
    if not controls.astype(bool).equals(condition_controls.astype(bool)):
        raise AssertionError("OBS control semantics drift")
    state_columns = sorted(column for column in obs if column.endswith("_state"))
    source_columns = sorted(column for column in obs if column.endswith("_source"))
    return {
        "rows": len(obs),
        "index_sha256": ordered_values_sha256(obs.index),
        "cell_barcode_equals_original_obs_index": True,
        "control_rows": int(controls.sum()),
        "state_columns": state_columns,
        "source_columns": source_columns,
        "canonical_state_column_count": len(state_columns),
    }


def var_receipt(var: pd.DataFrame, x_axis: pd.Index) -> dict[str, Any]:
    if len(var) != EXPECTED_N_VARS or not var.index.is_unique:
        raise AssertionError("VAR denominator or index uniqueness drift")
    if not var.index.astype(str).equals(x_axis.astype(str)):
        raise AssertionError("VAR/X feature axis ordering drift")
    stable = var.get("stable_feature_id", pd.Series(pd.NA, index=var.index)).astype(
        "string"
    )
    statuses = var.get(
        "stable_feature_id_mapping_status", pd.Series(pd.NA, index=var.index)
    ).astype("string")
    mapped = stable.dropna()
    feature_index = var.get("feature_index", pd.Series(pd.NA, index=var.index)).astype(
        "string"
    )
    return {
        "rows": len(var),
        "axis_sha256": ordered_values_sha256(var.index),
        "mapped_ensmusg_rows": int(mapped.str.fullmatch(r"ENSMUSG\d{11}").sum()),
        "non_ensmusg_non_null_rows": int(
            (~mapped.str.fullmatch(r"ENSMUSG\d{11}")).sum()
        ),
        "mapped_ids_unique": bool(mapped.is_unique),
        "unresolved_rows": int(stable.isna().sum()),
        "mapping_status_counts": {
            str(key): int(value) for key, value in statuses.value_counts(dropna=False).items()
        },
        "feature_index_complete_unique": bool(
            feature_index.notna().all() and feature_index.is_unique
        ),
        "organism_values": sorted(
            var.get("organism", pd.Series(pd.NA, index=var.index))
            .astype("string")
            .dropna()
            .unique()
            .tolist()
        ),
    }


def emit_product_execution(phase: str) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical_json(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "gse203592_live_readback",
                    "current": 1 if phase == "complete" else 0,
                    "denominator": 1,
                    "unit": "biological_dataset",
                }
            }
        ),
        flush=True,
    )


def main() -> int:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution: use the approved EU VM launcher")
    emit_product_execution("preflight")
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("wrong Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin branch")

    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_artifact, obs_history = current_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_linked_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_linked_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    x_axis, x = x_axis_receipt(x_artifact)
    collections = collection_receipt(ln)
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    if counts_before != counts_after:
        raise AssertionError("read-only verification changed registry counts")

    receipt = {
        "format": "pert-gym.gse203592-live-readback/v1",
        "task_id": TASK_ID,
        "dataset_id": PREFIX,
        "writes": 0,
        "deletions": 0,
        "triplet": {
            "obs": artifact_identity(obs_artifact),
            "x": artifact_identity(x_artifact),
            "var": artifact_identity(var_artifact),
            "obs_history": [artifact_identity(item) for item in obs_history],
        },
        "obs": obs_receipt(obs),
        "x": x,
        "var": var_receipt(var, x_axis),
        "collections": collections,
        "registry_counts": {"before": counts_before, "after": counts_after},
        "host": os.uname().nodename,
    }
    receipt["canonical_sha256"] = sha256_text(canonical_json(receipt))
    print("GSE203592_LIVE_READBACK=" + canonical_json(receipt), flush=True)
    emit_product_execution("complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
