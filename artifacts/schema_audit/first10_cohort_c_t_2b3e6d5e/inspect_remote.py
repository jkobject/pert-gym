#!/usr/bin/env python3
"""Read-only bounded live audit for first-10 cohort C (GSM5901230-32)."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import numpy as np
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_2b3e6d5e"
DATASETS = {
    "GSM5901230": {
        "timepoint_days": 6.0,
        "expected_shape": [4807, 20631],
        "expected_nnz": 17517151,
        "expected_sum": 58069115,
        "uids": {
            "obs": "770vKgYITsWgwsQ50000",
            "x": "vn537Bt2uFhZncXo0000",
            "var": "NCuDtw4vtWZpliBU0000",
        },
    },
    "GSM5901231": {
        "timepoint_days": 9.0,
        "expected_shape": [6430, 20631],
        "expected_nnz": 26039746,
        "expected_sum": 97683665,
        "uids": {
            "obs": "HAjNzeHiAFVj18Bl0000",
            "x": "DMOKXzr7fGa2BzFx0000",
            "var": "2L5w9PF2KfG568jw0000",
        },
    },
    "GSM5901232": {
        "timepoint_days": 18.0,
        "expected_shape": [4396, 20631],
        "expected_nnz": 17949355,
        "expected_sum": 56570353,
        "uids": {
            "obs": "GbF6GoN62lv1gHph0000",
            "x": "ZX3O7cVwL7bFPZYv0000",
            "var": "v6fulA6ihI0nfyDF0000",
        },
    },
}
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


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
    columns: dict[str, Any] = {}
    for column in frame.columns:
        series = frame[column]
        text = series.dropna().astype(str)
        counts = text.value_counts(dropna=False)
        columns[str(column)] = {
            "dtype": str(series.dtype),
            "rows": len(series),
            "non_null": int(series.notna().sum()),
            "null": int(series.isna().sum()),
            "nunique_non_null": int(text.nunique()),
            "top_values": [
                {"value": str(value), "count": int(count)}
                for value, count in counts.head(12).items()
            ],
            "control_character_rows": int(text.str.contains(CONTROL_RE).sum()),
        }
    index = frame.index.astype(str)
    return {
        "rows": len(frame),
        "column_order": list(map(str, frame.columns)),
        "columns": columns,
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(index),
        "index_control_character_rows": int(
            index.to_series().str.contains(CONTROL_RE).sum()
        ),
        "index_sample": index[:12].tolist(),
    }


def var_identity_summary(var: pd.DataFrame) -> dict[str, Any]:
    feature_id = var["feature_id"].astype(str)
    gene_symbol = var["gene_symbol"].astype(str)
    return {
        "rows": len(var),
        "index_unique": bool(var.index.is_unique),
        "index_equals_feature_id": bool(
            pd.Series(var.index.astype(str), index=var.index).equals(feature_id)
        ),
        "feature_id_unique": bool(feature_id.is_unique),
        "feature_id_exact_ensg_count": int(
            feature_id.str.fullmatch(r"ENSG\d{11}").sum()
        ),
        "feature_id_control_character_rows": int(
            feature_id.str.contains(CONTROL_RE).sum()
        ),
        "gene_symbol_non_null": int(var["gene_symbol"].notna().sum()),
        "gene_symbol_unique": int(gene_symbol.nunique()),
        "duplicate_gene_symbol_rows": int(gene_symbol.duplicated(keep=False).sum()),
    }


def identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": None if artifact.version is None else str(artifact.version),
        "size": int(artifact.size),
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at),
        "description": None
        if artifact.description is None
        else str(artifact.description),
        "path": str(artifact.path),
        "is_latest": bool(artifact.is_latest),
    }


def resolve(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    records = list(ln.Artifact.filter(uid=value).all())
    if len(records) == 1:
        return records[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve linked Artifact: {value}")
    return records[-1]


def object_metadata(uri: str) -> dict[str, Any]:
    proc = subprocess.run(
        [
            "gcloud",
            "storage",
            "objects",
            "describe",
            "--billing-project=jkobject-1549353370965",
            "--format=json",
            uri,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    raw = json.loads(proc.stdout)
    return {
        key: raw.get(key)
        for key in (
            "bucket",
            "name",
            "generation",
            "size",
            "md5Hash",
            "crc32c",
            "etag",
            "timeCreated",
            "updated",
        )
    }


def download_object(uri: str, destination: Path) -> Path:
    subprocess.run(
        [
            "gcloud",
            "storage",
            "cp",
            "--billing-project=jkobject-1549353370965",
            uri,
            str(destination),
        ],
        check=True,
        timeout=600,
    )
    return destination


def inspect_x(path: Path) -> dict[str, Any]:
    with h5py.File(path, "r") as handle:
        x_obj = handle["X"]
        encoding = x_obj.attrs.get("encoding-type")
        if isinstance(encoding, bytes):
            encoding = encoding.decode()
        if isinstance(x_obj, h5py.Group):
            data = x_obj["data"]
            total = 0.0
            minimum = None
            maximum = None
            integer_values = True
            block = 2_000_000
            for start in range(0, len(data), block):
                values = np.asarray(data[start : start + block])
                if values.size:
                    current_min = float(values.min())
                    current_max = float(values.max())
                    minimum = (
                        current_min if minimum is None else min(minimum, current_min)
                    )
                    maximum = (
                        current_max if maximum is None else max(maximum, current_max)
                    )
                    integer_values = integer_values and bool(
                        np.equal(values, np.floor(values)).all()
                    )
                    total += float(values.sum(dtype=np.float64))
            x_storage = {
                "kind": "sparse_group",
                "encoding_type": str(encoding),
                "shape_attr": [int(v) for v in x_obj.attrs["shape"]],
                "data_dtype": str(data.dtype),
                "indices_dtype": str(x_obj["indices"].dtype),
                "indptr_dtype": str(x_obj["indptr"].dtype),
                "stored_nnz": int(len(data)),
                "minimum": minimum,
                "maximum": maximum,
                "sum": int(total) if total.is_integer() else total,
                "all_values_integer": integer_values,
            }
        else:
            x_storage = {
                "kind": "dense_dataset",
                "encoding_type": str(encoding),
                "shape": [int(v) for v in x_obj.shape],
                "dtype": str(x_obj.dtype),
            }
    backed = ad.read_h5ad(path, backed="r")
    result = {
        "shape": [int(backed.n_obs), int(backed.n_vars)],
        "obs_names_sha256": ordered_sha256(backed.obs_names.astype(str)),
        "var_names_sha256": ordered_sha256(backed.var_names.astype(str)),
        "obs_names_unique": bool(backed.obs_names.is_unique),
        "var_names_unique": bool(backed.var_names.is_unique),
        "storage": x_storage,
    }
    backed.file.close()
    return result


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    report: dict[str, Any] = {
        "format": "pert-gym.first10-cohort-c-live-audit/v1",
        "task_id": TASK_ID,
        "host": capacity.hostname,
        "pid": os.getpid(),
        "writes": 0,
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "datasets": {},
    }
    local_root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-cohort-c-"))
    for dataset, expected in DATASETS.items():
        keys = {
            "obs": f"data/cleaned/{dataset}/obs.parquet",
            "x": f"data/cleaned/{dataset}/X.h5ad",
            "var": f"data/cleaned/{dataset}/var.parquet",
        }
        artifacts: dict[str, Any] = {}
        histories: dict[str, Any] = {}
        for role, key in keys.items():
            records = list(ln.Artifact.filter(key=key).all())
            records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
            if not records:
                raise AssertionError(f"missing current key {key}")
            artifacts[role] = records[-1]
            histories[role] = [identity(item) for item in records]
            assert str(records[-1].uid) == expected["uids"][role]
            assert bool(records[-1].is_latest)

        obs_art = artifacts["obs"]
        x_art = artifacts["x"]
        var_art = artifacts["var"]
        linked_x = resolve(ln, obs_art.features.get_values()["X"])
        linked_var = resolve(ln, x_art.features.get_values()["var"])
        assert str(linked_x.uid) == str(x_art.uid)
        assert str(linked_var.uid) == str(var_art.uid)

        path_metadata_before = {
            role: object_metadata(str(artifact.path))
            for role, artifact in artifacts.items()
        }

        dataset_root = local_root / dataset
        dataset_root.mkdir()
        obs_path = download_object(str(obs_art.path), dataset_root / "obs.parquet")
        x_path = download_object(str(x_art.path), dataset_root / "X.h5ad")
        var_path = download_object(str(var_art.path), dataset_root / "var.parquet")
        obs = pd.read_parquet(obs_path)
        var = pd.read_parquet(var_path)
        x_summary = inspect_x(x_path)
        backed = ad.read_h5ad(x_path, backed="r")
        x_obs_names = backed.obs_names.astype(str).copy()
        x_var_names = backed.var_names.astype(str).copy()
        backed.file.close()
        shape = expected["expected_shape"]
        assert [len(obs), len(var)] == shape
        assert x_summary["shape"] == shape
        assert obs.index.astype(str).equals(pd.Index(x_obs_names))
        assert var.index.astype(str).equals(pd.Index(x_var_names))
        assert x_summary["storage"]["stored_nnz"] == expected["expected_nnz"]
        assert x_summary["storage"]["sum"] == expected["expected_sum"]

        path_metadata_after = {
            role: object_metadata(str(artifact.path))
            for role, artifact in artifacts.items()
        }
        for role in artifacts:
            assert (
                path_metadata_before[role]["generation"]
                == path_metadata_after[role]["generation"]
            )
            assert (
                path_metadata_before[role]["size"] == path_metadata_after[role]["size"]
            )
        report["datasets"][dataset] = {
            "expected": expected,
            "artifacts": {
                role: identity(artifact) for role, artifact in artifacts.items()
            },
            "histories": histories,
            "object_metadata": path_metadata_after,
            "object_metadata_before_download": path_metadata_before,
            "payload_sha256": {
                "obs": sha256_file(obs_path),
                "x": sha256_file(x_path),
                "var": sha256_file(var_path),
            },
            "obs": frame_summary(obs),
            "var": frame_summary(var),
            "var_all_rows_identity": var_identity_summary(var),
            "x": x_summary,
            "axis_parity": {
                "obs_index_equals_x_obs_names": obs.index.astype(str).equals(
                    pd.Index(x_obs_names)
                ),
                "var_index_equals_x_var_names": var.index.astype(str).equals(
                    pd.Index(x_var_names)
                ),
            },
            "links": {
                "obs_to_x": str(linked_x.uid) == str(x_art.uid),
                "x_to_var": str(linked_var.uid) == str(var_art.uid),
            },
        }
    var_hashes = {
        dataset: payload["payload_sha256"]["var"]
        for dataset, payload in report["datasets"].items()
    }
    report["family_checks"] = {
        "all_var_payloads_byte_identical": len(set(var_hashes.values())) == 1,
        "var_payload_sha256_by_dataset": var_hashes,
        "dataset_count": len(DATASETS),
    }
    print("COHORT_C_LIVE_AUDIT=" + canonical(report), flush=True)


if __name__ == "__main__":
    main()
