#!/usr/bin/env python3
"""Download and inspect the complete filtered GSE207360 Seurat source on the EU VM."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_a2234c88"
PREFIX = "prism_collection/GSE207360"
EXPECTED_N_OBS = 12_487
EXPECTED_N_VARS = 60_736
FILTERED_SOURCE = {
    "url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE207nnn/GSE207360/suppl/GSE207360_Human_Mouse_filtered.rds.gz",
    "size": 4_174_159_639,
    "last_modified": "Fri, 01 Jul 2022 16:57:08 GMT",
}
R_EXTRACTOR = Path(__file__).with_name("extract_seurat_metadata.R")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(16 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: pd.Index) -> str:
    return hashlib.sha256("\n".join(values.astype(str)).encode()).hexdigest()


def latest_artifact(ln: Any, key: str) -> Any:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records or not bool(records[-1].is_latest):
        raise AssertionError(f"missing latest artifact: {key}")
    return records[-1]


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    return latest_artifact(ln, value)


def download_source(root: Path) -> Path:
    path = root / "GSE207360_Human_Mouse_filtered.rds.gz"
    if not path.exists() or path.stat().st_size != FILTERED_SOURCE["size"]:
        subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--location",
                "--fail",
                "--retry",
                "5",
                "--retry-all-errors",
                "--continue-at",
                "-",
                "--output",
                str(path),
                FILTERED_SOURCE["url"],
            ],
            check=True,
            timeout=7_200,
        )
    if path.stat().st_size != FILTERED_SOURCE["size"]:
        raise AssertionError("filtered source size drift")
    return path


def extract_metadata(source: Path, root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    tsv = root / "filtered_metadata.tsv"
    summary_path = root / "filtered_summary.json"
    with source.open("rb") as source_handle:
        outer = subprocess.Popen(
            ["gzip", "-dc"], stdin=source_handle, stdout=subprocess.PIPE
        )
        assert outer.stdout is not None
        inner = subprocess.Popen(["gzip", "-dc"], stdin=outer.stdout, stdout=subprocess.PIPE)
        outer.stdout.close()
        assert inner.stdout is not None
        r_process = subprocess.run(
            ["Rscript", "--vanilla", str(R_EXTRACTOR), str(tsv), str(summary_path)],
            stdin=inner.stdout,
            check=False,
            timeout=7_200,
        )
        inner.stdout.close()
        inner_returncode = inner.wait()
        outer_returncode = outer.wait()
    if r_process.returncode or inner_returncode or outer_returncode:
        raise RuntimeError(
            f"filtered RDS extraction failed: r={r_process.returncode} inner={inner_returncode} outer={outer_returncode}"
        )
    frame = pd.read_csv(tsv, sep="\t", index_col=0)
    summary = json.loads(summary_path.read_text())
    if len(frame) != EXPECTED_N_OBS or not frame.index.is_unique:
        raise AssertionError("filtered source OBS denominator/identity drift")
    if summary["counts_rows"] != EXPECTED_N_VARS or summary["counts_columns"] != EXPECTED_N_OBS:
        raise AssertionError("filtered source matrix shape drift")
    if not summary["counts_integral"] or summary["counts_min"] < 0:
        raise AssertionError("filtered source counts semantics drift")
    if not summary["counts_colnames_equal_metadata"]:
        raise AssertionError("filtered source count/metadata axis drift")
    return frame, summary


def compare_column(source: pd.Series, current: pd.Series) -> int:
    if pd.api.types.is_numeric_dtype(source) and pd.api.types.is_numeric_dtype(current):
        left = pd.to_numeric(source, errors="coerce").to_numpy(dtype=float)
        right = pd.to_numeric(current, errors="coerce").to_numpy(dtype=float)
        return int(np.count_nonzero(~np.isclose(left, right, equal_nan=True, rtol=0, atol=1e-12)))
    left = source.astype("string").fillna("<NA>")
    right = current.astype("string").fillna("<NA>")
    return int((~left.eq(right)).sum())


def source_join(source: pd.DataFrame, obs: pd.DataFrame) -> dict[str, Any]:
    keys = pd.Index(obs["original_obs_index"].astype(str))
    if not keys.equals(source.index.astype(str)):
        raise AssertionError("filtered source/current OBS ordered identity drift")
    joined = source.set_axis(obs.index)
    missing_current = sorted(set(source.columns) - set(obs.columns))
    mismatches = {
        column: compare_column(joined[column], obs[column])
        for column in source.columns
        if column in obs
    }
    if any(mismatches.values()):
        raise AssertionError(f"filtered source/current metadata mismatch: {mismatches}")
    return {
        "rows": len(joined),
        "ordered_index_sha256": ordered_sha256(keys),
        "source_columns": list(source.columns),
        "source_columns_absent_from_current": missing_current,
        "column_mismatch_counts": mismatches,
        "sample_by_source_sample_name": {
            str(key): int(value) for key, value in source["sample.name"].value_counts().items()
        },
        "cell_type_counts": {
            str(key): int(value) for key, value in source["Cell_type1"].value_counts().items()
        },
        "sample_cell_type_counts": {
            f"{sample}|{cell_type}": int(value)
            for (sample, cell_type), value in source.groupby(
                ["sample.name", "Cell_type1"], observed=True
            ).size().items()
        },
    }


def inspect_x(x_artifact: Any, expected_obs: pd.Index) -> dict[str, Any]:
    path = Path(x_artifact.cache())
    backed = ad.read_h5ad(path, backed="r")
    if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
        raise AssertionError("accepted X shape drift")
    if not backed.obs_names.astype(str).equals(expected_obs.astype(str)):
        raise AssertionError("accepted X/OBS row-axis drift")
    minimum = float("inf")
    maximum = float("-inf")
    nonzero = 0
    non_integral = 0
    for start in range(0, backed.n_obs, 128):
        values = backed.X[start : min(start + 128, backed.n_obs)]
        data = values.data if hasattr(values, "data") else np.asarray(values).ravel()
        if len(data):
            minimum = min(minimum, float(np.min(data)))
            maximum = max(maximum, float(np.max(data)))
            nonzero += int(np.count_nonzero(data))
            non_integral += int(np.count_nonzero(data != np.floor(data)))
    receipt = {
        "uid": str(x_artifact.uid),
        "hash": str(x_artifact.hash),
        "shape": [backed.n_obs, backed.n_vars],
        "dtype": str(backed.X.dtype),
        "obs_names_sha256": ordered_sha256(backed.obs_names),
        "var_names_sha256": ordered_sha256(backed.var_names),
        "minimum_stored_value": minimum,
        "maximum_stored_value": maximum,
        "stored_nonzero": nonzero,
        "non_integral_stored_values": non_integral,
        "raw_counts": minimum >= 0 and non_integral == 0,
    }
    backed.file.close()
    if not receipt["raw_counts"]:
        raise AssertionError("accepted X is not a nonnegative integral count matrix")
    return receipt


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    root = Path("/var/tmp/pert-gym-gse207360")
    root.mkdir(parents=True, exist_ok=True)
    source_path = download_source(root)
    source_sha256 = sha256_file(source_path)
    source, r_summary = extract_metadata(source_path, root)

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    obs_artifact = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    join = source_join(source, obs)
    x_receipt = inspect_x(x_artifact, pd.Index(obs["original_obs_index"].astype(str)))
    report = {
        "format": "pert-gym.gse207360-filtered-source/v1",
        "task_id": TASK_ID,
        "host": {
            "hostname": capacity.hostname,
            "pid": os.getpid(),
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "source": {
            **FILTERED_SOURCE,
            "sha256": source_sha256,
            "local_size": source_path.stat().st_size,
            "r_summary": r_summary,
            "metadata_tsv_sha256": sha256_file(root / "filtered_metadata.tsv"),
            "metadata_rows": len(source),
            "metadata_columns": list(source.columns),
            "metadata_dtypes": {column: str(source[column].dtype) for column in source.columns},
        },
        "current_obs": {
            "uid": str(obs_artifact.uid),
            "hash": str(obs_artifact.hash),
            "rows": len(obs),
            "columns": list(obs.columns),
        },
        "source_join": join,
        "x_semantics": x_receipt,
        "invariants": {"writes": 0, "x_revisions": 0, "obs_revisions": 0, "var_revisions": 0},
        "completed_at": int(time.time()),
    }
    print("PRODUCT_EXECUTION=" + canonical({"product_execution": {
        "host": capacity.hostname,
        "pid": os.getpid(),
        "phase": "checkpointing",
        "payload_heartbeat_at": int(time.time()),
        "metric": "accepted_obs_datasets",
        "current": 9,
        "denominator": 70,
    }}), flush=True)
    print("GSE207360_FILTERED_SOURCE_REPORT=" + canonical(report), flush=True)


if __name__ == "__main__":
    main()
