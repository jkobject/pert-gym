#!/usr/bin/env python3
"""Bounded read-only Lamin audit for first-10 cohort B.

Loads exactly four small OBS/VAR dataframes and metadata for their X artifacts.
It never calls save(), track(), delete(), revise(), feature setters, or GCS APIs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from tools.lamin_context import connect_pertdata, ensure_project_cache

DATASETS = ("GSE194214", "GSE269572", "GSM5901228", "GSM5901229")
ROLES = {"obs": "obs.parquet", "X": "X.h5ad", "var": "var.parquet"}
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
ENSEMBL_RE = re.compile(r"^ENS[A-Z]*G\d+(?:\.\d+)?$")


def scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def artifact_metadata(artifact: Any) -> dict[str, Any]:
    return {
        name: scalar(getattr(artifact, name, None))
        for name in (
            "key",
            "uid",
            "hash",
            "size",
            "n_observations",
            "suffix",
            "otype",
            "created_at",
            "updated_at",
            "storage_id",
        )
    }


def feature_links(artifact: Any) -> dict[str, str]:
    try:
        values = artifact.features.get_values()
    except Exception as exc:  # fail closed but retain exact error
        return {"_error": f"{type(exc).__name__}: {exc}"}
    return {
        str(name): str(getattr(value, "key", value)) for name, value in values.items()
    }


def bounded_candidates(ln: Any, dataset: str) -> list[dict[str, Any]]:
    records = list(ln.Artifact.filter(key__icontains=dataset).all())[:40]
    return [artifact_metadata(record) for record in records]


def summarize_series(series: pd.Series) -> dict[str, Any]:
    missing = int(series.isna().sum())
    non_null = series.dropna()
    unique = int(non_null.nunique(dropna=True))
    sample_values: list[Any] = []
    value_counts: dict[str, int] = {}
    if unique <= 20:
        sample_values = [scalar(value) for value in non_null.drop_duplicates().tolist()]
        value_counts = {
            str(value): int(count)
            for value, count in non_null.value_counts(dropna=False).items()
        }
    numeric_min = None
    numeric_max = None
    if pd.api.types.is_numeric_dtype(series.dtype) and len(non_null):
        numeric_min = scalar(non_null.min())
        numeric_max = scalar(non_null.max())
    return {
        "dtype": str(series.dtype),
        "missing": missing,
        "missing_fraction": missing / len(series) if len(series) else None,
        "unique_non_null": unique,
        "values_if_at_most_20": sample_values,
        "value_counts_if_at_most_20": value_counts,
        "numeric_min": numeric_min,
        "numeric_max": numeric_max,
    }


def dataframe_summary(frame: pd.DataFrame, *, var: bool) -> dict[str, Any]:
    index_values = frame.index.astype(str)
    control_char_rows = int(
        sum(bool(CONTROL_RE.search(value)) for value in index_values)
    )
    result: dict[str, Any] = {
        "shape": list(frame.shape),
        "index_name": scalar(frame.index.name),
        "index_dtype": str(frame.index.dtype),
        "index_sha256": hashlib.sha256(
            "\n".join(index_values).encode("utf-8")
        ).hexdigest(),
        "index_unique": bool(frame.index.is_unique),
        "index_missing": int(frame.index.isna().sum()),
        "index_control_character_count": control_char_rows,
        "columns": [str(column) for column in frame.columns],
        "column_audit": {
            str(column): summarize_series(frame[column]) for column in frame.columns
        },
    }
    if var:
        result["index_ensembl_count"] = int(
            sum(bool(ENSEMBL_RE.fullmatch(value)) for value in index_values)
        )
        result["index_ensembl_fraction"] = (
            result["index_ensembl_count"] / len(frame) if len(frame) else None
        )
        result["index_duplicate_count"] = int(frame.index.duplicated().sum())
        result["index_empty_string_count"] = int(
            sum(not value.strip() for value in index_values)
        )
        result["column_control_character_counts"] = {
            str(column): int(
                frame[column]
                .dropna()
                .astype(str)
                .map(lambda value: bool(CONTROL_RE.search(value)))
                .sum()
            )
            for column in frame.columns
        }
    return result


def exact_one(ln: Any, key: str) -> Any:
    matches = list(ln.Artifact.filter(key=key).all())
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one active artifact for {key!r}, got {len(matches)}"
        )
    return matches[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    ensure_project_cache()
    ln = connect_pertdata()
    branch = ln.setup.settings.branch
    if branch.name != "jkobject" or branch.uid != "GCjqQtGwPzkY":
        raise RuntimeError(f"unexpected branch {branch.name}/{branch.uid}")

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instance": ln.setup.settings.instance.slug,
        "branch": {"name": branch.name, "uid": branch.uid},
        "read_boundary": {
            "datasets": list(DATASETS),
            "payloads_loaded": "exactly four obs.parquet and four var.parquet dataframes",
            "X_policy": "registry metadata and feature links only; no X payload load",
            "writes": "none",
        },
        "datasets": {},
    }

    for dataset in DATASETS:
        artifacts: dict[str, Any] = {}
        entry: dict[str, Any] = {
            "bounded_key_candidates": bounded_candidates(ln, dataset),
            "artifacts": {},
        }
        for role, suffix in ROLES.items():
            key = f"data/cleaned/{dataset}/{suffix}"
            artifact = exact_one(ln, key)
            artifacts[role] = artifact
            entry["artifacts"][role] = {
                "metadata": artifact_metadata(artifact),
                "feature_links": feature_links(artifact),
            }

        obs = artifacts["obs"].load()
        var = artifacts["var"].load()
        if not isinstance(obs, pd.DataFrame) or not isinstance(var, pd.DataFrame):
            raise TypeError(
                f"{dataset}: obs/var did not both load as pandas DataFrames"
            )
        entry["obs"] = dataframe_summary(obs, var=False)
        entry["var"] = dataframe_summary(var, var=True)
        entry["parity"] = {
            "obs_rows_equal_X_n_observations": (
                None
                if artifacts["X"].n_observations is None
                else len(obs) == artifacts["X"].n_observations
            ),
            "obs_rows": len(obs),
            "X_n_observations": artifacts["X"].n_observations,
            "var_rows": len(var),
            "obs_X_link_exact": (
                feature_links(artifacts["obs"]).get("X") == artifacts["X"].key
            ),
            "X_var_link_exact": (
                feature_links(artifacts["X"]).get("var") == artifacts["var"].key
            ),
            "same_prefix_var": artifacts["var"].key
            == f"data/cleaned/{dataset}/var.parquet",
        }
        report["datasets"][dataset] = entry

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "out": str(args.out),
                "datasets": list(DATASETS),
                "instance": report["instance"],
                "branch": report["branch"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
