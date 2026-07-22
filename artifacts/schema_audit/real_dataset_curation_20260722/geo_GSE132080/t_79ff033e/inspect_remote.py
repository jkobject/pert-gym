#!/usr/bin/env python3
"""Read-only source and accepted-triplet inspection for GEO GSE132080."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_79ff033e"
PREFIX = "prism_collection/GSE132080"
EXPECTED_N_OBS = 23_608
EXPECTED_N_VARS = 33_694
EXPECTED_SOURCE_N_OBS = 23_633
EXPECTED = {
    "obs": {"uid": "lhR6Ny3n8QcVeItH0002", "key": f"{PREFIX}/obs.parquet"},
    "x": {"uid": "NEbod0p6ws0H5wug0000", "key": f"{PREFIX}/X.h5ad"},
    "var": {"uid": "GJ1HqkBSHfDD1o4m0001", "key": f"{PREFIX}/var.parquet"},
}
SOURCE_ROOT = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE132nnn/GSE132080/suppl"
SOURCE_FILES = {
    "GSE132080_10X_barcodes.tsv.gz": 94_141,
    "GSE132080_10X_genes.tsv.gz": 264_786,
    "GSE132080_10X_matrix.mtx.gz": 352_247_428,
    "GSE132080_cell_identities.csv.gz": 366_158,
    "GSE132080_sgRNA_barcode_sequences_and_phenotypes.csv.gz": 4_463,
}


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
        "index_sample": frame.index.astype(str)[:8].tolist(),
        "dtypes": {str(column): str(frame[column].dtype) for column in frame.columns},
        "non_null": {str(column): int(frame[column].notna().sum()) for column in frame.columns},
        "nunique": {str(column): int(frame[column].dropna().astype(str).nunique()) for column in frame.columns},
        "value_samples": {
            str(column): frame[column].dropna().astype(str).drop_duplicates().head(8).tolist()
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
        raise AssertionError(f"cannot resolve feature Artifact: {value}")
    return records[-1]


def download_sources(root: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, expected_size in SOURCE_FILES.items():
        path = root / name
        url = f"{SOURCE_ROOT}/{name}"
        headers = subprocess.run(
            ["curl", "--silent", "--show-error", "--location", "--fail", "--head", url],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        ).stdout
        if not path.exists() or path.stat().st_size != expected_size:
            subprocess.run(
                [
                    "curl",
                    "--silent",
                    "--show-error",
                    "--location",
                    "--fail",
                    "--retry",
                    "3",
                    "--output",
                    str(path),
                    url,
                ],
                check=True,
                timeout=3600,
            )
        if path.stat().st_size != expected_size:
            raise AssertionError(f"source size drift: {name}={path.stat().st_size}")
        result[name] = {
            "url": url,
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
            "response_headers": [line for line in headers.splitlines() if line.lower().startswith(("last-modified:", "etag:", "content-length:"))],
            "local_path": str(path),
        }
    return result


def read_matrix_header(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt") as handle:
        banner = handle.readline().strip()
        while True:
            line = handle.readline().strip()
            if not line.startswith("%"):
                break
    rows, columns, entries = map(int, line.split())
    return {"banner": banner, "rows": rows, "columns": columns, "entries": entries}


def axis_relations(obs: pd.DataFrame, barcodes: pd.Index, identities: pd.DataFrame) -> dict[str, Any]:
    candidates: dict[str, pd.Index] = {"obs_index": obs.index.astype(str)}
    if "original_obs_index" in obs:
        candidates["original_obs_index"] = pd.Index(obs["original_obs_index"].astype(str))
    identity_index = identities.index.astype(str)
    result: dict[str, Any] = {}
    for name, axis in candidates.items():
        result[name] = {
            "unique": bool(axis.is_unique),
            "ordered_equals_barcodes": axis.equals(barcodes),
            "set_equals_barcodes": len(axis) == len(barcodes) and set(axis) == set(barcodes),
            "ordered_equals_identity_index": axis.equals(identity_index),
            "set_equals_identity_index": len(axis) == len(identity_index) and set(axis) == set(identity_index),
            "sha256": ordered_sha256(axis),
        }
    return result


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
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse132080-sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = download_sources(root)

    barcodes_path = Path(sources["GSE132080_10X_barcodes.tsv.gz"]["local_path"])
    genes_path = Path(sources["GSE132080_10X_genes.tsv.gz"]["local_path"])
    identities_path = Path(sources["GSE132080_cell_identities.csv.gz"]["local_path"])
    sgrna_path = Path(sources["GSE132080_sgRNA_barcode_sequences_and_phenotypes.csv.gz"]["local_path"])
    matrix_path = Path(sources["GSE132080_10X_matrix.mtx.gz"]["local_path"])
    barcodes_frame = pd.read_csv(barcodes_path, sep="\t", header=None, dtype="string")
    genes = pd.read_csv(genes_path, sep="\t", header=None, dtype="string")
    identities = pd.read_csv(identities_path, index_col=0)
    sgrna = pd.read_csv(sgrna_path)
    barcodes = pd.Index(barcodes_frame.iloc[:, 0].astype(str))

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    obs_artifact, obs_history = latest_artifact(ln, EXPECTED["obs"]["key"])
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    for role, artifact in (("obs", obs_artifact), ("x", x_artifact), ("var", var_artifact)):
        if str(artifact.uid) != EXPECTED[role]["uid"] or str(artifact.key) != EXPECTED[role]["key"]:
            raise AssertionError(f"accepted {role} identity drift")
    if len(obs) != EXPECTED_N_OBS or len(var) != EXPECTED_N_VARS:
        raise AssertionError("accepted triplet denominator drift")
    matrix_header = read_matrix_header(matrix_path)
    if (matrix_header["rows"], matrix_header["columns"]) != (
        EXPECTED_N_VARS,
        EXPECTED_SOURCE_N_OBS,
    ):
        raise AssertionError(f"source matrix shape drift: {matrix_header}")
    if (
        len(barcodes) != EXPECTED_SOURCE_N_OBS
        or len(genes) != EXPECTED_N_VARS
        or len(identities) != EXPECTED_N_OBS
    ):
        raise AssertionError("source sidecar denominator drift")

    stable = var["stable_feature_id"].astype("string") if "stable_feature_id" in var else pd.Series([], dtype="string")
    report = {
        "format": "pert-gym.gse132080-source-inspection/v1",
        "task_id": TASK_ID,
        "dataset_id": "geo/GSE132080",
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {"free_disk_bytes": capacity.free_disk_bytes, "available_memory_bytes": capacity.available_memory_bytes},
        "sources": sources,
        "source_tables": {
            "barcodes": frame_summary(barcodes_frame),
            "genes": frame_summary(genes),
            "matrix": matrix_header,
            "cell_identities": frame_summary(identities),
            "sgrna_phenotypes": frame_summary(sgrna),
        },
        "current": {
            "obs": artifact_identity(obs_artifact),
            "obs_history": [artifact_identity(item) for item in obs_history],
            "obs_frame": frame_summary(obs),
            "x": artifact_identity(x_artifact),
            "var": artifact_identity(var_artifact),
            "var_frame": frame_summary(var),
            "var_uniqueness": {
                "index_unique": bool(var.index.is_unique),
                "duplicate_index_rows": int(var.index.duplicated(keep=False).sum()),
                "duplicate_index_values": var.index[var.index.duplicated(keep=False)].astype(str)[:50].tolist(),
                "stable_feature_id_present": "stable_feature_id" in var,
                "stable_feature_id_non_null": int(stable.notna().sum()),
                "stable_feature_id_unique": bool(stable.dropna().is_unique),
                "stable_feature_id_ensg": int(stable.str.fullmatch(r"ENSG\d{11}", na=False).sum()),
            },
            "links": {"obs_to_x": True, "x_to_var": True},
        },
        "axis_relations": axis_relations(obs, barcodes, identities),
        "source_axis_relations": {
            "barcodes_unique": bool(barcodes.is_unique),
            "identities_index_unique": bool(identities.index.is_unique),
            "barcodes_equal_identity_index": barcodes.equals(identities.index.astype(str)),
            "barcodes_set_equal_identity_index": set(barcodes) == set(identities.index.astype(str)),
            "barcodes_without_identity_count": len(
                barcodes.difference(identities.index.astype(str))
            ),
            "barcodes_without_identity_sample": barcodes.difference(
                identities.index.astype(str)
            )[:50].tolist(),
            "identity_without_barcode_count": len(
                identities.index.astype(str).difference(barcodes)
            ),
            "genes_index0_unique": bool(genes.iloc[:, 0].is_unique),
            "genes_index1_unique": bool(genes.iloc[:, -1].is_unique),
        },
        "collection": collection_snapshot(ln),
        "invariants": {"source_file_count": len(sources), "source_total_bytes": sum(item["size"] for item in sources.values()), "writes": 0, "x_payload_materialized": False},
    }
    print("GSE132080_REPORT_JSON=" + canonical(report))


if __name__ == "__main__":
    main()
