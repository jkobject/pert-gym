#!/usr/bin/env python3
"""VM-only, read-only source/accepted-OBS inspection for Cellarity public data."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h5py
import pandas as pd
from anndata.io import read_elem
from google.cloud import storage

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import BILLING_PROJECT, preflight

TASK_ID = "t_9c09e453"
REAL_DATASET_ID = "cellarity/public-collection"
REPORT_FORMAT = "pert-gym.cellarity-source-inspection/v1"
STAGING_PREFIX = "pert-gym/staging/pert-gym/curation/cellarity/t_9c09e453"

MEMBERS = (
    {
        "accession": "GSE305370",
        "prefix": "cellarity/GSE305370/GSE305370_citeseq_alldonors_alldays",
        "filename": "GSE305370_citeseq_alldonors_alldays.h5ad",
        "obs_uid": "dAFlb7abls3HQl5M0001",
        "x_uid": "afizT5wyemDxlpat0000",
        "var_uid": "iCyDpIOMsfFaSz6O0002",
        "n_obs": 134_583,
    },
    {
        "accession": "GSE305370",
        "prefix": "cellarity/GSE305370/GSE305370_multiome_alldonors_alldays",
        "filename": "GSE305370_multiome_alldonors_alldays.h5ad",
        "obs_uid": "MLE2MEOCoBJT5kPE0001",
        "x_uid": "kUAJmEMAqFPqcQ6s0000",
        "var_uid": "pE0RfeqVYLg08lPG0002",
        "n_obs": 164_462,
    },
    {
        "accession": "GSE305370",
        "prefix": "cellarity/GSE305370/GSE305370_rna_combined_with_velocity_and_refined_annotations",
        "filename": "GSE305370_rna_combined_with_velocity_and_refined_annotations.h5ad",
        "obs_uid": "RJbcZEfscysBCeMj0000",
        "x_uid": "YarbYWCMuzxYlHi10000",
        "var_uid": "h9D2Ff07ruRv3kJe0002",
        "n_obs": 135_341,
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day0-7_normalized_counts_with_celltype_annotations",
        "filename": "GSE305979_day0-7_normalized_counts_with_celltype_annotations.h5ad",
        "obs_uid": "Is3hhpzyzvMqRAZ20001",
        "x_uid": "6pRQ9KGEwDyKBt430000",
        "var_uid": "FvUX0GmLYUIGty6e0001",
        "n_obs": 146_735,
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day0_raw_counts",
        "filename": "GSE305979_day0_raw_counts.h5ad",
        "obs_uid": "ugxvzG3isKtDchNG0001",
        "x_uid": "yZKh7KulUomkaK8w0000",
        "var_uid": "9AWpNsCAn10GS9ml0001",
        "n_obs": 2_875,
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day1-7_demuxed_counts",
        "filename": "GSE305979_day1-7_demuxed_counts.h5ad",
        "obs_uid": "SVmujjG5GwXU0KZl0001",
        "x_uid": "Tni8nLRSMPBqOklS0000",
        "var_uid": "Lg0yhjurEtaPPTP70001",
        "n_obs": 143_396,
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day1-7_raw_counts",
        "filename": "GSE305979_day1-7_raw_counts.h5ad",
        "obs_uid": "3gdXNHm2avLD3dLG0001",
        "x_uid": "YVyGoCiaqFzitCBv0000",
        "var_uid": "Zr3NlWlca2YIou4K0001",
        "n_obs": 223_971,
    },
    {
        "accession": "GSE306429",
        "prefix": "cellarity/GSE306429/GSE306429_combined_demuxed",
        "filename": "GSE306429_combined_demuxed.h5ad",
        "obs_uid": "DWsyQzOmlTS19kUr0001",
        "x_uid": "s4Vsts7vMewJqf9n0000",
        "var_uid": "ezlGxRjcR0jqdW6E0001",
        "n_obs": 1_257_778,
    },
    {
        "accession": "GSE306429",
        "prefix": "cellarity/GSE306429/GSE306429_combined_pseudobulk",
        "filename": "GSE306429_combined_pseudobulk.h5ad",
        "obs_uid": "JL1I0jwLaqqNU1vu0002",
        "x_uid": "AmycM9CgrR1r551F0000",
        "var_uid": "HzlKD1eIPXTvAQzu0001",
        "n_obs": 1_737,
    },
    {
        "accession": "GSE306429",
        "prefix": "cellarity/GSE306429/GSE306429_combined_vscores",
        "filename": "GSE306429_combined_vscores.h5ad",
        "obs_uid": "K0em7aNI1182HV8m0001",
        "x_uid": "sofPAagXB7Nifke80000",
        "var_uid": "wQUnGNTREkmvE9XG0001",
        "n_obs": 1_563,
    },
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


def multiset_sha256(values: pd.Index) -> str:
    hashed = pd.util.hash_pandas_object(
        pd.Index(values.astype(str)), index=False
    ).to_numpy(copy=True)
    hashed.sort()
    return hashlib.sha256(hashed.tobytes()).hexdigest()


def source_url(member: dict[str, Any]) -> str:
    accession = member["accession"]
    stem = accession[:6] + "nnn"
    return (
        f"https://ftp.ncbi.nlm.nih.gov/geo/series/{stem}/{accession}/suppl/"
        f"{member['filename']}"
    )


def download(member: dict[str, Any], root: Path) -> tuple[Path, dict[str, Any]]:
    path = root / member["filename"]
    url = source_url(member)
    subprocess.run(
        [
            "curl",
            "--location",
            "--fail",
            "--show-error",
            "--silent",
            "--retry",
            "5",
            "--continue-at",
            "-",
            "--output",
            str(path),
            url,
        ],
        check=True,
        timeout=4 * 3600,
    )
    return path, {
        "url": url,
        "size": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def exact_artifact(ln: Any, uid: str, key: str) -> Any:
    records = [
        item
        for item in ln.Artifact.filter(uid=uid).all()
        if str(item.uid) == uid and str(item.key) == key
    ]
    if len(records) != 1:
        raise AssertionError(f"exact Artifact absent or ambiguous: {uid} {key}")
    return records[0]


def bounded_samples(series: pd.Series, limit: int = 12) -> list[str]:
    values = series.dropna().astype(str).drop_duplicates().head(limit)
    return values.tolist()


def series_summary(series: pd.Series) -> dict[str, Any]:
    return {
        "dtype": str(series.dtype),
        "non_null": int(series.notna().sum()),
        "unique_non_null": int(series.nunique(dropna=True)),
        "samples": bounded_samples(series),
    }


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    columns: dict[str, Any] = {}
    for column in frame.columns:
        columns[str(column)] = series_summary(frame[column])
    return {
        "rows": len(frame),
        "columns": columns,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(pd.Index(frame.index.astype(str))),
        "index_samples": pd.Index(frame.index.astype(str))[:12].tolist(),
    }


def normalized_sha256(series: pd.Series) -> str:
    normalized = series.astype("string").fillna("<NA>").reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(normalized, index=False).to_numpy()
    return hashlib.sha256(hashed.tobytes()).hexdigest()


def _frame_axis(group: h5py.Group) -> pd.Index:
    index_name = group.attrs["_index"]
    if isinstance(index_name, bytes):
        index_name = index_name.decode()
    values = read_elem(group[str(index_name)])
    return pd.Index(pd.Series(values, dtype="string").astype(str))


def inspect_source_h5ad(path: Path) -> dict[str, Any]:
    with h5py.File(path, "r") as handle:
        obs_group = handle["obs"]
        if not isinstance(obs_group, h5py.Group):
            raise AssertionError("source H5AD obs is not a dataframe group")
        column_order = obs_group.attrs["column-order"]
        columns = [
            item.decode() if isinstance(item, bytes) else str(item)
            for item in column_order
        ]
        summaries: dict[str, Any] = {}
        hashes: dict[str, str] = {}
        for column in columns:
            series = pd.Series(read_elem(obs_group[column]))
            summaries[column] = series_summary(series)
            hashes[column] = normalized_sha256(series)
        obs_index = _frame_axis(obs_group)

        var_group = handle["var"]
        if not isinstance(var_group, h5py.Group):
            raise AssertionError("source H5AD var is not a dataframe group")
        var_index = _frame_axis(var_group)
    return {
        "obs_summary": {
            "rows": len(obs_index),
            "columns": summaries,
            "index_unique": bool(obs_index.is_unique),
            "index_sha256": ordered_sha256(obs_index),
            "index_samples": obs_index[:12].tolist(),
        },
        "column_hashes": hashes,
        "obs_index": obs_index,
        "n_vars": len(var_index),
        "var_index_unique": bool(var_index.is_unique),
        "var_index_sha256": ordered_sha256(var_index),
        "var_index_samples": var_index[:12].tolist(),
    }


def inspect_member(ln: Any, member: dict[str, Any], root: Path) -> dict[str, Any]:
    prefix = member["prefix"]
    obs_key = f"{prefix}/obs.parquet"
    x_key = f"{prefix}/X.h5ad"
    var_key = f"{prefix}/var.parquet"
    source_path, source_receipt = download(member, root)

    source_result = inspect_source_h5ad(source_path)
    source_index = source_result["obs_index"]
    if len(source_index) != member["n_obs"]:
        raise AssertionError(f"source OBS denominator drift: {prefix}")
    source_summary = source_result["obs_summary"]
    source_column_hashes = source_result["column_hashes"]
    source_index_ordered_sha256 = ordered_sha256(source_index)
    source_index_multiset_sha256 = multiset_sha256(source_index)

    accepted_artifact = exact_artifact(ln, member["obs_uid"], obs_key)
    x_artifact = exact_artifact(ln, member["x_uid"], x_key)
    var_artifact = exact_artifact(ln, member["var_uid"], var_key)
    accepted = accepted_artifact.load()
    if len(accepted) != member["n_obs"]:
        raise AssertionError(f"accepted OBS denominator drift: {prefix}")
    accepted_index = pd.Index(accepted.index.astype(str))
    original = (
        pd.Index(accepted["original_obs_index"].astype(str))
        if "original_obs_index" in accepted
        else pd.Index([])
    )
    common = sorted(set(source_column_hashes).intersection(accepted.columns))
    common_equal = {
        str(column): source_column_hashes[str(column)]
        == normalized_sha256(accepted[column])
        for column in common
    }
    result = {
        "identity": dict(member),
        "source": source_receipt,
        "source_obs": source_summary,
        "accepted_obs": frame_summary(accepted),
        "axis_relations": {
            "source_index_equals_accepted_index": source_index_ordered_sha256
            == ordered_sha256(accepted_index),
            "source_index_equals_original_obs_index": source_index_ordered_sha256
            == ordered_sha256(original),
            "source_index_set_equals_accepted_index": source_index_multiset_sha256
            == multiset_sha256(accepted_index),
            "source_index_set_equals_original_obs_index": source_index_multiset_sha256
            == multiset_sha256(original),
        },
        "common_column_equalities": common_equal,
        "source_var": {
            "rows": source_result["n_vars"],
            "index_unique": source_result["var_index_unique"],
            "index_sha256": source_result["var_index_sha256"],
            "index_samples": source_result["var_index_samples"],
        },
        "accepted_artifacts": {
            "obs": {
                "uid": str(accepted_artifact.uid),
                "hash": str(accepted_artifact.hash),
            },
            "x": {"uid": str(x_artifact.uid), "hash": str(x_artifact.hash)},
            "var": {"uid": str(var_artifact.uid), "hash": str(var_artifact.hash)},
        },
    }
    del accepted
    return result


def upload_report(report: dict[str, Any]) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_name = f"{STAGING_PREFIX}/source-inspection-{timestamp}.json"
    client = storage.Client(project=BILLING_PROJECT)
    blob = client.bucket("scperturb", user_project=BILLING_PROJECT).blob(object_name)
    payload = canonical(report).encode() + b"\n"
    blob.upload_from_string(
        payload, content_type="application/json", if_generation_match=0
    )
    blob.reload()
    return {
        "uri": f"gs://scperturb/{object_name}",
        "generation": int(blob.generation),
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-cellarity-sources"
    root.mkdir(parents=True, exist_ok=True)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")

    results = []
    for index, member in enumerate(MEMBERS, start=1):
        heartbeat = {
            "product_execution": {
                "task_id": TASK_ID,
                "host": capacity.hostname,
                "pid": os.getpid(),
                "phase": "source_inspection",
                "epoch": index,
                "metric": "logical_families_inspected",
                "current": index - 1,
                "denominator": len(MEMBERS),
            }
        }
        print("CELLARITY_HEARTBEAT=" + canonical(heartbeat), flush=True)
        results.append(inspect_member(ln, member, root))

    report = {
        "format": REPORT_FORMAT,
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "members": results,
        "invariants": {
            "writes_to_lamin": 0,
            "logical_families": len(results),
            "observations": sum(item["identity"]["n_obs"] for item in results),
        },
    }
    receipt = upload_report(report)
    print("CELLARITY_SOURCE_INSPECTION=" + canonical(receipt), flush=True)


if __name__ == "__main__":
    main()
