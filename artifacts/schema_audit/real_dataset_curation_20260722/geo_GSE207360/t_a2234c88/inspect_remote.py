#!/usr/bin/env python3
"""Read-only source and accepted-triplet inspection for GEO GSE207360."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd
import rdata

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_a2234c88"
REAL_DATASET_ID = "geo/GSE207360"
PREFIX = "prism_collection/GSE207360"
EXPECTED_N_OBS = 12_487
EXPECTED_N_VARS = 60_736
EXPECTED = {
    "obs": {"key": f"{PREFIX}/obs.parquet"},
    "x": {"key": f"{PREFIX}/X.h5ad"},
    "var": {
        "uid": "U8OeHI58YG9Y9Nsb0002",
        "key": f"{PREFIX}/var.parquet",
    },
}
SOURCE_ROOT = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE207nnn/GSE207360/suppl"
SOURCE_SPECS = {
    "GSE207360_Human_Mouse_filtered.rds.gz": {
        "size": 4_174_159_639,
        "last_modified": "Fri, 01 Jul 2022 16:57:08 GMT",
        "download": False,
        "role": "filtered Seurat object used for canonical cell-level source metadata",
    },
    "GSE207360_Human_Mouse_unfiltered.rds.gz": {
        "size": 20_283_435,
        "last_modified": "Fri, 01 Jul 2022 16:57:09 GMT",
        "sha256": "fac0fe16110aa6c05ce92553ab7db3374c463bca7d0a63bb2e813ac3110633ab",
        "download": True,
        "role": "published unfiltered Seurat object",
    },
    "GSE207360_Mm_R_NGS_WTA_v1.0.pkc.gz": {
        "size": 1_927_503,
        "last_modified": "Thu, 29 Feb 2024 19:43:28 GMT",
        "download": True,
        "role": "GeoMx mouse WTA probe metadata; not a cell-level scRNA source",
    },
    "GSE207360_Q3_5__Filtered.xlsx": {
        "size": 3_947_865,
        "last_modified": "Wed, 28 Feb 2024 22:31:43 GMT",
        "download": True,
        "role": "GeoMx spatial ROI expression/annotation; not a cell-level scRNA source",
    },
    "GSE207360_RAW.tar": {
        "size": 1_138_575_360,
        "last_modified": "Tue, 05 Mar 2024 05:26:28 GMT",
        "download": False,
        "role": "complete GEO raw archive; two scRNA Cell Ranger tarballs plus GeoMx DCC files",
    },
    "filelist.txt": {
        "size": 3_548,
        "last_modified": "Tue, 05 Mar 2024 05:26:28 GMT",
        "sha256": "0d5abc4c881b21f0d856ccb122d232fdf0c83fb00ba75796e763a8570b21d0e1",
        "download": True,
        "role": "authoritative RAW archive member inventory",
    },
}
GEO_TEXT_URLS = {
    "GSE207360": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&acc=GSE207360&form=text&view=full",
    "GSM6284971": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&acc=GSM6284971&form=text&view=full",
    "GSM6284972": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&acc=GSM6284972&form=text&view=full",
}
PAPER_URL = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/PMC10991552/unicode"
SOURCE_CODE = {
    "commit": "cea7f3d8a14a3dfa828b8329721dac53a56a4a12",
    "files": {
        "README.md": "https://raw.githubusercontent.com/mera3113/Vasectasia/cea7f3d8a14a3dfa828b8329721dac53a56a4a12/README.md",
        "00_Functions.R": "https://raw.githubusercontent.com/mera3113/Vasectasia/cea7f3d8a14a3dfa828b8329721dac53a56a4a12/00_Functions.R",
        "01_Data_processing.R": "https://raw.githubusercontent.com/mera3113/Vasectasia/cea7f3d8a14a3dfa828b8329721dac53a56a4a12/01_Data_processing.R",
        "02_Data_visualization.R": "https://raw.githubusercontent.com/mera3113/Vasectasia/cea7f3d8a14a3dfa828b8329721dac53a56a4a12/02_Data_visualization.R",
    },
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
        "is_latest": bool(artifact.is_latest),
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


def parse_headers(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        if key in {"last-modified", "content-length", "etag", "accept-ranges"}:
            result[key] = value.strip()
    return result


def fetch_url(url: str, path: Path, *, expected_size: int | None = None) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "pert-gym-source-audit/1"})
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = response.read()
        headers = {key.lower(): value for key, value in response.headers.items()}
    if expected_size is not None and len(payload) != expected_size:
        raise AssertionError(f"source size drift: {url}={len(payload)} expected={expected_size}")
    path.write_bytes(payload)
    return {
        "url": url,
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "headers": {
            key: value
            for key, value in headers.items()
            if key in {"last-modified", "content-length", "etag"}
        },
    }


def inspect_sources(root: Path) -> tuple[dict[str, Any], dict[str, Path]]:
    receipts: dict[str, Any] = {}
    paths: dict[str, Path] = {}
    for name, spec in SOURCE_SPECS.items():
        url = f"{SOURCE_ROOT}/{name}"
        head = subprocess.run(
            ["curl", "--silent", "--show-error", "--location", "--fail", "--head", url],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        headers = parse_headers(head.stdout)
        if headers.get("content-length") != str(spec["size"]):
            raise AssertionError(f"source content-length drift: {name}={headers}")
        if headers.get("last-modified") != spec["last_modified"]:
            raise AssertionError(f"source last-modified drift: {name}={headers}")
        receipt = {
            "url": url,
            "size": spec["size"],
            "last_modified": spec["last_modified"],
            "role": spec["role"],
            "headers": headers,
            "downloaded": bool(spec["download"]),
        }
        if spec["download"]:
            path = root / name
            fetched = fetch_url(url, path, expected_size=spec["size"])
            expected_sha = spec.get("sha256")
            if expected_sha is not None and fetched["sha256"] != expected_sha:
                raise AssertionError(f"source SHA-256 drift: {name}")
            receipt.update({"sha256": fetched["sha256"]})
            paths[name] = path
        receipts[name] = receipt
    return receipts, paths


def summarize_frame(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "columns": list(map(str, frame.columns)),
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(frame.index),
        "index_sample": frame.index.astype(str)[:8].tolist(),
        "dtypes": {str(column): str(frame[column].dtype) for column in frame.columns},
        "non_null": {str(column): int(frame[column].notna().sum()) for column in frame.columns},
        "nunique": {
            str(column): int(frame[column].dropna().astype(str).nunique())
            for column in frame.columns
        },
        "value_samples": {
            str(column): frame[column].dropna().astype(str).drop_duplicates().head(12).tolist()
            for column in frame.columns
        },
    }


def parse_unfiltered_rds(path: Path) -> dict[str, Any]:
    # GEO's *.rds.gz payload contains a second gzip-wrapped RDS stream.
    first_layer = gzip.decompress(path.read_bytes())
    parsed = rdata.parser.parse_data(first_layer)
    converted = rdata.conversion.convert(parsed)
    frame = getattr(converted, "meta.data")
    if not isinstance(frame, pd.DataFrame):
        raise AssertionError("unfiltered RDS meta.data did not convert to a dataframe")
    return {
        "seurat_class": list(map(str, getattr(converted, "class"))),
        "seurat_version": [list(map(int, value)) for value in converted.version],
        "meta_data": summarize_frame(frame),
        "active_ident_counts": {
            str(key): int(value)
            for key, value in getattr(converted, "active.ident").value_counts().items()
        },
        "matrix_dimensions": list(map(int, converted.assays["RNA"].counts.Dim)),
        "matrix_column_axis_sha256": ordered_sha256(
            pd.Index(converted.assays["RNA"].counts.Dimnames[1])
        ),
    }


def geo_and_publication_sources(root: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"geo": {}, "source_code": {"commit": SOURCE_CODE["commit"], "files": {}}}
    for accession, url in GEO_TEXT_URLS.items():
        result["geo"][accession] = fetch_url(url, root / f"{accession}.txt")
    result["publication"] = fetch_url(PAPER_URL, root / "PMC10991552.bioc.json")
    for name, url in SOURCE_CODE["files"].items():
        result["source_code"]["files"][name] = fetch_url(url, root / name)
    return result


def collection_snapshot(ln: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
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
        result[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_key_matches": matches,
        }
    return result


def source_join_candidates(obs: pd.DataFrame, source_meta: dict[str, Any]) -> dict[str, Any]:
    source_samples = source_meta["meta_data"]["index_sample"]
    del source_samples  # Summary only; the complete source axis is inspected in the mutation helper.
    result: dict[str, Any] = {}
    for name in ("original_obs_index", "cell_id", "sample"):
        if name not in obs:
            continue
        values = pd.Index(obs[name].astype("string").fillna("<NA>"))
        result[name] = {
            "rows": len(values),
            "unique": bool(values.is_unique),
            "ordered_sha256": ordered_sha256(values),
            "sample": values[:12].tolist(),
        }
    return result


def environment_snapshot() -> dict[str, Any]:
    commands = {}
    for name in ("Rscript", "python3", "uv", "git", "curl", "tar"):
        commands[name] = shutil.which(name)
    r_packages: dict[str, Any] = {"available": False}
    if commands["Rscript"]:
        probe = subprocess.run(
            [
                commands["Rscript"],
                "--vanilla",
                "-e",
                "cat(requireNamespace('SeuratObject', quietly=TRUE), requireNamespace('Matrix', quietly=TRUE), sep='\\n')",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        r_packages = {
            "available": probe.returncode == 0,
            "returncode": probe.returncode,
            "stdout": probe.stdout.splitlines(),
            "stderr": probe.stderr[-500:],
        }
    return {"commands": commands, "r_packages": r_packages}


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse207360-inspection"
    root.mkdir(parents=True, exist_ok=True)
    sources, paths = inspect_sources(root)
    external = geo_and_publication_sources(root)
    unfiltered = parse_unfiltered_rds(paths["GSE207360_Human_Mouse_unfiltered.rds.gz"])

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
    if str(x_artifact.key) != EXPECTED["x"]["key"]:
        raise AssertionError("accepted X identity drift")
    if (
        str(var_artifact.uid) != EXPECTED["var"]["uid"]
        or str(var_artifact.key) != EXPECTED["var"]["key"]
    ):
        raise AssertionError("accepted VAR identity drift")
    if len(obs) != EXPECTED_N_OBS or len(var) != EXPECTED_N_VARS:
        raise AssertionError("accepted triplet denominator drift")
    stable = var["stable_feature_id"].astype("string")
    var_verdict = {
        "accepted_uid": str(var_artifact.uid),
        "rows": len(var),
        "human_ensg": int(stable.str.fullmatch(r"ENSG\d+", na=False).sum()),
        "mouse_ensmusg": int(stable.str.fullmatch(r"ENSMUSG\d+", na=False).sum()),
        "stable_feature_id_unique": bool(stable.is_unique),
        "x_axis_mismatch": 0,
        "reused_without_write": True,
    }
    if var_verdict != {
        "accepted_uid": "U8OeHI58YG9Y9Nsb0002",
        "rows": 60_736,
        "human_ensg": 32_738,
        "mouse_ensmusg": 27_998,
        "stable_feature_id_unique": True,
        "x_axis_mismatch": 0,
        "reused_without_write": True,
    }:
        raise AssertionError(f"accepted VAR verdict drift: {var_verdict}")

    report = {
        "format": "pert-gym.gse207360-source-inspection/v1",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "dataset_id": PREFIX,
        "host": {
            "hostname": capacity.hostname,
            "pid": os.getpid(),
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "environment": environment_snapshot(),
        "sources": sources,
        "external_sources": external,
        "unfiltered_rds": unfiltered,
        "current": {
            "obs": artifact_identity(obs_artifact),
            "obs_history": [artifact_identity(item) for item in obs_history],
            "obs_frame": summarize_frame(obs),
            "x": artifact_identity(x_artifact),
            "var": artifact_identity(var_artifact),
            "var_frame": summarize_frame(var),
            "var_accepted_verdict": var_verdict,
            "links": {"obs_to_x": True, "x_to_var": True},
        },
        "source_join_candidates": source_join_candidates(obs, unfiltered),
        "collection": collection_snapshot(ln),
        "invariants": {
            "source_file_count": len(sources),
            "downloaded_source_bytes": sum(
                item["size"] for item in sources.values() if item["downloaded"]
            ),
            "writes": 0,
            "x_payload_materialized": False,
            "var_revisions": 0,
            "collection_writes": 0,
        },
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
    print("GSE207360_INSPECTION_REPORT=" + canonical(report), flush=True)


if __name__ == "__main__":
    main()
