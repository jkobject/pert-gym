#!/usr/bin/env python3
"""Build the frozen STT0000071 zebrafish-heart logical component on the EU VM.

The writer consumes generation-pinned staged CNGB objects, emits one sparse-Zarr
zip plus obs Parquet per section and one shared var Parquet, verifies every object
through a fresh GCS readback, and writes the immutable manifest last.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import zarr
from scipy import sparse

ROOT = Path.home() / "work" / "pert-gym"
sys.path.insert(0, str(ROOT))
from pert_gym.logical_sparse_zarr import shared_var_identity  # noqa: E402
from pert_gym.sparse_zarr_contract import load_compatible_surface  # noqa: E402
from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import (  # noqa: E402
    lamin_writer_lock,
    legacy_lamin_writer_lock_paths,
    require_heavy_vm,
    vm_global_lamin_writer_lock_path,
)

REQUIRED_FROZEN_SHA256 = "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
RECORD_ID = "stt0000071"
COMPONENT = "zebrafish heart regeneration"
CATALOGUE_ROW_ID = 150
LOGICAL_KEY = "pert-gym/logical/stt0000071"
INSTANCE = "laminlabs/pertdata"
BRANCH = "jkobject"
HOST = "pert-gym-worker-eu"
ZONE = "europe-west1-b"
BILLING_PROJECT = "jkobject-1549353370965"
GCS_OUTPUT_ROOT = "gs://scperturb/pert-gym/staging"
SOURCE_PREFIX = "gs://scperturb/pert-gym/staging/temporal_pretraining/stt0000071_cngb_non_tiff_20260630"
TIMEPOINT_MINUTES = {
    "uninjured_1": 0,
    "6_hpa": 360,
    "12_hpa": 720,
    "1_dpa": 1_440,
    "3_dpa": 4_320,
    "7_dpa": 10_080,
    "14_dpa": 20_160,
    "28_dpa": 40_320,
    "uninjured_2": 0,
}
EXPECTED_SAMPLE_TIMEPOINT_SECTIONS = {
    ("STSA0000734", "uninjured_1"): 3,
    ("STSA0000735", "6_hpa"): 3,
    ("STSA0000736", "12_hpa"): 3,
    ("STSA0000737", "1_dpa"): 3,
    ("STSA0000738", "3_dpa"): 3,
    ("STSA0000739", "7_dpa"): 3,
    ("STSA0000740", "14_dpa"): 3,
    ("STSA0000741", "28_dpa"): 3,
    ("STSA0000742", "uninjured_2"): 22,
}
WRITER_VERSION = "pert-gym.stt0000071.writer/v2"
VAR_SCHEMA_FINGERPRINT = "stt0000071-shared-var/v1"
CELL_BIN = re.compile(r"\.(?:50|70)\.gem\.gz$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
FAILED_PREDECESSORS = [
    {
        "revision": "stt0000071-20260716T162204Z-02688128",
        "revision_uri": (
            f"{GCS_OUTPUT_ROOT}/{LOGICAL_KEY}/revisions/"
            "stt0000071-20260716T162204Z-02688128"
        ),
        "status": "immutable_partial_no_manifest_no_credit_no_resume",
        "accepted_components_credit": 0,
        "manifest_present": False,
        "failure": "metadata_schema_drift_before_first_section_payload",
        "objects": [
            {
                "role": "shared_var",
                "uri": (
                    f"{GCS_OUTPUT_ROOT}/{LOGICAL_KEY}/revisions/"
                    "stt0000071-20260716T162204Z-02688128/shared-var.parquet"
                ),
                "generation": "1784219494302293",
                "size": 440_278,
                "sha256": "5137cff5ae04406969feb0b23b81d92e817ecbbef1ba9556ed8796325981a57a",
            }
        ],
        "policy": "preserve immutable for audit; never resume, overwrite, promote, or count",
    },
    {
        "revision": "stt0000071-20260716T163421Z-9b1a2db2",
        "revision_uri": (
            f"{GCS_OUTPUT_ROOT}/{LOGICAL_KEY}/revisions/"
            "stt0000071-20260716T163421Z-9b1a2db2"
        ),
        "status": "external_vm_stop_no_objects_no_manifest_no_credit_no_resume",
        "accepted_components_credit": 0,
        "manifest_present": False,
        "objects": [],
        "failure": {
            "kind": "external_vm_stop",
            "audit_timestamp": "2026-07-16T16:40:41.800551Z",
            "principal": "jkobject@gmail.com",
            "observed_phase": "source_identity_validation_before_materialization",
        },
        "policy": "never resume or count; every retry uses a fresh absent revision",
    },
]


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=check)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def sha256_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(json_bytes(value))
    temporary.replace(path)


def product_heartbeat(path: Path, **values: object) -> None:
    payload = {
        "format": "pert-gym.product-execution-heartbeat/v1",
        "host": socket.gethostname().split(".")[0],
        "pid": os.getpid(),
        "payload_heartbeat_at": time.time(),
        "metric": "accepted_components",
        "current": 4,
        "denominator": 153,
        **values,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(json_bytes(payload))
    temporary.replace(path)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_frozen_record(path: Path) -> dict[str, Any]:
    observed = sha256_file(path)
    if observed != REQUIRED_FROZEN_SHA256:
        raise RuntimeError(f"frozen publication manifest SHA-256 mismatch: {observed}")
    payload = load_json(path)
    rows = [row for row in payload["records"] if row.get("record_id") == RECORD_ID]
    if len(rows) != 1:
        raise RuntimeError("frozen manifest must contain exactly one STT0000071 record")
    row = rows[0]
    expected = {
        "classification": "executable",
        "component": COMPONENT,
        "target_logical_key": LOGICAL_KEY,
        "catalogue_row_ids": [CATALOGUE_ROW_ID],
        "completion_state": "staged_needs_converter",
    }
    mismatches = {key: (row.get(key), value) for key, value in expected.items() if row.get(key) != value}
    if mismatches:
        raise RuntimeError(f"frozen record mismatch: {mismatches}")
    return row


def classify_sections(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(rows) != 138:
        raise RuntimeError(
            f"staging manifest must contain exactly 138 rows, got {len(rows)}"
        )
    if len({row["relative_path"] for row in rows}) != 138:
        raise RuntimeError("staging manifest relative paths are not unique")
    sections: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not str(row.get("gcs_uri", "")).startswith(SOURCE_PREFIX + "/"):
            raise RuntimeError("source object escaped exact frozen staging prefix")
        generation = str(row.get("gcs_generation", ""))
        if not generation.isdigit() or int(row.get("gcs_size", 0)) <= 0:
            raise RuntimeError("source staging identity lacks generation or size")
        sections.setdefault(str(row["section_id"]), []).append(row)
    if len(sections) != 46:
        raise RuntimeError(f"expected 46 sections, got {len(sections)}")
    result: list[dict[str, Any]] = []
    for section_id in sorted(sections):
        group = sections[section_id]
        cell_bins = [row for row in group if CELL_BIN.search(str(row["filename"]))]
        metadata = [row for row in group if str(row["filename"]).endswith(".tsv.gz")]
        raw = [
            row
            for row in group
            if str(row["filename"]).endswith(".gem.gz")
            and not CELL_BIN.search(str(row["filename"]))
        ]
        if not (len(group) == 3 and len(cell_bins) == len(metadata) == len(raw) == 1):
            raise RuntimeError(
                f"section {section_id} does not have cell-bin GEM/TSV/raw GEM"
            )
        if (
            len({row["sample_id"] for row in group}) != 1
            or len({row["timepoint"] for row in group}) != 1
        ):
            raise RuntimeError(f"section {section_id} metadata is incoherent")
        result.append(
            {
                "section_id": section_id,
                "sample_id": group[0]["sample_id"],
                "timepoint": group[0]["timepoint"],
                "section_label": group[0]["section_label"],
                "cell_bin": cell_bins[0],
                "metadata": metadata[0],
                "raw_unbinned": raw[0],
            }
        )
    coverage = Counter((item["sample_id"], item["timepoint"]) for item in result)
    if coverage != Counter(EXPECTED_SAMPLE_TIMEPOINT_SECTIONS):
        raise RuntimeError(
            "staging manifest sample/timepoint coverage does not match the frozen contract: "
            f"{dict(sorted(coverage.items()))}"
        )
    return result


def describe_gcs(uri: str) -> dict[str, Any]:
    result = run(
        [
            "gcloud",
            "storage",
            "objects",
            "describe",
            uri,
            f"--billing-project={BILLING_PROJECT}",
            "--format=json",
        ]
    )
    value = json.loads(result.stdout)
    return {
        "uri": uri.split("#", 1)[0],
        "generation": str(value["generation"]),
        "size": int(value["size"]),
        "md5_base64": value.get("md5Hash"),
        "crc32c_base64": value.get("crc32c"),
        "updated": value.get("updateTime") or value.get("updated"),
    }


def probe_gcs(uri: str) -> dict[str, Any] | None:
    result = run(
        [
            "gcloud",
            "storage",
            "objects",
            "describe",
            uri,
            f"--billing-project={BILLING_PROJECT}",
            "--format=json",
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    value = json.loads(result.stdout)
    return {
        "uri": uri.split("#", 1)[0],
        "generation": str(value["generation"]),
        "size": int(value["size"]),
        "md5_base64": value.get("md5Hash"),
        "crc32c_base64": value.get("crc32c"),
        "updated": value.get("updateTime") or value.get("updated"),
    }


def generation_uri(row: dict[str, Any]) -> str:
    return f"{row['gcs_uri']}#{row['gcs_generation']}"


def copy_generation(row: dict[str, Any], destination: Path) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "gcloud",
            "storage",
            "cp",
            f"--billing-project={BILLING_PROJECT}",
            generation_uri(row),
            str(destination),
        ]
    )
    if destination.stat().st_size != int(row["gcs_size"]):
        raise RuntimeError(f"generation-pinned source size mismatch: {row['gcs_uri']}")
    return {"bytes": destination.stat().st_size, "sha256": sha256_file(destination)}


def coordinate_keys(frame: pd.DataFrame) -> list[tuple[float, float]]:
    return list(zip(frame["x"].round(9), frame["y"].round(9)))


def convert_section(
    gem_path: Path,
    metadata_path: Path,
    *,
    genes: list[str],
    section: dict[str, Any],
) -> tuple[pd.DataFrame, sparse.csr_matrix, dict[str, Any]]:
    gem = pd.read_csv(
        gem_path,
        sep="\t",
        dtype={"geneID": "string", "x": "float64", "y": "float64", "MIDCounts": "int64"},
    )
    source_metadata = pd.read_csv(metadata_path, sep="\t")
    if list(gem.columns) != ["geneID", "x", "y", "MIDCounts"]:
        raise RuntimeError("cell-bin GEM schema drift")
    late_metadata = [
        "x",
        "y",
        "nGenes",
        "nUMI",
        "sid",
        "3D_x",
        "3D_y",
        "3D_z",
        "annotation",
        "cellname",
        "cid",
    ]
    early_metadata = [
        "orig.ident",
        "nCount_Spatial",
        "nFeature_Spatial",
        "x",
        "y",
        "grid_x",
        "grid_y",
        "isolate",
        "cid",
        "time_points",
        "annotation",
    ]
    if list(source_metadata.columns) == late_metadata:
        metadata = source_metadata.copy()
        metadata_schema = "reconstructed_3d_cell_metadata/v1"
        metadata["spatial_3d_missingness_reason"] = pd.Series(
            pd.NA, index=metadata.index, dtype="string"
        )
    elif list(source_metadata.columns) == early_metadata:
        if not source_metadata.index.is_unique or source_metadata.index.isna().any():
            raise RuntimeError("early section metadata source index is not a cell identity")
        metadata = pd.DataFrame(
            {
                "x": source_metadata["grid_x"],
                "y": source_metadata["grid_y"],
                "nGenes": source_metadata["nFeature_Spatial"],
                "nUMI": source_metadata["nCount_Spatial"],
                "sid": source_metadata["isolate"],
                "3D_x": pd.Series(pd.NA, index=source_metadata.index, dtype="Float64"),
                "3D_y": pd.Series(pd.NA, index=source_metadata.index, dtype="Float64"),
                "3D_z": pd.Series(pd.NA, index=source_metadata.index, dtype="Float64"),
                "annotation": source_metadata["annotation"],
                "cellname": source_metadata.index.astype(str),
                "cid": source_metadata["cid"],
                "source_orig_ident": source_metadata["orig.ident"],
                "source_x": source_metadata["x"],
                "source_y": source_metadata["y"],
                "source_time_points": source_metadata["time_points"],
                "spatial_3d_missingness_reason": "source_not_reported",
            },
            index=source_metadata.index,
        )
        metadata_schema = "seurat_spatial_cell_metadata/v1"
    else:
        raise RuntimeError("section metadata schema drift")
    required_non_null = ["x", "y", "nGenes", "nUMI", "sid", "annotation", "cellname", "cid"]
    if metadata[required_non_null].isna().any().any() or not metadata["cellname"].is_unique:
        raise RuntimeError("section metadata identity or non-null contract failed")
    meta_keys = coordinate_keys(metadata)
    if len(meta_keys) != len(set(meta_keys)):
        raise RuntimeError("section metadata has duplicate rounded coordinates")
    row_by_coordinate = {key: index for index, key in enumerate(meta_keys)}
    gem_keys = coordinate_keys(gem)
    missing_coordinates = sorted(set(gem_keys) - set(meta_keys))
    if missing_coordinates or set(meta_keys) - set(gem_keys):
        raise RuntimeError("GEM/metadata coordinate parity failed")
    gene_to_column = {gene: index for index, gene in enumerate(genes)}
    row_indices = np.fromiter((row_by_coordinate[key] for key in gem_keys), dtype=np.int64)
    column_indices = np.fromiter((gene_to_column[str(gene)] for gene in gem["geneID"]), dtype=np.int64)
    values = gem["MIDCounts"].to_numpy(dtype=np.int64, copy=True)
    matrix = sparse.coo_matrix(
        (values, (row_indices, column_indices)), shape=(len(metadata), len(genes))
    ).tocsr()
    matrix.sum_duplicates()
    if int(matrix.sum()) != int(metadata["nUMI"].sum()):
        raise RuntimeError("section matrix sum does not equal metadata nUMI sum")
    obs = metadata.rename(
        columns={"cellname": "cell_id", "annotation": "cell_type", "nUMI": "n_counts", "nGenes": "n_genes"}
    ).copy()
    obs.index = obs["cell_id"].astype(str)
    obs.index.name = "obs_id"
    obs["dataset"] = RECORD_ID
    obs["sample"] = section["sample_id"]
    obs["section_id"] = section["section_id"]
    obs["timepoint_label"] = section["timepoint"]
    obs["timepoint"] = TIMEPOINT_MINUTES[section["timepoint"]]
    obs["trajectory_id"] = "zebrafish_heart_regeneration"
    obs["organism"] = "Danio rerio"
    obs["tissue_type"] = "heart"
    obs["assay"] = "Stereo-seq"
    obs["modality"] = "spatial_RNA"
    obs["perturbation"] = "heart injury" if not section["timepoint"].startswith("uninjured") else "none"
    obs["perturbation_type"] = "injury" if obs["perturbation"].iloc[0] != "none" else "none"
    obs["is_control"] = section["timepoint"].startswith("uninjured")
    obs["is_baseline"] = obs["is_control"]
    obs["age"] = pd.Series(pd.NA, index=obs.index, dtype="string")
    obs["age_missingness_reason"] = "source_not_reported"
    obs["sex"] = pd.Series(pd.NA, index=obs.index, dtype="string")
    obs["sex_missingness_reason"] = "source_not_reported"
    obs["disease"] = "regenerating heart injury model"
    obs["technology"] = "Stereo-seq"
    obs["is_bulk"] = False
    obs["is_pseudobulk"] = False
    obs["is_low_quality"] = pd.Series(pd.NA, index=obs.index, dtype="boolean")
    obs["quality_missingness_reason"] = "source_has_n_counts_and_n_genes_but_no_accepted_quality_threshold"
    stats = {
        "metadata_schema": metadata_schema,
        "n_obs": int(matrix.shape[0]),
        "n_vars": int(matrix.shape[1]),
        "section_present_genes": int(gem["geneID"].nunique()),
        "nnz": int(matrix.nnz),
        "sum": int(matrix.sum()),
        "metadata_n_counts_sum": int(metadata["nUMI"].sum()),
    }
    return obs, matrix, stats


def write_sparse_zarr_zip(path: Path, matrix: sparse.csr_matrix) -> None:
    if path.exists():
        raise FileExistsError(path)
    store = zarr.storage.ZipStore(str(path), mode="w")
    try:
        group = zarr.group(store=store)
        group.attrs.update(
            {"format": "csr_matrix", "shape": list(matrix.shape), "nnz": int(matrix.nnz), "dtype": str(matrix.dtype)}
        )
        for name, values in (("data", matrix.data), ("indices", matrix.indices), ("indptr", matrix.indptr)):
            group.create_dataset(name, data=values, chunks=(max(1, min(len(values), 65_536)),))
    finally:
        store.close()


def read_sparse_zarr_zip(path: Path) -> sparse.csr_matrix:
    store = zarr.storage.ZipStore(str(path), mode="r")
    try:
        group = zarr.open_group(store=store, mode="r")
        return sparse.csr_matrix(
            (np.asarray(group["data"]), np.asarray(group["indices"]), np.asarray(group["indptr"])),
            shape=tuple(group.attrs["shape"]),
        )
    finally:
        store.close()


def write_or_validate_sparse_zarr_zip(path: Path, matrix: sparse.csr_matrix) -> None:
    if not path.exists():
        write_sparse_zarr_zip(path, matrix)
        return
    loaded = read_sparse_zarr_zip(path)
    if (
        loaded.shape != matrix.shape
        or loaded.dtype != matrix.dtype
        or loaded.nnz != matrix.nnz
        or (loaded != matrix).nnz != 0
    ):
        raise RuntimeError(f"local sparse-Zarr resume mismatch: {path}")


def write_or_validate_parquet(path: Path, frame: pd.DataFrame) -> None:
    if not path.exists():
        frame.to_parquet(path)
        return
    if not pd.read_parquet(path).equals(frame):
        raise RuntimeError(f"local Parquet resume mismatch: {path}")


def upload_or_reconcile(path: Path, uri: str) -> dict[str, Any]:
    expected_size = path.stat().st_size
    expected_sha256 = sha256_file(path)
    existing = probe_gcs(uri)
    if existing is not None:
        existing["sha256"] = expected_sha256
        with tempfile.TemporaryDirectory(
            prefix="stt0000071-upload-reconcile-"
        ) as temporary:
            destination = Path(temporary) / path.name
            try:
                readback_object(existing, destination)
            except RuntimeError as error:
                raise RuntimeError(
                    f"existing immutable object mismatch: {uri}"
                ) from error
            if (
                destination.stat().st_size != expected_size
                or sha256_file(destination) != expected_sha256
            ):
                raise RuntimeError(f"existing immutable object mismatch: {uri}")
        return existing
    result = run(
        [
            "gcloud",
            "storage",
            "cp",
            "--if-generation-match=0",
            f"--billing-project={BILLING_PROJECT}",
            str(path),
            uri,
        ],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"immutable upload failed for {uri}: {result.stderr.strip()}"
        )
    identity = describe_gcs(uri)
    identity["sha256"] = expected_sha256
    if identity["size"] != expected_size:
        raise RuntimeError(f"uploaded object size mismatch: {uri}")
    with tempfile.TemporaryDirectory(prefix="stt0000071-upload-readback-") as temporary:
        readback_object(identity, Path(temporary) / path.name)
    return identity


def readback_object(identity: dict[str, Any], destination: Path) -> None:
    run(
        [
            "gcloud",
            "storage",
            "cp",
            f"--billing-project={BILLING_PROJECT}",
            f"{identity['uri']}#{identity['generation']}",
            str(destination),
        ]
    )
    if destination.stat().st_size != identity["size"] or sha256_file(destination) != identity["sha256"]:
        raise RuntimeError(f"remote readback mismatch: {identity['uri']}")


def load_or_create_publication_journal(
    path: Path, identity: dict[str, Any]
) -> dict[str, Any]:
    if path.exists():
        journal = load_json(path)
        if journal.get("identity") != identity:
            raise RuntimeError(
                "publication journal identity mismatch; refusing drifted resume"
            )
        if journal.get(
            "format"
        ) != "pert-gym.stt0000071.publication-journal/v1" or not isinstance(
            journal.get("completed_stages"), dict
        ):
            raise RuntimeError("publication journal is malformed")
        return journal
    journal = {
        "format": "pert-gym.stt0000071.publication-journal/v1",
        "identity": dict(identity),
        "publication_started_at": time.time(),
        "completed_stages": {},
    }
    atomic_json(path, journal)
    return journal


def publish_output(
    path: Path,
    uri: str,
    *,
    stage: str,
    journal_path: Path,
    journal_identity: dict[str, Any],
) -> dict[str, Any]:
    journal = load_or_create_publication_journal(journal_path, journal_identity)
    remote = upload_or_reconcile(path, uri)
    completed = journal["completed_stages"]
    recorded = completed.get(stage)
    if recorded is not None and recorded != remote:
        raise RuntimeError(f"publication journal stage identity mismatch: {stage}")
    if recorded is None:
        completed[stage] = remote
        atomic_json(journal_path, journal)
    return remote


def sha256_array(values: np.ndarray) -> str:
    return hashlib.sha256(np.ascontiguousarray(values).tobytes(order="C")).hexdigest()


def canonical_surface_manifest(
    *,
    revision: str,
    section_records: list[dict[str, Any]],
    shared_var_key: str,
    var_identity: Any,
) -> dict[str, Any]:
    chunks = []
    start = 0
    n_vars: int | None = None
    total_nnz = 0
    for record in section_records:
        stats = record["stats"]
        if n_vars is None:
            n_vars = int(stats["n_vars"])
        elif int(stats["n_vars"]) != n_vars:
            raise RuntimeError("section var dimensions are inconsistent")
        end = start + int(stats["n_obs"])
        total_nnz += int(stats["nnz"])
        source = record["source_cell_metadata"]
        chunks.append(
            {
                "key": record["X"]["key"],
                "start": start,
                "end": end,
                "nnz": int(stats["nnz"]),
                "shape": [end - start, int(stats["n_vars"])],
                "dtype": record["dtype"],
                "checksums": dict(record["checksums"]),
                "obs": {
                    "key": record["obs"]["key"],
                    "provenance": {
                        "source_uri": source["uri"],
                        "source_checksum": f"sha256-file-bytes/v1:{source['sha256']}",
                        "source_row_start": 0,
                        "source_row_end": end - start,
                        "ingestion_run_id": revision,
                        "writer_version": WRITER_VERSION,
                    },
                },
            }
        )
        start = end
    manifest = {
        "format": "pert-gym.logical-sparse-zarr",
        "version": 1,
        "revision": revision,
        "shape": [start, n_vars or 0],
        "nnz": total_nnz,
        "sparse_format": "csr",
        "chunks": chunks,
        "shared_var": {
            "key": shared_var_key,
            "index_sha256": var_identity.index_sha256,
            "frame_sha256": var_identity.frame_sha256,
            "schema_fingerprint": var_identity.schema_fingerprint,
        },
    }
    load_compatible_surface(manifest)
    return manifest


def duplicate_probe(ln: Any) -> dict[str, Any]:
    queries = {
        "logical_key_prefix": list(ln.Artifact.filter(key__startswith=LOGICAL_KEY).all()),
        "record_id_description": list(ln.Artifact.filter(description__icontains=RECORD_ID).all()),
        "component_description": list(ln.Artifact.filter(description__icontains=COMPONENT).all()),
    }
    rows: dict[str, dict[str, Any]] = {}
    for records in queries.values():
        for record in records:
            rows[str(record.uid)] = {
                "uid": str(record.uid),
                "key": str(record.key or ""),
                "description": str(record.description or ""),
            }
    return {"query_counts": {key: len(value) for key, value in queries.items()}, "candidates": list(rows.values())}


def exact_context() -> Any:
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != INSTANCE or ln.setup.settings.branch.name != BRANCH:
        raise RuntimeError("refusing execution outside laminlabs/pertdata branch jkobject")
    return ln


def validate_ledger(path: Path) -> dict[str, Any]:
    ledger = load_json(path)
    if ledger.get("protocol") != "pert-gym-accepted-components-loopback/v1" or ledger.get("metric") != "accepted_components":
        raise RuntimeError("accepted-ledger evidence protocol mismatch")
    owner = ledger.get("latest_owner", {})
    if owner.get("current") != 4 or owner.get("denominator") != 153 or owner.get("mismatch") != 0:
        raise RuntimeError("accepted-ledger precondition is not exact 4/153")
    manifest = owner.get("manifest", {})
    if not str(manifest.get("generation", "")).isdigit() or SHA256.fullmatch(str(manifest.get("sha256", ""))) is None:
        raise RuntimeError("accepted-ledger owner manifest identity is incomplete")
    return ledger


def dry_run(args: argparse.Namespace, sections: list[dict[str, Any]]) -> int:
    section = next((item for item in sections if item["section_id"] == args.section_id), None)
    if section is None:
        raise RuntimeError(f"unknown dry-run section {args.section_id}")
    ln = exact_context()
    duplicates = duplicate_probe(ln)
    if duplicates["candidates"]:
        raise RuntimeError(f"duplicate preflight found candidates: {duplicates}")
    with tempfile.TemporaryDirectory(prefix="stt0000071-dry-run-") as temporary:
        root = Path(temporary)
        gem_path = root / section["cell_bin"]["filename"]
        meta_path = root / section["metadata"]["filename"]
        gem_identity = copy_generation(section["cell_bin"], gem_path)
        metadata_identity = copy_generation(section["metadata"], meta_path)
        genes = sorted(pd.read_csv(gem_path, sep="\t", usecols=["geneID"])["geneID"].astype(str).unique())
        obs, matrix, stats = convert_section(gem_path, meta_path, genes=genes, section=section)
        zarr_path = root / "X.zarr.zip"
        obs_path = root / "obs.parquet"
        write_sparse_zarr_zip(zarr_path, matrix)
        obs.to_parquet(obs_path)
        loaded = read_sparse_zarr_zip(zarr_path)
        observed = {
            "status": "PASS",
            "mode": "one-section-converter-dry-run",
            "record_id": RECORD_ID,
            "component": COMPONENT,
            "catalogue_row_id": CATALOGUE_ROW_ID,
            "logical_key": LOGICAL_KEY,
            "branch": BRANCH,
            "section": {key: section[key] for key in ("section_id", "sample_id", "timepoint", "section_label")},
            "source": {"cell_bin": gem_identity, "metadata": metadata_identity},
            "duplicate_probe": duplicates,
            "stats": stats,
            "obs_columns": list(obs.columns),
            "readback": {
                "shape": list(loaded.shape),
                "nnz": int(loaded.nnz),
                "sum": int(loaded.sum()),
                "matrix_equal": bool((matrix != loaded).nnz == 0),
                "obs_parquet_equal": bool(pd.read_parquet(obs_path).equals(obs)),
            },
        }
        args.evidence_out.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_out.write_bytes(json_bytes(observed))
        print(json.dumps(observed, indent=2, sort_keys=True))
    return 0


def execute(
    args: argparse.Namespace,
    frozen: dict[str, Any],
    staging: dict[str, Any],
    sections: list[dict[str, Any]],
) -> int:
    require_heavy_vm()
    ledger = validate_ledger(args.ledger_evidence)
    ln = exact_context()
    duplicates = duplicate_probe(ln)
    if duplicates["candidates"]:
        raise RuntimeError(f"duplicate preflight found candidates: {duplicates}")
    revision = args.revision
    if re.fullmatch(r"stt0000071-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", revision) is None:
        raise RuntimeError("revision is not a fresh immutable STT0000071 revision")
    candidate_uri = f"{GCS_OUTPUT_ROOT}/{LOGICAL_KEY}/revisions/{revision}"
    lock_metadata = {
        "pid": os.getpid(),
        "run_id": f"t_2ddd92ed-{revision}",
        "host": HOST,
        "project": BILLING_PROJECT,
        "zone": ZONE,
        "branch": BRANCH,
        "started_at": time.time(),
    }
    output_root = args.work_dir / revision
    journal_path = output_root / "publication-journal.json"
    journal_identity = {
        "revision": revision,
        "candidate_uri": candidate_uri,
        "frozen_record_sha256": sha256_json(frozen),
        "staging_manifest_sha256": sha256_file(args.staging_manifest),
        "ledger_owner_sha256": sha256_json(ledger["latest_owner"]),
        "writer_sha256": sha256_file(Path(__file__)),
    }
    if output_root.exists() and not journal_path.exists():
        raise RuntimeError("existing local revision lacks its publication journal")
    output_root.mkdir(parents=True, exist_ok=True)
    journal = load_or_create_publication_journal(journal_path, journal_identity)
    if not isinstance(journal.get("publication_started_at"), (int, float)):
        raise RuntimeError("publication journal is missing its stable start time")

    def publish(path: Path, uri: str, *, stage: str) -> dict[str, Any]:
        return publish_output(
            path,
            uri,
            stage=stage,
            journal_path=journal_path,
            journal_identity=journal_identity,
        )

    source_root = output_root / "source"
    built_root = output_root / "built"
    readback_root = output_root / "readback"
    source_root.mkdir(exist_ok=True)
    built_root.mkdir(exist_ok=True)
    readback_root.mkdir(exist_ok=True)
    global_lock = vm_global_lamin_writer_lock_path()
    heartbeat_path = args.evidence_out.with_name("product-execution.json")
    with ExitStack() as locks:
        locks.enter_context(lamin_writer_lock(global_lock, lock_metadata))
        for path in legacy_lamin_writer_lock_paths():
            locks.enter_context(
                lamin_writer_lock(path, lock_metadata, check_live_metadata=False)
            )
        ln.track()
        product_heartbeat(
            heartbeat_path, phase="preflight", revision=revision, completed=0, total=138
        )
        source_identities: list[dict[str, Any]] = []
        for index, row in enumerate(staging["rows"], 1):
            identity = describe_gcs(generation_uri(row))
            if identity["generation"] != str(row["gcs_generation"]) or identity[
                "size"
            ] != int(row["gcs_size"]):
                raise RuntimeError(f"source physical identity drift: {row['gcs_uri']}")
            source_identities.append(
                {
                    **identity,
                    "relative_path": row["relative_path"],
                    "sample_id": row["sample_id"],
                    "timepoint": row["timepoint"],
                    "section_id": row["section_id"],
                    "role": "cell_bin_expression"
                    if CELL_BIN.search(row["filename"])
                    else (
                        "cell_metadata"
                        if row["filename"].endswith(".tsv.gz")
                        else "raw_unbinned_expression_provenance"
                    ),
                }
            )
            if index % 20 == 0:
                print(f"source identity {index}/138", flush=True)
                product_heartbeat(
                    heartbeat_path,
                    phase="source-identity",
                    revision=revision,
                    completed=index,
                    total=138,
                )
        local_sources: dict[str, dict[str, Any]] = {}
        all_genes: set[str] = set()
        for index, section in enumerate(sections, 1):
            section_root = source_root / section["section_id"]
            for role in ("cell_bin", "metadata"):
                row = section[role]
                destination = section_root / row["filename"]
                local_sources[row["relative_path"]] = copy_generation(row, destination)
            gem_path = section_root / section["cell_bin"]["filename"]
            genes = pd.read_csv(gem_path, sep="\t", usecols=["geneID"])[
                "geneID"
            ].astype(str)
            all_genes.update(genes.unique())
            print(f"source materialized {index}/46 genes={len(all_genes)}", flush=True)
            product_heartbeat(
                heartbeat_path,
                phase="source-materialization",
                revision=revision,
                completed=index,
                total=46,
            )
        genes = sorted(all_genes)
        var = pd.DataFrame(
            {
                "gene_symbol": genes,
                "organism": "Danio rerio",
                "feature_namespace": "source gene symbol",
                "ensembl_gene_id": pd.Series([pd.NA] * len(genes), dtype="string"),
                "ensembl_missingness_reason": "source_uses_gene_symbols_without_ensembl_ids",
            },
            index=pd.Index(genes, name="var_id"),
        )
        var_path = built_root / "shared-var.parquet"
        write_or_validate_parquet(var_path, var)
        output_identities: list[dict[str, Any]] = []
        var_contract_identity = shared_var_identity(
            var, schema_fingerprint=VAR_SCHEMA_FINGERPRINT
        )
        shared_var_key = f"vars/{var_contract_identity.key}/var.parquet"
        var_output_identity = publish(
            var_path,
            f"{GCS_OUTPUT_ROOT}/{shared_var_key}",
            stage="shared-var",
        )
        var_output_identity.update({"role": "shared_var", "key": shared_var_key})
        output_identities.append(var_output_identity)
        section_records: list[dict[str, Any]] = []
        total_obs = 0
        total_nnz = 0
        total_counts = 0
        for index, section in enumerate(sections, 1):
            source_section = source_root / section["section_id"]
            output_section = (
                built_root / "sections" / f"{index - 1:04d}-{section['section_id']}"
            )
            output_section.mkdir(parents=True, exist_ok=True)
            obs, matrix, stats = convert_section(
                source_section / section["cell_bin"]["filename"],
                source_section / section["metadata"]["filename"],
                genes=genes,
                section=section,
            )
            obs_path = output_section / "obs.parquet"
            matrix_path = output_section / "X.zarr.zip"
            write_or_validate_parquet(obs_path, obs)
            write_or_validate_sparse_zarr_zip(matrix_path, matrix)
            section_key = f"sections/{index - 1:04d}-{section['section_id']}"
            section_prefix = f"{candidate_uri}/{section_key}"
            obs_identity = publish(
                obs_path,
                f"{section_prefix}/obs.parquet",
                stage=f"section-{index - 1:04d}-obs",
            )
            matrix_identity = publish(
                matrix_path,
                f"{section_prefix}/X.zarr.zip",
                stage=f"section-{index - 1:04d}-X",
            )
            obs_identity.update(
                {"role": "canonical_obs", "key": f"{section_key}/obs.parquet"}
            )
            matrix_identity.update(
                {
                    "role": "canonical_X_sparse_zarr_zip",
                    "key": f"{section_key}/X.zarr.zip",
                }
            )
            output_identities.extend([obs_identity, matrix_identity])
            total_obs += stats["n_obs"]
            total_nnz += stats["nnz"]
            total_counts += stats["sum"]
            section_records.append(
                {
                    "chunk_index": index - 1,
                    "section_id": section["section_id"],
                    "sample_id": section["sample_id"],
                    "timepoint": section["timepoint"],
                    "source_cell_bin_relative_path": section["cell_bin"][
                        "relative_path"
                    ],
                    "source_metadata_relative_path": section["metadata"][
                        "relative_path"
                    ],
                    "source_raw_unbinned_relative_path": section["raw_unbinned"][
                        "relative_path"
                    ],
                    "stats": stats,
                    "dtype": str(matrix.dtype),
                    "checksums": {
                        "data_sha256": sha256_array(matrix.data),
                        "indices_sha256": sha256_array(matrix.indices),
                        "indptr_sha256": sha256_array(matrix.indptr),
                    },
                    "source_cell_bin": {
                        "uri": generation_uri(section["cell_bin"]),
                        "sha256": local_sources[section["cell_bin"]["relative_path"]][
                            "sha256"
                        ],
                    },
                    "source_cell_metadata": {
                        "uri": generation_uri(section["metadata"]),
                        "sha256": local_sources[
                            section["metadata"]["relative_path"]
                        ]["sha256"],
                    },
                    "obs": obs_identity,
                    "X": matrix_identity,
                    "var": var_output_identity,
                }
            )
            print(
                f"section built {index}/46 n_obs={stats['n_obs']} nnz={stats['nnz']}",
                flush=True,
            )
            product_heartbeat(
                heartbeat_path,
                phase="writing",
                revision=revision,
                completed=index,
                total=46,
                n_obs=total_obs,
                nnz=total_nnz,
            )
        for identity in source_identities:
            local = local_sources.get(identity["relative_path"])
            if local:
                identity["local_sha256"] = local["sha256"]
                identity["local_bytes"] = local["bytes"]
        source_inventory = {
            "format": "pert-gym.stt0000071.source-inventory/v1",
            "record_id": RECORD_ID,
            "staging_manifest_sha256": sha256_file(args.staging_manifest),
            "objects": source_identities,
            "counts": {
                "all_non_tiff": 138,
                "cell_bin_expression": 46,
                "cell_metadata": 46,
                "raw_unbinned_expression_provenance": 46,
                "tiff_explicitly_excluded": 46,
                "total_staged_bytes": sum(item["size"] for item in source_identities),
            },
        }
        source_inventory_path = built_root / "source-inventory.json"
        source_inventory_path.write_bytes(json_bytes(source_inventory))
        source_inventory_identity = publish(
            source_inventory_path,
            f"{candidate_uri}/source-inventory.json",
            stage="source-inventory",
        )
        source_inventory_identity["role"] = "source_inventory"
        output_identities.append(source_inventory_identity)
        missingness = {
            "format": "pert-gym.metadata-quality-missingness/v1",
            "record_id": RECORD_ID,
            "dataset_age": {
                "value": None,
                "reason": "source_not_reported",
                "excluded": False,
            },
            "sex": {"value": None, "reason": "source_not_reported", "excluded": False},
            "ethnicity": {
                "value": None,
                "reason": "not_applicable_non_human",
                "excluded": False,
            },
            "donor_id": {
                "value": None,
                "reason": "source_sections_not_donor-resolved",
                "excluded": False,
            },
            "quality": {
                "available": ["n_counts", "n_genes"],
                "missing": [
                    "pct_mito",
                    "pct_ribo",
                    "accepted_is_low_quality_threshold",
                ],
                "reason": "source_not_reported_or_no_reviewed_threshold",
                "excluded": False,
            },
            "var": {
                "gene_symbol": "complete_source_axis",
                "ensembl_gene_id": None,
                "reason": "source_uses_gene_symbols_without_ensembl_ids",
                "excluded": False,
            },
            "source_metadata_schemas": {
                "seurat_spatial_cell_metadata/v1": 24,
                "reconstructed_3d_cell_metadata/v1": 22,
            },
            "spatial_3d_coordinates": {
                "available_sections": 22,
                "missing_sections": 24,
                "reason": "source_not_reported_for_seurat_spatial_sections",
                "excluded": False,
            },
            "images": {
                "count": 46,
                "status": "explicitly_excluded_from_this_expression_component",
                "reason": "frozen workload requires no TIFF canonical ingestion",
            },
        }
        missingness_path = built_root / "metadata-quality-missingness.json"
        missingness_path.write_bytes(json_bytes(missingness))
        missingness_identity = publish(
            missingness_path,
            f"{candidate_uri}/metadata-quality-missingness.json",
            stage="metadata-quality-missingness",
        )
        missingness_identity["role"] = "metadata_quality_missingness"
        output_identities.append(missingness_identity)
        readback_sections: list[dict[str, Any]] = []
        readback_obs = 0
        readback_nnz = 0
        readback_counts = 0
        for section in section_records:
            target = (
                readback_root / f"{section['chunk_index']:04d}-{section['section_id']}"
            )
            target.mkdir(parents=True, exist_ok=True)
            obs_path = target / "obs.parquet"
            matrix_path = target / "X.zarr.zip"
            readback_object(section["obs"], obs_path)
            readback_object(section["X"], matrix_path)
            obs = pd.read_parquet(obs_path)
            matrix = read_sparse_zarr_zip(matrix_path)
            expected = section["stats"]
            if (
                list(matrix.shape) != [expected["n_obs"], expected["n_vars"]]
                or len(obs) != expected["n_obs"]
            ):
                raise RuntimeError("remote section readback shape mismatch")
            if (
                int(matrix.nnz) != expected["nnz"]
                or int(matrix.sum()) != expected["sum"]
            ):
                raise RuntimeError("remote section readback sparse parity mismatch")
            if int(obs["n_counts"].sum()) != expected["sum"]:
                raise RuntimeError("remote obs/X count parity mismatch")
            readback_obs += len(obs)
            readback_nnz += int(matrix.nnz)
            readback_counts += int(matrix.sum())
            readback_sections.append(
                {
                    "chunk_index": section["chunk_index"],
                    "section_id": section["section_id"],
                    "shape": list(matrix.shape),
                    "nnz": int(matrix.nnz),
                    "sum": int(matrix.sum()),
                    "obs_index_sha256": hashlib.sha256(
                        ("\n".join(map(str, obs.index)) + "\n").encode()
                    ).hexdigest(),
                }
            )
            product_heartbeat(
                heartbeat_path,
                phase="readback",
                revision=revision,
                completed=len(readback_sections),
                total=46,
                n_obs=readback_obs,
                nnz=readback_nnz,
            )
        var_readback_path = readback_root / "shared-var.parquet"
        readback_object(var_output_identity, var_readback_path)
        var_readback = pd.read_parquet(var_readback_path)
        if not var_readback.equals(var):
            raise RuntimeError("remote shared-var readback mismatch")
        if (readback_obs, readback_nnz, readback_counts) != (
            total_obs,
            total_nnz,
            total_counts,
        ):
            raise RuntimeError("component-level remote readback parity mismatch")
        manifest = {
            **canonical_surface_manifest(
                revision=revision,
                section_records=section_records,
                shared_var_key=shared_var_key,
                var_identity=var_contract_identity,
            ),
            "manifest_last": True,
            "record_id": RECORD_ID,
            "component": COMPONENT,
            "catalogue_row_id": CATALOGUE_ROW_ID,
            "logical_key": LOGICAL_KEY,
            "revision": revision,
            "candidate_uri": candidate_uri,
            "branch": BRANCH,
            "instance": INSTANCE,
            "provenance": {
                "producer_task_id": "t_2ddd92ed",
                "host": HOST,
                "zone": ZONE,
                "billing_project": BILLING_PROJECT,
                "frozen_publication_manifest_sha256": REQUIRED_FROZEN_SHA256,
                "frozen_record_sha256": sha256_json(frozen),
                "staging_manifest_sha256": sha256_file(args.staging_manifest),
                "writer_sha256": sha256_file(Path(__file__)),
            },
            "source": {
                "identity": frozen["source_integrity_identity"],
                "uri": frozen["source_uri"],
                "object_identity": frozen["source_object_identity"],
                "inventory": source_inventory_identity,
                "counts": source_inventory["counts"],
            },
            "dimensions": {
                "n_obs": total_obs,
                "n_vars": len(genes),
                "nnz": total_nnz,
                "sum_counts": total_counts,
                "sections": len(sections),
                "samples": len({section["sample_id"] for section in sections}),
                "timepoints": len({section["timepoint"] for section in sections}),
            },
            "shared_var_output": {
                "identity": var_output_identity,
                "ordered_var_sha256": hashlib.sha256(
                    ("\n".join(genes) + "\n").encode()
                ).hexdigest(),
                "namespace": "source gene symbol",
                "n_vars": len(genes),
            },
            "sections": section_records,
            "physical_outputs": output_identities,
            "duplicate_preflight": duplicates,
            "metadata_quality_missingness": {
                "identity": missingness_identity,
                "summary": missingness,
            },
            "readback": {
                "status": "PASS",
                "mode": "generation-pinned-remote-GCS-all-sections",
                "n_obs": readback_obs,
                "n_vars": len(var_readback),
                "nnz": readback_nnz,
                "sum_counts": readback_counts,
                "sections": readback_sections,
                "mismatch": 0,
            },
            "accepted_ledger": {
                "precondition": ledger["latest_owner"],
                "producer_credit": 0,
                "proposed_delta_if_independently_accepted": {
                    "before": 4,
                    "after": 5,
                    "denominator": 153,
                    "unit": "components",
                    "mismatch": 0,
                },
                "self_accepted": False,
            },
            "failed_predecessors": FAILED_PREDECESSORS,
            "publication_journal": {
                "format": "pert-gym.stt0000071.publication-journal/v1",
                "identity": journal_identity,
            },
            "forbidden_actions_observed": {
                "promotion": False,
                "collection_mutation": False,
                "cleanup": False,
                "deletion": False,
                "lamin_main": False,
            },
        }
        load_compatible_surface(manifest)
        manifest_path = built_root / "manifest.json"
        manifest_path.write_bytes(json_bytes(manifest))
        manifest_identity = publish(
            manifest_path,
            f"{candidate_uri}/manifest.json",
            stage="manifest",
        )
        manifest_readback_path = readback_root / "manifest.json"
        readback_object(manifest_identity, manifest_readback_path)
        loaded_manifest = load_json(manifest_readback_path)
        if loaded_manifest != manifest:
            raise RuntimeError("manifest remote readback mismatch")
        final = {
            "status": "PASS",
            "manifest": manifest_identity,
            "candidate_uri": candidate_uri,
            "dimensions": manifest["dimensions"],
            "source_counts": source_inventory["counts"],
            "accepted_ledger": manifest["accepted_ledger"],
            "duplicate_preflight": duplicates,
            "manifest_last": True,
            "readback_mismatch": 0,
        }
        args.evidence_out.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_out.write_bytes(json_bytes(final))
        product_heartbeat(
            heartbeat_path,
            phase="completed",
            revision=revision,
            completed=46,
            total=46,
            n_obs=total_obs,
            nnz=total_nnz,
            manifest=manifest_identity,
        )
        print(json.dumps(final, indent=2, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--frozen-manifest", type=Path, required=True)
    parser.add_argument("--staging-manifest", type=Path, required=True)
    parser.add_argument("--ledger-evidence", type=Path)
    parser.add_argument("--evidence-out", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, default=Path("/tmp/stt0000071-builds"))
    parser.add_argument("--section-id", default="STTS0001197")
    parser.add_argument("--revision")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    frozen = validate_frozen_record(args.frozen_manifest)
    staging = load_json(args.staging_manifest)
    sections = classify_sections(staging["rows"])
    if args.dry_run:
        return dry_run(args, sections)
    if args.ledger_evidence is None or args.revision is None:
        raise RuntimeError("--execute requires --ledger-evidence and --revision")
    return execute(args, frozen, staging, sections)


if __name__ == "__main__":
    raise SystemExit(main())
