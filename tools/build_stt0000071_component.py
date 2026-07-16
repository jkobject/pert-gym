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
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import zarr
from scipy import sparse

ROOT = Path.home() / "work" / "pert-gym"
sys.path.insert(0, str(ROOT))
from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import (  # noqa: E402
    lamin_writer_lock,
    legacy_lamin_writer_lock_paths,
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
CELL_BIN = re.compile(r"\.(?:50|70)\.gem\.gz$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


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
        raise RuntimeError(f"staging manifest must contain exactly 138 rows, got {len(rows)}")
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
            raise RuntimeError(f"section {section_id} does not have cell-bin GEM/TSV/raw GEM")
        if len({row["sample_id"] for row in group}) != 1 or len({row["timepoint"] for row in group}) != 1:
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
    return list(zip(frame["x"].round(10), frame["y"].round(10)))


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
    metadata = pd.read_csv(metadata_path, sep="\t")
    if list(gem.columns) != ["geneID", "x", "y", "MIDCounts"]:
        raise RuntimeError("cell-bin GEM schema drift")
    required_metadata = [
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
    if list(metadata.columns) != required_metadata:
        raise RuntimeError("section metadata schema drift")
    if metadata[required_metadata].isna().any().any() or not metadata["cellname"].is_unique:
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


def upload_immutable(path: Path, uri: str) -> dict[str, Any]:
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
        raise RuntimeError(f"immutable upload failed for {uri}: {result.stderr.strip()}")
    identity = describe_gcs(uri)
    identity["sha256"] = sha256_file(path)
    if identity["size"] != path.stat().st_size:
        raise RuntimeError(f"uploaded object size mismatch: {uri}")
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


def execute(args: argparse.Namespace, frozen: dict[str, Any], staging: dict[str, Any], sections: list[dict[str, Any]]) -> int:
    if socket.gethostname().split(".")[0] != HOST:
        raise RuntimeError(f"production execution is restricted to {HOST}")
    ledger = validate_ledger(args.ledger_evidence)
    ln = exact_context()
    duplicates = duplicate_probe(ln)
    if duplicates["candidates"]:
        raise RuntimeError(f"duplicate preflight found candidates: {duplicates}")
    revision = args.revision
    if re.fullmatch(r"stt0000071-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", revision) is None:
        raise RuntimeError("revision is not a fresh immutable STT0000071 revision")
    candidate_uri = f"{GCS_OUTPUT_ROOT}/{LOGICAL_KEY}/revisions/{revision}"
    existence = run(
        ["gcloud", "storage", "ls", f"--billing-project={BILLING_PROJECT}", f"{candidate_uri}/**"],
        check=False,
    )
    if existence.stdout.strip():
        raise RuntimeError("fresh immutable candidate prefix already contains objects")
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
    if output_root.exists():
        raise RuntimeError("local immutable revision directory already exists")
    output_root.mkdir(parents=True)
    source_root = output_root / "source"
    built_root = output_root / "built"
    readback_root = output_root / "readback"
    source_root.mkdir()
    built_root.mkdir()
    readback_root.mkdir()
    global_lock = vm_global_lamin_writer_lock_path()
    with ExitStack() as locks:
        lease_acquired = time.time()
        locks.enter_context(lamin_writer_lock(global_lock, lock_metadata))
        for path in legacy_lamin_writer_lock_paths():
            locks.enter_context(lamin_writer_lock(path, lock_metadata, check_live_metadata=False))
        ln.track()
        source_identities: list[dict[str, Any]] = []
        for index, row in enumerate(staging["rows"], 1):
            identity = describe_gcs(generation_uri(row))
            if identity["generation"] != str(row["gcs_generation"]) or identity["size"] != int(row["gcs_size"]):
                raise RuntimeError(f"source physical identity drift: {row['gcs_uri']}")
            source_identities.append(
                {
                    **identity,
                    "relative_path": row["relative_path"],
                    "sample_id": row["sample_id"],
                    "timepoint": row["timepoint"],
                    "section_id": row["section_id"],
                    "role": "cell_bin_expression" if CELL_BIN.search(row["filename"]) else ("cell_metadata" if row["filename"].endswith(".tsv.gz") else "raw_unbinned_expression_provenance"),
                }
            )
            if index % 20 == 0:
                print(f"source identity {index}/138", flush=True)
        local_sources: dict[str, dict[str, Any]] = {}
        all_genes: set[str] = set()
        for index, section in enumerate(sections, 1):
            section_root = source_root / section["section_id"]
            for role in ("cell_bin", "metadata"):
                row = section[role]
                destination = section_root / row["filename"]
                local_sources[row["relative_path"]] = copy_generation(row, destination)
            gem_path = section_root / section["cell_bin"]["filename"]
            genes = pd.read_csv(gem_path, sep="\t", usecols=["geneID"])["geneID"].astype(str)
            all_genes.update(genes.unique())
            print(f"source materialized {index}/46 genes={len(all_genes)}", flush=True)
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
        var.to_parquet(var_path)
        output_identities: list[dict[str, Any]] = []
        var_identity = upload_immutable(var_path, f"{candidate_uri}/shared-var.parquet")
        var_identity["role"] = "shared_var"
        output_identities.append(var_identity)
        section_records: list[dict[str, Any]] = []
        total_obs = 0
        total_nnz = 0
        total_counts = 0
        for index, section in enumerate(sections, 1):
            source_section = source_root / section["section_id"]
            output_section = built_root / "sections" / f"{index - 1:04d}-{section['section_id']}"
            output_section.mkdir(parents=True)
            obs, matrix, stats = convert_section(
                source_section / section["cell_bin"]["filename"],
                source_section / section["metadata"]["filename"],
                genes=genes,
                section=section,
            )
            obs_path = output_section / "obs.parquet"
            matrix_path = output_section / "X.zarr.zip"
            obs.to_parquet(obs_path)
            write_sparse_zarr_zip(matrix_path, matrix)
            section_prefix = f"{candidate_uri}/sections/{index - 1:04d}-{section['section_id']}"
            obs_identity = upload_immutable(obs_path, f"{section_prefix}/obs.parquet")
            matrix_identity = upload_immutable(matrix_path, f"{section_prefix}/X.zarr.zip")
            obs_identity["role"] = "canonical_obs"
            matrix_identity["role"] = "canonical_X_sparse_zarr_zip"
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
                    "source_cell_bin_relative_path": section["cell_bin"]["relative_path"],
                    "source_metadata_relative_path": section["metadata"]["relative_path"],
                    "source_raw_unbinned_relative_path": section["raw_unbinned"]["relative_path"],
                    "stats": stats,
                    "obs": obs_identity,
                    "X": matrix_identity,
                    "var": var_identity,
                }
            )
            print(f"section built {index}/46 n_obs={stats['n_obs']} nnz={stats['nnz']}", flush=True)
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
        source_inventory_identity = upload_immutable(source_inventory_path, f"{candidate_uri}/source-inventory.json")
        source_inventory_identity["role"] = "source_inventory"
        output_identities.append(source_inventory_identity)
        missingness = {
            "format": "pert-gym.metadata-quality-missingness/v1",
            "record_id": RECORD_ID,
            "dataset_age": {"value": None, "reason": "source_not_reported", "excluded": False},
            "sex": {"value": None, "reason": "source_not_reported", "excluded": False},
            "ethnicity": {"value": None, "reason": "not_applicable_non_human", "excluded": False},
            "donor_id": {"value": None, "reason": "source_sections_not_donor-resolved", "excluded": False},
            "quality": {
                "available": ["n_counts", "n_genes"],
                "missing": ["pct_mito", "pct_ribo", "accepted_is_low_quality_threshold"],
                "reason": "source_not_reported_or_no_reviewed_threshold",
                "excluded": False,
            },
            "var": {
                "gene_symbol": "complete_source_axis",
                "ensembl_gene_id": None,
                "reason": "source_uses_gene_symbols_without_ensembl_ids",
                "excluded": False,
            },
            "images": {"count": 46, "status": "explicitly_excluded_from_this_expression_component", "reason": "frozen workload requires no TIFF canonical ingestion"},
        }
        missingness_path = built_root / "metadata-quality-missingness.json"
        missingness_path.write_bytes(json_bytes(missingness))
        missingness_identity = upload_immutable(missingness_path, f"{candidate_uri}/metadata-quality-missingness.json")
        missingness_identity["role"] = "metadata_quality_missingness"
        output_identities.append(missingness_identity)
        readback_sections: list[dict[str, Any]] = []
        readback_obs = 0
        readback_nnz = 0
        readback_counts = 0
        for section in section_records:
            target = readback_root / f"{section['chunk_index']:04d}-{section['section_id']}"
            target.mkdir(parents=True)
            obs_path = target / "obs.parquet"
            matrix_path = target / "X.zarr.zip"
            readback_object(section["obs"], obs_path)
            readback_object(section["X"], matrix_path)
            obs = pd.read_parquet(obs_path)
            matrix = read_sparse_zarr_zip(matrix_path)
            expected = section["stats"]
            if list(matrix.shape) != [expected["n_obs"], expected["n_vars"]] or len(obs) != expected["n_obs"]:
                raise RuntimeError("remote section readback shape mismatch")
            if int(matrix.nnz) != expected["nnz"] or int(matrix.sum()) != expected["sum"]:
                raise RuntimeError("remote section readback sparse parity mismatch")
            if int(obs["n_counts"].sum()) != expected["sum"]:
                raise RuntimeError("remote obs/X count parity mismatch")
            readback_obs += len(obs)
            readback_nnz += int(matrix.nnz)
            readback_counts += int(matrix.sum())
            readback_sections.append({"chunk_index": section["chunk_index"], "section_id": section["section_id"], "shape": list(matrix.shape), "nnz": int(matrix.nnz), "sum": int(matrix.sum()), "obs_index_sha256": hashlib.sha256(("\n".join(map(str, obs.index)) + "\n").encode()).hexdigest()})
        var_readback_path = readback_root / "shared-var.parquet"
        readback_object(var_identity, var_readback_path)
        var_readback = pd.read_parquet(var_readback_path)
        if not var_readback.equals(var):
            raise RuntimeError("remote shared-var readback mismatch")
        if (readback_obs, readback_nnz, readback_counts) != (total_obs, total_nnz, total_counts):
            raise RuntimeError("component-level remote readback parity mismatch")
        manifest = {
            "format": "pert-gym.frozen-logical-component-manifest/v1",
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
                "frozen_publication_manifest_path": str(args.frozen_manifest),
                "frozen_publication_manifest_sha256": REQUIRED_FROZEN_SHA256,
                "frozen_record_sha256": sha256_json(frozen),
                "staging_manifest_path": str(args.staging_manifest),
                "staging_manifest_sha256": sha256_file(args.staging_manifest),
                "writer_path": str(Path(__file__)),
                "writer_sha256": sha256_file(Path(__file__)),
                "lease_acquired_at": lease_acquired,
            },
            "source": {
                "identity": frozen["source_integrity_identity"],
                "uri": frozen["source_uri"],
                "object_identity": frozen["source_object_identity"],
                "inventory": source_inventory_identity,
                "counts": source_inventory["counts"],
            },
            "dimensions": {"n_obs": total_obs, "n_vars": len(genes), "nnz": total_nnz, "sum_counts": total_counts, "sections": 46, "samples": 9, "timepoints": 9},
            "shared_var": {"identity": var_identity, "ordered_var_sha256": hashlib.sha256(("\n".join(genes) + "\n").encode()).hexdigest(), "namespace": "source gene symbol", "n_vars": len(genes)},
            "sections": section_records,
            "physical_outputs": output_identities,
            "duplicate_preflight": duplicates,
            "metadata_quality_missingness": {"identity": missingness_identity, "summary": missingness},
            "readback": {"status": "PASS", "mode": "generation-pinned-remote-GCS-all-sections", "n_obs": readback_obs, "n_vars": len(var_readback), "nnz": readback_nnz, "sum_counts": readback_counts, "sections": readback_sections, "mismatch": 0},
            "accepted_ledger": {
                "precondition": ledger["latest_owner"],
                "producer_credit": 0,
                "proposed_delta_if_independently_accepted": {"before": 4, "after": 5, "denominator": 153, "unit": "components", "mismatch": 0},
                "self_accepted": False,
            },
            "forbidden_actions_observed": {"promotion": False, "collection_mutation": False, "cleanup": False, "deletion": False, "lamin_main": False},
        }
        manifest_path = built_root / "manifest.json"
        manifest_path.write_bytes(json_bytes(manifest))
        manifest_identity = upload_immutable(manifest_path, f"{candidate_uri}/manifest.json")
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
