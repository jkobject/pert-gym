#!/usr/bin/env python3
"""Independent generation-qualified verifier for frozen temporal-v4 row 95."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import socket
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import h5py
import numpy as np
import pandas as pd

RECORD_ID = "temporal_v4_095_organoiddb_odd001137_gse158999"
COMPONENT = "Mouse embryonic heart organoid/development scRNA-seq Odd001137"
LOGICAL_KEY = "pert-gym/logical/temporal/organoiddb_odd001137_gse158999"
ACCESSION = "GSE158999"
ROW = 95
EXPECTED_OBS = 30_496
EXPECTED_VARS = 23_961
EXPECTED_SAMPLES = 8
SUPERSEDES_REVISION = "temporal-v4-095-wave10-b1dee01a49f4c78c"
SOURCE_MANIFEST_SHA = "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
GRAPH_SHA = "59c18752f65257270b980353811da5bf554d5ac2b6c11c550a63849664ce9c98"
CATALOGUE_SHA = "4d31f341b60163ba1bcf6293746b9f8fe483cbccf6cd975367ed62a30467fdea"
CATALOGUE_PACKET_SHA = "f8aa67e0c21078aa19374e790f701e13fa28dc037521a236227640ebc3159b66"
CATALOGUE_SEMANTIC_SHA = "264fa793c892b237df165983e238bc8bca174fe7901c620d50f87012a7233484"
SOURCE_BASE = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE158nnn/GSE158999/suppl"
SOFT_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE158nnn/GSE158999/soft/GSE158999_family.soft.gz"
SOURCE_FILES = {
    "matrix": ("GSE158999_matrix.mtx.gz", 497_917_800, "16475994194e6e7e41be8c6deaef8426f02fda34ba80f12f7d25ec7c2a579d7e"),
    "barcodes": ("GSE158999_barcodes.tsv.gz", 157_988, "2d4197dec8573dfeda91e9827e64656fff12345379700701511dbcf2d7d2db10"),
    "features": ("GSE158999_features.tsv.gz", 93_683, "72819b605d74325c8973cae725c9c90df85e53c96eb25a519b4cc0da962f4445"),
    "metadata": ("GSE158999_metadata.tsv.gz", 9_420_602, "4a4d9046d21250fa1e4001f431fe02ae943ae613a40f9356f7d94c249e3765f3"),
    "soft": ("GSE158999_family.soft.gz", 2_946, "7b03f6390b7777ab5360a271657e5d0dcd7cb74c1869fcd332b017270bacb186"),
}
WAVE = {"id": "publication-wave-10-of-13", "number": 10, "assignment_count": 1, "singular": True, "outcome_task_id": "t_48ac1f74"}
BILLING_PROJECT = "jkobject-1549353370965"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024**2):
            digest.update(block)
    return digest.hexdigest()


def catalogue_semantic_sha(path: Path) -> str:
    """Hash TSV values after review-only end-of-line whitespace normalization."""
    text = path.read_text()
    normalized = "\n".join(line.rstrip(" \t\r") for line in text.splitlines()) + "\n"
    rows = list(csv.reader(io.StringIO(normalized, newline=""), delimiter="\t"))
    if not rows:
        raise RuntimeError("catalogue is empty")
    width = len(rows[0])
    canonical = []
    for line_number, row in enumerate(rows, start=1):
        if len(row) not in {width, width - 1}:
            raise RuntimeError(
                f"catalogue row {line_number} has {len(row)} fields; expected {width}"
            )
        canonical.append(row + [""] * (width - len(row)))
    payload = json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def download_generation(fs: Any, obj: dict[str, Any], path: Path) -> None:
    digest, size = hashlib.sha256(), 0
    with fs.open(f"{obj['key']}#{obj['generation']}", "rb") as source, path.open("wb") as target:
        while block := source.read(8 * 1024**2):
            target.write(block)
            digest.update(block)
            size += len(block)
    if digest.hexdigest() != obj["sha256"] or size != obj["size_bytes"]:
        raise RuntimeError(f"generation readback mismatch: {obj['key']}")


def independent_source_hash(url: str, path: Path, expected_size: int, expected_sha: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "pert-gym/row95-independent-verifier"})
    digest, size = hashlib.sha256(), 0
    with urllib.request.urlopen(request, timeout=120) as source, path.open("wb") as target:
        while block := source.read(8 * 1024**2):
            target.write(block)
            digest.update(block)
            size += len(block)
    if digest.hexdigest() != expected_sha or size != expected_size:
        raise RuntimeError("independent GEO source identity mismatch")
    return {"uri": url, "sha256": digest.hexdigest(), "size_bytes": size}


def h5_array_sha(dataset: h5py.Dataset) -> str:
    digest = hashlib.sha256()
    step = max(1, min(int(dataset.shape[0]), 1_048_576))
    for start in range(0, int(dataset.shape[0]), step):
        digest.update(np.ascontiguousarray(dataset[start : start + step]).tobytes(order="C"))
    return digest.hexdigest()


def matrix_inventory(path: Path) -> dict[str, Any]:
    with h5py.File(path, "r") as handle:
        group, data = handle["X"], handle["X/data"]
        raw_encoding = group.attrs.get("encoding-type")
        if isinstance(raw_encoding, bytes):
            raw_encoding = raw_encoding.decode("utf-8")
        encoding_type = str(raw_encoding)
        formats = {"csr_matrix": "csr", "csc_matrix": "csc"}
        if encoding_type not in formats:
            raise RuntimeError(f"unsupported physical sparse encoding: {encoding_type!r}")
        shape = [int(value) for value in group.attrs["shape"]]
        indptr_length = int(group["indptr"].shape[0])
        expected_indptr_length = shape[0] + 1 if encoding_type == "csr_matrix" else shape[1] + 1
        if indptr_length != expected_indptr_length:
            raise RuntimeError(
                "physical sparse indptr cardinality mismatch: "
                f"encoding={encoding_type} shape={shape} "
                f"observed={indptr_length} expected={expected_indptr_length}"
            )
        if int(group["indices"].shape[0]) != int(data.shape[0]):
            raise RuntimeError("physical sparse indices/data cardinality mismatch")
        return {"format": formats[encoding_type], "physical_encoding_type": encoding_type, "shape": shape, "nnz_stored": int(data.shape[0]), "indptr_length": indptr_length, "expected_indptr_length": expected_indptr_length, "data_dtype": str(data.dtype), "data_sha256": h5_array_sha(data), "indices_sha256": h5_array_sha(group["indices"]), "indptr_sha256": h5_array_sha(group["indptr"]), "finite_value_sum": int(np.asarray(data[:], dtype=np.uint64).sum(dtype=np.uint64)), "max_count": int(np.asarray(data[:]).max(initial=0))}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-object", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if socket.gethostname().split(".")[0] != "pert-gym-worker-eu":
        raise RuntimeError("verification may run only on pert-gym-worker-eu")
    catalogue_path = args.input / "temporal_pretraining_datasets_v4.tsv"
    controls = {"frozen_manifest": sha256_file(args.input / "downloadable_logical_publication_manifest_20260713.json"), "graph": sha256_file(args.input / "kanban_graph_compaction_t_36a3533e_manifest.json"), "catalogue": sha256_file(catalogue_path)}
    if controls != {"frozen_manifest": SOURCE_MANIFEST_SHA, "graph": GRAPH_SHA, "catalogue": CATALOGUE_PACKET_SHA}:
        raise RuntimeError(f"control drift: {controls}")
    if catalogue_semantic_sha(catalogue_path) != CATALOGUE_SEMANTIC_SHA:
        raise RuntimeError("catalogue semantic identity mismatch")
    manifest = json.loads(args.manifest.read_text())
    manifest_obj = json.loads(args.manifest_object.read_text())
    if manifest["record_id"] != RECORD_ID or manifest["component"] != COMPONENT or manifest["target_logical_key"] != LOGICAL_KEY or manifest["catalogue_row_ids"] != [ROW]:
        raise RuntimeError("component scope drift")
    if manifest["bounded_wave"] != WAVE:
        raise RuntimeError("wave identity drift")
    if manifest.get("control_inputs") != {
        "publication_manifest": {"sha256": SOURCE_MANIFEST_SHA},
        "graph": {"sha256": GRAPH_SHA},
        "catalogue": {"sha256": CATALOGUE_SHA},
    }:
        raise RuntimeError("frozen control identity drift")
    if manifest.get("supersedes_revision") != SUPERSEDES_REVISION or manifest["immutability"].get("superseded_revision_mutated") is not False:
        raise RuntimeError("immutable supersession lineage drift")
    graph = json.loads((args.input / "kanban_graph_compaction_t_36a3533e_manifest.json").read_text())
    assignments = [row for row in graph["component_assignments"] if row.get("record_id") == RECORD_ID]
    if assignments != [manifest["bounded_wave_assignment"]] or assignments[0].get("wave") != 10 or assignments[0].get("outcome_task_id") != WAVE["outcome_task_id"]:
        raise RuntimeError("component is not assigned exactly once")
    expected_counts = {"dataset_count": 1, "triplet_count": 1, "artifact_count": 3, "observation_count": EXPECTED_OBS, "variable_count": EXPECTED_VARS, "sample_count": EXPECTED_SAMPLES}
    if any(manifest.get(key) != value for key, value in expected_counts.items()):
        raise RuntimeError("manifest denominator drift")
    source = manifest["source_identity"]
    if source["accession"] != ACCESSION or source["source_payload_object_count"] != 5 or set(source["upstream_objects"]) != set(SOURCE_FILES) or not source["verified"]:
        raise RuntimeError("manifest source identity drift")
    missing = manifest["missingness"]
    if not missing["non_excluding"] or missing["excluded_observations"] != 0 or missing["excluded_records"] != 0 or missing["dependency_created"] or "donor_age" not in missing["absent_source_fields"]:
        raise RuntimeError("missingness accounting drift")
    ledger = manifest["ledger"]
    if ledger["record_id"] != RECORD_ID or ledger["catalogue_row_ids"] != [ROW] or ledger["target_logical_key"] != LOGICAL_KEY or ledger["accepted_delta_at_build"] != 0 or ledger["debits"] != {"dropped_observations": 0, "exclusions": 0, "product_credit": 0}:
        raise RuntimeError("ledger identity/debits drift")
    if ledger["input_accounting"]["expected_inputs"] != ledger["input_accounting"]["accounted_inputs"] or ledger["output_accounting"]["expected_outputs"] != ledger["output_accounting"]["accounted_outputs"] or ledger["observation_accounting"] != {"catalogue_source_n_obs_verbatim": 0, "resolved_source_n_obs": EXPECTED_OBS, "materialized_n_obs": EXPECTED_OBS, "excluded_n_obs": 0}:
        raise RuntimeError("ledger balance drift")
    fs = fsspec.filesystem("gcs", version_aware=True, project=BILLING_PROJECT, requester_pays=BILLING_PROJECT)
    with tempfile.TemporaryDirectory(prefix="verify-row95-") as raw_tmp:
        tmp = Path(raw_tmp)
        independent_source: dict[str, dict[str, Any]] = {}
        for role, (filename, expected_size, expected_sha) in SOURCE_FILES.items():
            url = SOFT_URL if role == "soft" else f"{SOURCE_BASE}/{filename}"
            independent_source[role] = independent_source_hash(url, tmp / filename, expected_size, expected_sha)
        paths: dict[str, Path] = {}
        for obj in manifest["actual_artifact_inventory"]:
            path = tmp / obj["filename"]
            download_generation(fs, obj, path)
            paths[obj["role"]] = path
        if set(paths) != {"obs", "X", "var"}:
            raise RuntimeError("triplet role partition drift")
        ledger_path = tmp / "ledger.json"
        download_generation(fs, manifest["ledger_object"], ledger_path)
        if json.loads(ledger_path.read_text()) != ledger:
            raise RuntimeError("ledger object/content drift")
        remote_manifest = tmp / "manifest.json"
        download_generation(fs, manifest_obj, remote_manifest)
        if remote_manifest.read_bytes() != args.manifest.read_bytes():
            raise RuntimeError("manifest object/content drift")
        if int(manifest_obj["generation"]) <= max(int(row["generation"]) for row in manifest["actual_artifact_inventory"] + [manifest["ledger_object"]]):
            raise RuntimeError("manifest was not written last")
        obs, var = pd.read_parquet(paths["obs"]), pd.read_parquet(paths["var"])
        adata = ad.read_h5ad(paths["X"], backed="r")
        if adata.shape != (EXPECTED_OBS, EXPECTED_VARS) or not adata.obs_names.equals(obs.index) or not adata.var_names.equals(var.index):
            raise RuntimeError("ordered triplet axes drift")
        adata.file.close()
        expected_time_counts = {4: 6898, 5: 7783, 6: 8313, 7: 7502}
        observed_time_counts = {int(key): int(value) for key, value in obs["timepoint"].value_counts().sort_index().items()}
        if len(obs) != EXPECTED_OBS or obs["sample_accession"].nunique() != EXPECTED_SAMPLES or set(obs["source_accession"].astype(str)) != {ACCESSION} or observed_time_counts != expected_time_counts:
            raise RuntimeError("obs identity/temporal denominator drift")
        required_qc = {"nCount_RNA", "nFeature_RNA", "percent.mito", "doublet_score"}
        if not required_qc.issubset(obs.columns) or obs["celltype"].nunique() != 32 or obs[list(required_qc)].isna().any().any():
            raise RuntimeError("retained metadata/quality fields drift")
        if len(var) != EXPECTED_VARS or var.index.has_duplicates or var["feature_namespace"].unique().tolist() != ["MGI gene symbol"]:
            raise RuntimeError("var identity drift")
        inventory = matrix_inventory(paths["X"])
        if inventory != manifest["dataset"]["readback"]["matrix"] or inventory["shape"] != [EXPECTED_OBS, EXPECTED_VARS]:
            raise RuntimeError("matrix content inventory drift")
        if inventory["format"] != "csc" or inventory["physical_encoding_type"] != "csc_matrix" or inventory["indptr_length"] != EXPECTED_VARS + 1:
            raise RuntimeError(f"unexpected physical sparse representation: {inventory}")
    result = {"schema_version": "pert-gym.independent-generation-readback/v1", "verdict": "PASS", "record_id": RECORD_ID, "target_logical_key": LOGICAL_KEY, "manifest_sha256": sha256_file(args.manifest), "manifest_generation_uri": manifest_obj["generation_uri"], "control_hashes": controls, "independent_source_identity": independent_source, "scope": {"catalogue_row_ids": [ROW], "datasets": 1, "triplets": 1, "payload_objects": 3, "samples": EXPECTED_SAMPLES}, "wave": WAVE, "counts": {"observations": EXPECTED_OBS, "variables": EXPECTED_VARS, "nnz_stored": inventory["nnz_stored"], "finite_value_sum": inventory["finite_value_sum"]}, "physical_sparse_encoding": {key: inventory[key] for key in ("format", "physical_encoding_type", "shape", "indptr_length", "expected_indptr_length")}, "timepoint_counts": expected_time_counts, "generation_qualified_payloads_read": 3, "generation_qualified_ledger_read": True, "generation_qualified_manifest_read": True, "ordered_axis_parity": True, "source_identity_verified": True, "explicit_missingness_preserved": True, "ledger_balanced": True, "logical_key_resolves_to_built_dataset": True, "product_credit": 0}
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
