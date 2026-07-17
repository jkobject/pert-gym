#!/usr/bin/env python3
"""Build the frozen PerturBase GSE107185 extend_61 component on the EU worker."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
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

TASK_ID = "t_fe451c4d"
RECORD_ID = "temporal_v4_116_perturbase_gse107185"
COMPONENT = "Mapping Cellular Reprogramming via Pooled Overexpression Screens with Paired Fitness and Single Cell RNA-Sequencing Readout"
LOGICAL_KEY = "pert-gym/logical/temporal/perturbase_gse107185"
CATALOGUE_ROW = 116
ACCESSION = "GSE107185"
BIOPROJECT = "PRJNA419230"
DATA_INDEX = "extend_61"
FROZEN_SHA256 = "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
GRAPH_SHA256 = "59c18752f65257270b980353811da5bf554d5ac2b6c11c550a63849664ce9c98"
SOURCE_URL = "http://www.perturbase.cn/static/extend_61/extend_61.filter.tar.gz"
SOURCE_SIZE = 65772471
SOURCE_SHA256 = "e5d2fa6f7a3c3faced2649e53a5226a41e4b93b6278b375e5f09346739e0bbfa"
SOURCE_ETAG = '"65b51ee2-3eb9bb7"'
SOURCE_LAST_MODIFIED = "Sat, 27 Jan 2024 15:18:58 GMT"
SOURCE_MEMBER = "mixscape_hvg_filter.h5ad"
SOURCE_MEMBER_SIZE = 215672608
SOURCE_MEMBER_SHA256 = "e8e19bf30b6b028d9bb257907f9bb4a0747478c6b5af24405df2a8da15e18e75"
EXPECTED_SHAPE = (8428, 2000)
EXPECTED_PERTURBATIONS = 61
EXPECTED_HOST = "pert-gym-worker-eu"
INSTANCE = "laminlabs/pertdata"
BRANCH = "jkobject"
ZONE = "europe-west1-b"
BILLING_PROJECT = "jkobject-1549353370965"
GCS_ROOT = "gs://scperturb/pert-gym/staging"
WAVE = {"id": "publication-wave-11-of-13", "number": 11, "assignment_count": 1, "singular": True, "outcome_task_id": "t_f196b29f"}
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=check)


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False, default=str) + "\n").encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_array(value: np.ndarray) -> str:
    value = np.ascontiguousarray(value)
    digest = hashlib.sha256()
    digest.update(str(value.dtype).encode())
    digest.update(json.dumps(list(value.shape)).encode())
    digest.update(value.tobytes())
    return digest.hexdigest()


def index_sha256(index: pd.Index) -> str:
    return hashlib.sha256(("\n".join(map(str, index)) + "\n").encode()).hexdigest()


def frame_semantic_sha256(frame: pd.DataFrame) -> str:
    hashed = pd.util.hash_pandas_object(frame, index=True, categorize=True).to_numpy(dtype=np.uint64)
    digest = hashlib.sha256()
    digest.update(json.dumps(list(map(str, frame.columns))).encode())
    digest.update(json.dumps([str(dtype) for dtype in frame.dtypes]).encode())
    digest.update(hashed.tobytes())
    return digest.hexdigest()


def frame_signature(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "columns": list(map(str, frame.columns)),
        "dtypes": {str(column): str(frame[column].dtype) for column in frame},
        "null_counts": {str(column): int(frame[column].isna().sum()) for column in frame},
        "index_name": str(frame.index.name),
        "index_sha256": index_sha256(frame.index),
        "semantic_sha256": frame_semantic_sha256(frame),
    }


def validate_controls() -> tuple[dict[str, Any], dict[str, Any]]:
    frozen_path = ROOT / "artifacts/schema_audit/downloadable_logical_publication_manifest_20260713.json"
    graph_path = ROOT / "artifacts/orchestration/kanban_graph_compaction_t_36a3533e_manifest.json"
    if sha256_file(frozen_path) != FROZEN_SHA256:
        raise RuntimeError("frozen publication manifest SHA-256 mismatch")
    if sha256_file(graph_path) != GRAPH_SHA256:
        raise RuntimeError("bounded-wave graph SHA-256 mismatch")
    frozen = json.loads(frozen_path.read_text())
    records = [item for item in frozen["records"] if item.get("record_id") == RECORD_ID]
    if len(records) != 1:
        raise RuntimeError("target publication record is not unique")
    record = records[0]
    expected = {
        "classification": "executable",
        "downloadable": "yes",
        "component": COMPONENT,
        "catalogue_row_ids": [CATALOGUE_ROW],
        "target_logical_key": LOGICAL_KEY,
        "source_object_identity": f"{ACCESSION};{BIOPROJECT}",
        "source_n_obs": EXPECTED_SHAPE[0],
    }
    mismatches = {key: [record.get(key), value] for key, value in expected.items() if record.get(key) != value}
    if mismatches:
        raise RuntimeError(f"frozen publication identity drift: {mismatches}")
    graph = json.loads(graph_path.read_text())
    assignments = [item for item in graph["component_assignments"] if item.get("record_id") == RECORD_ID]
    if len(assignments) != 1 or assignments[0].get("wave") != WAVE["number"] or assignments[0].get("outcome_task_id") != WAVE["outcome_task_id"]:
        raise RuntimeError(f"bounded-wave assignment drift: {assignments}")
    return record, assignments[0]


def exact_context() -> Any:
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != INSTANCE or ln.setup.settings.branch.name != BRANCH:
        raise RuntimeError("refusing execution outside laminlabs/pertdata branch jkobject")
    return ln


def duplicate_probe(ln: Any, candidate_uri: str) -> dict[str, Any]:
    queries = {
        "logical_key": list(ln.Artifact.filter(key__startswith=LOGICAL_KEY).all()),
        "record_id": list(ln.Artifact.filter(description__icontains=RECORD_ID).all()),
        "accession": list(ln.Artifact.filter(description__icontains=ACCESSION).all()),
        "data_index": list(ln.Artifact.filter(description__icontains=DATA_INDEX).all()),
    }
    records: dict[str, dict[str, str]] = {}
    for rows in queries.values():
        for row in rows:
            records[str(row.uid)] = {"uid": str(row.uid), "key": str(row.key or ""), "description": str(row.description or "")}
    existing = run(["gcloud", "storage", "ls", f"--billing-project={BILLING_PROJECT}", f"{GCS_ROOT}/{LOGICAL_KEY}/revisions/**"], check=False)
    candidate = run(["gcloud", "storage", "ls", f"--billing-project={BILLING_PROJECT}", f"{candidate_uri}/**"], check=False)
    return {
        "lamin_query_counts": {key: len(value) for key, value in queries.items()},
        "lamin_candidates": list(records.values()),
        "existing_revision_objects": sorted(line for line in existing.stdout.splitlines() if line.strip()),
        "candidate_objects": sorted(line for line in candidate.stdout.splitlines() if line.strip()),
    }


def describe_gcs(uri: str) -> dict[str, Any]:
    result = run(["gcloud", "storage", "objects", "describe", uri, f"--billing-project={BILLING_PROJECT}", "--format=json"])
    value = json.loads(result.stdout)
    return {
        "uri": uri.split("#", 1)[0],
        "generation": str(value["generation"]),
        "generation_uri": f"{uri.split('#', 1)[0]}#{value['generation']}",
        "size_bytes": int(value["size"]),
        "md5_base64": value.get("md5_hash") or value.get("md5Hash"),
        "crc32c_base64": value.get("crc32c_hash") or value.get("crc32c"),
        "updated": value.get("update_time") or value.get("updateTime") or value.get("updated"),
    }


def upload_immutable(path: Path, uri: str, *, role: str) -> dict[str, Any]:
    result = run(["gcloud", "storage", "cp", "--if-generation-match=0", f"--billing-project={BILLING_PROJECT}", str(path), uri], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"immutable upload failed for {uri}: {result.stderr.strip()}")
    identity = describe_gcs(uri)
    identity.update({"role": role, "sha256": sha256_file(path)})
    if identity["size_bytes"] != path.stat().st_size or SHA_RE.fullmatch(identity["sha256"]) is None:
        raise RuntimeError(f"uploaded object identity failure: {uri}")
    return identity


def generation_readback(identity: dict[str, Any], destination: Path) -> dict[str, Any]:
    run(["gcloud", "storage", "cp", f"--billing-project={BILLING_PROJECT}", identity["generation_uri"], str(destination)])
    result = {"size_bytes": destination.stat().st_size, "sha256": sha256_file(destination)}
    if result != {"size_bytes": identity["size_bytes"], "sha256": identity["sha256"]}:
        raise RuntimeError(f"generation-pinned readback mismatch: {identity['uri']}")
    return result


def extract_source(archive: Path, destination: Path) -> dict[str, Any]:
    with tarfile.open(archive, "r:gz") as handle:
        members = handle.getmembers()
        if len(members) != 1 or members[0].name != SOURCE_MEMBER or members[0].size != SOURCE_MEMBER_SIZE:
            raise RuntimeError(f"source archive member contract drift: {[(item.name, item.size) for item in members]}")
        source = handle.extractfile(members[0])
        if source is None:
            raise RuntimeError("source archive member is unreadable")
        with source, destination.open("wb") as target:
            shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
    identity = {"name": SOURCE_MEMBER, "size_bytes": destination.stat().st_size, "sha256": sha256_file(destination)}
    if identity != {"name": SOURCE_MEMBER, "size_bytes": SOURCE_MEMBER_SIZE, "sha256": SOURCE_MEMBER_SHA256}:
        raise RuntimeError(f"source member identity drift: {identity}")
    return identity


def parquet_safe(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    result = frame.copy()
    normalizations: list[dict[str, str]] = []
    for column in result.columns:
        dtype = result[column].dtype
        if isinstance(dtype, pd.CategoricalDtype):
            result[column] = result[column].astype("string")
            normalizations.append({"column": str(column), "from": "category", "to": "string"})
        elif dtype == object and result[column].dropna().map(lambda value: isinstance(value, str)).all():
            result[column] = result[column].astype("string")
            normalizations.append({"column": str(column), "from": "object[str]", "to": "string"})
    result.index = result.index.astype(str)
    return result, normalizations


def prepare_obs(source: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    obs = source.copy()
    obs.index.name = "obs_id"
    obs["dataset"] = LOGICAL_KEY
    obs["source"] = "PerturBase"
    obs["source_accession"] = ACCESSION
    obs["source_bioproject"] = BIOPROJECT
    obs["source_component"] = DATA_INDEX
    obs["cell_id"] = obs.index.astype(str)
    obs["organism"] = "Homo sapiens"
    obs["cell_line"] = "hPSC"
    obs["cell_type"] = obs["media"].astype(str)
    obs["modality"] = "scRNA-seq"
    obs["assay"] = "pooled ORF overexpression scRNA-seq"
    obs["technology"] = "10x Chromium"
    obs["perturbation"] = obs["gene"].astype(str)
    obs["perturbation_target"] = obs["gene"].astype(str)
    obs["perturbation_type"] = "overexpression"
    obs["perturbation_technology"] = "pooled ORF overexpression"
    obs["perturbation_library"] = "Parekh et al. pooled TF ORF library"
    obs["is_control"] = obs["gene"].astype(str).eq("CTRL")
    obs["is_bulk"] = False
    obs["is_pseudobulk"] = False
    obs["n_counts"] = obs["total_counts"]
    obs["pct_mito"] = obs["pct_counts_mt"]
    obs["age"] = pd.Series(pd.NA, index=obs.index, dtype="string")
    obs["age_missingness_reason"] = "cell_line_experiment_has_no_donor_or_cellular_age_field"
    obs["donor_id"] = pd.Series(pd.NA, index=obs.index, dtype="string")
    obs["donor_id_missingness_reason"] = "hPSC_line_not_donor_resolved"
    obs["sex"] = pd.Series(pd.NA, index=obs.index, dtype="string")
    obs["sex_missingness_reason"] = "source_not_reported"
    obs["ethnicity"] = pd.Series(pd.NA, index=obs.index, dtype="string")
    obs["ethnicity_missingness_reason"] = "source_not_reported"
    obs["timepoint"] = pd.Series(pd.NA, index=obs.index, dtype="Float64")
    obs["timepoint_missingness_reason"] = "source_reports_media_conditions_not_elapsed_time"
    obs["is_low_quality"] = pd.Series(pd.NA, index=obs.index, dtype="boolean")
    obs["quality_missingness_reason"] = "PerturBase_QC_pass_filtered_component_has_no_reviewed_per_cell_low_quality_boolean"
    return parquet_safe(obs)


def prepare_var(source: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    var = source.copy()
    var.index = var.index.astype(str)
    var.index.name = "var_id"
    var["gene_symbol"] = var.index
    var["gene_id"] = var["ENSEMBL"].astype("string")
    var["organism"] = "Homo sapiens"
    var["feature_namespace"] = "source_gene_symbol_with_optional_Ensembl"
    return parquet_safe(var)


def write_sparse_zarr_zip(path: Path, matrix: sparse.csr_matrix) -> dict[str, Any]:
    matrix.sum_duplicates()
    matrix.sort_indices()
    store = zarr.storage.ZipStore(str(path), mode="w")
    try:
        group = zarr.group(store=store)
        group.attrs.update({"format": "csr_matrix", "shape": list(matrix.shape), "nnz": int(matrix.nnz), "dtype": str(matrix.dtype)})
        for name, values in (("data", matrix.data), ("indices", matrix.indices), ("indptr", matrix.indptr)):
            group.create_dataset(name, data=values, chunks=(max(1, min(len(values), 65536)),))
    finally:
        store.close()
    return {
        "format": "csr_matrix",
        "shape": list(matrix.shape),
        "nnz": int(matrix.nnz),
        "dtype": str(matrix.dtype),
        "sum": float(matrix.sum(dtype=np.float64)),
        "minimum": float(matrix.data.min()) if matrix.nnz else 0.0,
        "maximum": float(matrix.data.max()) if matrix.nnz else 0.0,
        "negative_values": int((matrix.data < 0).sum()),
        "data_sha256": sha256_array(matrix.data),
        "indices_sha256": sha256_array(matrix.indices),
        "indptr_sha256": sha256_array(matrix.indptr),
    }


def read_sparse_zarr_zip(path: Path) -> sparse.csr_matrix:
    store = zarr.storage.ZipStore(str(path), mode="r")
    try:
        group = zarr.open_group(store=store, mode="r")
        return sparse.csr_matrix((np.asarray(group["data"]), np.asarray(group["indices"]), np.asarray(group["indptr"])), shape=tuple(group.attrs["shape"]))
    finally:
        store.close()


def matrix_identity(matrix: sparse.csr_matrix) -> dict[str, Any]:
    return {
        "format": "csr_matrix",
        "shape": list(matrix.shape),
        "nnz": int(matrix.nnz),
        "dtype": str(matrix.dtype),
        "sum": float(matrix.sum(dtype=np.float64)),
        "minimum": float(matrix.data.min()) if matrix.nnz else 0.0,
        "maximum": float(matrix.data.max()) if matrix.nnz else 0.0,
        "negative_values": int((matrix.data < 0).sum()),
        "data_sha256": sha256_array(matrix.data),
        "indices_sha256": sha256_array(matrix.indices),
        "indptr_sha256": sha256_array(matrix.indptr),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if socket.gethostname().split(".")[0] != EXPECTED_HOST:
        raise RuntimeError(f"production execution is restricted to {EXPECTED_HOST}")
    if re.fullmatch(r"perturbase-gse107185-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", args.revision) is None:
        raise RuntimeError("revision does not satisfy the fresh immutable naming contract")
    if args.output.exists():
        raise RuntimeError("output evidence directory already exists")
    if args.source_archive.stat().st_size != SOURCE_SIZE or sha256_file(args.source_archive) != SOURCE_SHA256:
        raise RuntimeError("exact PerturBase source archive identity mismatch")

    record, assignment = validate_controls()
    candidate_uri = f"{GCS_ROOT}/{LOGICAL_KEY}/revisions/{args.revision}"
    ln = exact_context()
    pre_probe = duplicate_probe(ln, candidate_uri)
    if pre_probe["lamin_candidates"] or pre_probe["existing_revision_objects"] or pre_probe["candidate_objects"]:
        raise RuntimeError(f"duplicate preflight found an existing target: {pre_probe}")

    args.output.mkdir(parents=True)
    script_sha = sha256_file(Path(__file__))
    started = time.time()
    lock_metadata = {
        "task_id": TASK_ID,
        "record_id": RECORD_ID,
        "run_id": f"{TASK_ID}-{args.revision}",
        "pid": os.getpid(),
        "host": EXPECTED_HOST,
        "zone": ZONE,
        "project": BILLING_PROJECT,
        "branch": BRANCH,
        "started_at": started,
    }
    with ExitStack() as locks, tempfile.TemporaryDirectory(prefix=f"{TASK_ID}-") as temp_name:
        locks.enter_context(lamin_writer_lock(vm_global_lamin_writer_lock_path(), lock_metadata))
        for lock_path in legacy_lamin_writer_lock_paths():
            locks.enter_context(lamin_writer_lock(lock_path, lock_metadata, check_live_metadata=False))
        ln.track()
        under_lease_probe = duplicate_probe(ln, candidate_uri)
        if under_lease_probe["lamin_candidates"] or under_lease_probe["existing_revision_objects"] or under_lease_probe["candidate_objects"]:
            raise RuntimeError("duplicate appeared under global writer lease")

        temp = Path(temp_name)
        h5ad_path = temp / SOURCE_MEMBER
        member_identity = extract_source(args.source_archive, h5ad_path)
        source = ad.read_h5ad(h5ad_path, backed="r")
        try:
            if tuple(source.shape) != EXPECTED_SHAPE or not source.obs_names.is_unique or not source.var_names.is_unique:
                raise RuntimeError("filtered source shape or ordered-axis uniqueness drift")
            if int(source.obs["gene"].nunique(dropna=False)) != EXPECTED_PERTURBATIONS:
                raise RuntimeError("filtered perturbation denominator drift")
            obs, obs_normalizations = prepare_obs(source.obs.copy())
            var, var_normalizations = prepare_var(source.var.copy())
            matrix = sparse.csr_matrix(np.asarray(source.X[:, :], dtype=np.float32))
        finally:
            source.file.close()
        if tuple(matrix.shape) != EXPECTED_SHAPE or len(obs) != EXPECTED_SHAPE[0] or len(var) != EXPECTED_SHAPE[1]:
            raise RuntimeError("materialized denominator drift")
        if not obs.index.equals(pd.Index(source.obs_names.astype(str), name="obs_id")) or not var.index.equals(pd.Index(source.var_names.astype(str), name="var_id")):
            raise RuntimeError("ordered axis parity failure")

        dataset_dir = temp / "dataset"
        readback_dir = temp / "readback"
        dataset_dir.mkdir()
        readback_dir.mkdir()
        obs_path = dataset_dir / "obs.parquet"
        x_path = dataset_dir / "X.zarr.zip"
        var_path = dataset_dir / "var.parquet"
        obs.to_parquet(obs_path)
        var.to_parquet(var_path)
        matrix_stats = write_sparse_zarr_zip(x_path, matrix)
        obs_rb = pd.read_parquet(obs_path)
        var_rb = pd.read_parquet(var_path)
        if frame_semantic_sha256(obs_rb) != frame_semantic_sha256(obs) or frame_semantic_sha256(var_rb) != frame_semantic_sha256(var):
            raise RuntimeError("local Parquet semantic parity failure")
        if matrix_identity(read_sparse_zarr_zip(x_path)) != matrix_stats:
            raise RuntimeError("local sparse Zarr parity failure")

        fingerprint = hashlib.sha256(json_bytes({
            "task_id": TASK_ID,
            "record_id": RECORD_ID,
            "script_sha256": script_sha,
            "source_sha256": SOURCE_SHA256,
            "source_member_sha256": SOURCE_MEMBER_SHA256,
            "frozen_sha256": FROZEN_SHA256,
            "graph_sha256": GRAPH_SHA256,
            "revision": args.revision,
        })).hexdigest()
        dataset_prefix = f"{candidate_uri}/datasets/{DATA_INDEX}"
        source_object = upload_immutable(args.source_archive, f"{candidate_uri}/source/{DATA_INDEX}.filter.tar.gz", role="frozen_source_archive")
        objects = [
            upload_immutable(obs_path, f"{dataset_prefix}/obs.parquet", role="canonical_obs"),
            upload_immutable(x_path, f"{dataset_prefix}/X.zarr.zip", role="canonical_X_processed_mixscape_hvg"),
            upload_immutable(var_path, f"{dataset_prefix}/var.parquet", role="canonical_var"),
        ]
        source_object["producer_generation_readback"] = generation_readback(source_object, readback_dir / "source.tar.gz")
        for obj in objects:
            obj["producer_generation_readback"] = generation_readback(obj, readback_dir / Path(obj["uri"]).name)
        if frame_semantic_sha256(pd.read_parquet(readback_dir / "obs.parquet")) != frame_semantic_sha256(obs):
            raise RuntimeError("remote obs semantic readback failure")
        if frame_semantic_sha256(pd.read_parquet(readback_dir / "var.parquet")) != frame_semantic_sha256(var):
            raise RuntimeError("remote var semantic readback failure")
        if matrix_identity(read_sparse_zarr_zip(readback_dir / "X.zarr.zip")) != matrix_stats:
            raise RuntimeError("remote matrix semantic readback failure")

        media_counts = {str(key): int(value) for key, value in obs["media"].value_counts(dropna=False).items()}
        batch_counts = {str(key): int(value) for key, value in obs["batch"].value_counts(dropna=False).items()}
        perturbation_counts = {str(key): int(value) for key, value in obs["perturbation"].value_counts(dropna=False).items()}
        ordered_var_identity = hashlib.sha256(("\n".join(map(str, var.index)) + "\nHomo sapiens\nsource_gene_symbol_with_optional_Ensembl\n").encode()).hexdigest()
        missingness = {
            "non_excluding": True,
            "dependency_created": False,
            "excluded_component_records": 0,
            "excluded_observations": 0,
            "affected_observations": len(obs),
            "age_missing_count": int(obs["age"].isna().sum()),
            "donor_id_missing_count": int(obs["donor_id"].isna().sum()),
            "sex_missing_count": int(obs["sex"].isna().sum()),
            "ethnicity_missing_count": int(obs["ethnicity"].isna().sum()),
            "timepoint_missing_count": int(obs["timepoint"].isna().sum()),
            "timepoint_reason": "the source labels hPSC/endothelial/multilineage media conditions, not elapsed time",
            "is_low_quality_missing_count": int(obs["is_low_quality"].isna().sum()),
            "quality_reason": "the exact PerturBase QC-pass filtered component is retained in full; no reviewed per-cell low-quality boolean is supplied",
            "var_ensembl_missing_count": int(var["gene_id"].isna().sum()),
            "var_reason": "source HVG object has optional ENSEMBL identifiers; source gene symbols remain the ordered canonical axis",
        }
        ledger = {
            "schema_version": "pert-gym.proposed-publication-ledger/v2",
            "submission_id": f"pert-gym-ledger:materialized-build:{TASK_ID}:{RECORD_ID}",
            "submission_status": "submitted_materialized_build_entry_zero_product_credit",
            "accepted_delta_at_build": 0,
            "record_id": RECORD_ID,
            "catalogue_row_ids": [CATALOGUE_ROW],
            "target_logical_key": LOGICAL_KEY,
            "wave": WAVE,
            "record_accounting": {"source_component_records": 1, "included_component_records": 1, "excluded_component_records": 0, "materialized_component_records": 1},
            "dataset_accounting": {"source_datasets": 1, "included_datasets": 1, "excluded_datasets": 0, "materialized_triplets": 1},
            "observation_accounting": {"catalogue_reported_n_obs": EXPECTED_SHAPE[0], "resolved_eligible_n_obs": len(obs), "materialized_n_obs": len(obs), "excluded_n_obs": 0, "dropped_n_obs": 0},
            "artifact_accounting": {"source_object_count": 1, "payload_object_count": 3, "ledger_object_count": 1, "manifest_object_count": 1, "triplet_count": 1},
            "deduplication_key": f"record_id:{RECORD_ID}",
            "deduplication_scope_count": 1,
            "quality_missingness_is_not_exclusion": True,
            "credit_state": "materialized_pending_independent_test_review_and_administrative_acceptance",
            "debits": {"exclusions": 0, "dropped_observations": 0, "product_credit": 0},
            "credits": {"materialized_component_records": 1, "materialized_triplets": 1, "materialized_observations": len(obs)},
        }
        ledger_path = args.output / "ledger.json"
        ledger_path.write_bytes(json_bytes(ledger))
        ledger_object = upload_immutable(ledger_path, f"{candidate_uri}/ledger.json", role="proposed_ledger")
        ledger_object["producer_generation_readback"] = generation_readback(ledger_object, readback_dir / "ledger.json")

        manifest = {
            "schema_version": "pert-gym.perturbase-filtered-component/v1",
            "manifest_last": True,
            "task_id": TASK_ID,
            "record_id": RECORD_ID,
            "component": COMPONENT,
            "catalogue_row_ids": [CATALOGUE_ROW],
            "target_logical_key": LOGICAL_KEY,
            "bounded_wave": WAVE,
            "bounded_wave_assignment": assignment,
            "bounded_wave_duplicate_check": {"assignment_count": 1, "duplicated_in_other_wave": False},
            "revision": args.revision,
            "revision_fingerprint_sha256": fingerprint,
            "revision_prefix": candidate_uri,
            "source_identity": {
                "accession": ACCESSION,
                "bioproject": BIOPROJECT,
                "data_index": DATA_INDEX,
                "source_uri": SOURCE_URL,
                "http_identity": {"content_length": SOURCE_SIZE, "etag": SOURCE_ETAG, "last_modified": SOURCE_LAST_MODIFIED},
                "archive": {"size_bytes": SOURCE_SIZE, "sha256": SOURCE_SHA256, "immutable_copy": source_object},
                "archive_member": member_identity,
                "controlling_publication_manifest_sha256": FROZEN_SHA256,
                "controlling_record": record,
                "bounded_wave_graph_sha256": GRAPH_SHA256,
            },
            "build_identity": {
                "script_sha256": script_sha,
                "host": EXPECTED_HOST,
                "zone": ZONE,
                "billing_project": BILLING_PROJECT,
                "lamin_instance": INSTANCE,
                "lamin_branch": BRANCH,
                "operation": "exact PerturBase QC-pass extend_61 object to immutable sparse-Zarr obs/X/var dataset",
                "row_filtering": "none; all 8,428 observations in the exact filtered object retained",
                "source_matrix_semantics": "source X equals source X_pert: dense standardized Mixscape perturbation representation over 2,000 HVGs; converted value-exactly to CSR",
                "lamin_writes": 0,
                "collection_writes": 0,
                "duplicate_preflight": pre_probe,
                "duplicate_under_lease": under_lease_probe,
            },
            "dataset_count": 1,
            "sample_count": int(obs["batch"].nunique()),
            "triplet_count": 1,
            "artifact_count": 3,
            "observation_count": len(obs),
            "variable_count": len(var),
            "perturbation_label_count": int(obs["perturbation"].nunique()),
            "dataset": {
                "shape": [len(obs), len(var)],
                "prefix": dataset_prefix,
                "obs": frame_signature(obs),
                "var": frame_signature(var),
                "X": matrix_stats,
                "ordered_var_identity_sha256": ordered_var_identity,
                "objects": {obj["role"]: obj for obj in objects},
                "links": {"obs_to_X": "identical ordered cell index", "X_to_var": "identical ordered feature index"},
                "quality_disposition": "included_exactly_once",
            },
            "counts": {"media": media_counts, "batch": batch_counts, "perturbation": perturbation_counts},
            "obs_parquet_normalizations": obs_normalizations,
            "var_parquet_normalizations": var_normalizations,
            "actual_artifact_inventory": objects,
            "source_object": source_object,
            "ledger_object": ledger_object,
            "ledger": ledger,
            "missingness": missingness,
            "execution": {"started_at_epoch": started, "completed_at_epoch": time.time(), "payload_bytes_written": sum(obj["size_bytes"] for obj in objects), "product_credit": 0},
        }
        manifest_path = args.output / "manifest.json"
        manifest_path.write_bytes(json_bytes(manifest))
        manifest_object = upload_immutable(manifest_path, f"{candidate_uri}/manifest.json", role="manifest")
        manifest_object["producer_generation_readback"] = generation_readback(manifest_object, readback_dir / "manifest.json")
        if int(manifest_object["generation"]) <= max(int(item["generation"]) for item in objects + [source_object, ledger_object]):
            raise RuntimeError("manifest was not written last")

        producer_readback = {
            "schema_version": "pert-gym.machine-readback/v1",
            "verdict": "PASS",
            "record_id": RECORD_ID,
            "target_logical_key": LOGICAL_KEY,
            "revision_prefix": candidate_uri,
            "manifest": manifest_object,
            "scope": {"catalogue_row_ids": [CATALOGUE_ROW], "datasets": 1, "samples": int(obs["batch"].nunique()), "triplets": 1, "payload_objects": 3},
            "counts": {"observations": len(obs), "variables": len(var), "perturbations": int(obs["perturbation"].nunique()), "nnz": int(matrix.nnz), "media": media_counts, "batch": batch_counts},
            "matrix_identity": matrix_stats,
            "wave": WAVE,
            "all_generation_pinned_readbacks_passed": True,
            "ordered_axis_parity": True,
            "explicit_missingness_preserved": True,
            "ledger_balanced": True,
            "product_credit": 0,
        }
        (args.output / "producer-readback.json").write_bytes(json_bytes(producer_readback))
        (args.output / "manifest-object.json").write_bytes(json_bytes(manifest_object))
        (args.output / "writer-result.json").write_bytes(json_bytes({"verdict": "PASS", "revision_prefix": candidate_uri, "manifest": manifest_object, "counts": producer_readback["counts"], "runtime_seconds": time.time() - started, "product_credit": 0}))
        print(json.dumps(producer_readback, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
