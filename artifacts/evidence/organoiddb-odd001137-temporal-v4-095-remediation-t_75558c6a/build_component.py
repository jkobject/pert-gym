#!/usr/bin/env python3
"""Build remediated temporal-v4 row 95 (OrganoidDB Odd001137 / GSE158999)."""
from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import socket
import tempfile
import time
import urllib.request
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Iterator

import anndata as ad
import fsspec
import h5py
import numpy as np
import pandas as pd
from scipy import io as scipy_io

TASK_ID = "t_75558c6a"
SUPERSEDES_REVISION = "temporal-v4-095-wave10-b1dee01a49f4c78c"
RECORD_ID = "temporal_v4_095_organoiddb_odd001137_gse158999"
COMPONENT = "Mouse embryonic heart organoid/development scRNA-seq Odd001137"
LOGICAL_KEY = "pert-gym/logical/temporal/organoiddb_odd001137_gse158999"
ACCESSION = "GSE158999"
ORGANOIDDB_ID = "ODD001137"
ROW = 95
EXPECTED_SAMPLES = 8
EXPECTED_OBS = 30_496
EXPECTED_VARS = 23_961
SOURCE_MANIFEST_SHA = "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
GRAPH_SHA = "59c18752f65257270b980353811da5bf554d5ac2b6c11c550a63849664ce9c98"
CATALOGUE_SHA = "4d31f341b60163ba1bcf6293746b9f8fe483cbccf6cd975367ed62a30467fdea"
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
BUCKET_ROOT = "scperturb/pert-gym/staging"
EXPECTED_HOST = "pert-gym-worker-eu"



def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024**2):
            digest.update(block)
    return digest.hexdigest()


def index_sha(index: pd.Index) -> str:
    return hashlib.sha256(("\n".join(map(str, index)) + "\n").encode()).hexdigest()


def download(url: str, path: Path, expected_sha: str, expected_size: int | None = None) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "pert-gym/temporal-v4-row95"})
    digest, size = hashlib.sha256(), 0
    with urllib.request.urlopen(request, timeout=120) as source, path.open("wb") as target:
        headers = dict(source.headers.items())
        while block := source.read(8 * 1024**2):
            target.write(block)
            digest.update(block)
            size += len(block)
    if digest.hexdigest() != expected_sha or (expected_size is not None and size != expected_size):
        raise RuntimeError(f"source identity mismatch: {url}")
    return {"uri": url, "size_bytes": size, "sha256": digest.hexdigest(), "http_headers": {k: headers.get(k) for k in ("ETag", "Last-Modified", "Content-Length")}}


def parse_soft(path: Path) -> dict[str, dict[str, str]]:
    text = gzip.decompress(path.read_bytes()).decode("utf-8", "replace")
    samples: dict[str, dict[str, str]] = {}
    for chunk in text.split("^SAMPLE = ")[1:]:
        lines = chunk.splitlines()
        gsm = lines[0].strip()
        row: dict[str, str] = {"sample_accession": gsm}
        for line in lines[1:]:
            if line.startswith("!Sample_title = "):
                row["sample_title"] = line.split(" = ", 1)[1]
            elif line.startswith("!Sample_source_name_ch1 = "):
                row["source_name"] = line.split(" = ", 1)[1]
            elif line.startswith("!Sample_characteristics_ch1 = "):
                value = line.split(" = ", 1)[1]
                if ": " in value:
                    key, value = value.split(": ", 1)
                    row[key.strip().lower()] = value.strip()
        samples[gsm] = row
    if len(samples) != EXPECTED_SAMPLES:
        raise RuntimeError(f"expected {EXPECTED_SAMPLES} SOFT samples, got {len(samples)}")
    return samples


def sample_accession_map(sample_meta: dict[str, dict[str, str]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for accession, row in sample_meta.items():
        day = int(row["day"])
        batch = int(row["batch"])
        result[f"GAS_Day{day}_batch{batch}"] = accession
    if len(result) != EXPECTED_SAMPLES:
        raise RuntimeError("SOFT sample-to-metadata mapping is not one-to-one")
    return result


def validate_controls(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    files = {
        "manifest": (root / "downloadable_logical_publication_manifest_20260713.json", SOURCE_MANIFEST_SHA),
        "graph": (root / "kanban_graph_compaction_t_36a3533e_manifest.json", GRAPH_SHA),
        "catalogue": (root / "temporal_pretraining_datasets_v4.tsv", CATALOGUE_SHA),
    }
    for name, (path, expected) in files.items():
        if sha256_file(path) != expected:
            raise RuntimeError(f"{name} checksum mismatch")
    publication = json.loads(files["manifest"][0].read_text())
    records = [row for row in publication["records"] if row.get("record_id") == RECORD_ID]
    if len(records) != 1:
        raise RuntimeError("controlling record is not unique")
    record = records[0]
    expected = {"component": COMPONENT, "catalogue_row_ids": [ROW], "target_logical_key": LOGICAL_KEY, "source_object_identity": f"{ACCESSION};{ORGANOIDDB_ID}", "classification": "executable", "downloadable": "yes"}
    if any(record.get(key) != value for key, value in expected.items()):
        raise RuntimeError("controlling record identity drift")
    graph = json.loads(files["graph"][0].read_text())
    assignments = [row for row in graph["component_assignments"] if row.get("record_id") == RECORD_ID]
    if len(assignments) != 1 or assignments[0].get("wave") != 10 or assignments[0].get("outcome_task_id") != WAVE["outcome_task_id"]:
        raise RuntimeError("bounded wave assignment drift")
    return record, assignments[0]


@contextmanager
def exclusive_lock(path: Path, metadata: dict[str, Any]) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(metadata, sort_keys=True) + "\n")
        handle.flush()
        yield
    finally:
        handle.seek(0)
        handle.truncate()
        handle.flush()
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        path.unlink(missing_ok=True)


def remote_upload(fs: Any, local: Path, key: str) -> dict[str, Any]:
    if fs.exists(key):
        raise RuntimeError(f"refusing overwrite: gs://{key}")
    digest, size = hashlib.sha256(), 0
    with local.open("rb") as source, fs.open(key, "xb") as target:
        while block := source.read(8 * 1024**2):
            target.write(block)
            digest.update(block)
            size += len(block)
    info = fs.info(key)
    if int(info["size"]) != size:
        raise RuntimeError(f"uploaded size mismatch: {key}")
    return {"key": key, "uri": f"gs://{key}", "generation": str(info["generation"]), "generation_uri": f"gs://{key}#{info['generation']}", "size_bytes": size, "sha256": digest.hexdigest()}


def remote_download(fs: Any, obj: dict[str, Any], local: Path) -> dict[str, Any]:
    digest, size = hashlib.sha256(), 0
    with fs.open(f"{obj['key']}#{obj['generation']}", "rb") as source, local.open("wb") as target:
        while block := source.read(8 * 1024**2):
            target.write(block)
            digest.update(block)
            size += len(block)
    result = {"generation": obj["generation"], "size_bytes": size, "sha256": digest.hexdigest()}
    if result["size_bytes"] != obj["size_bytes"] or result["sha256"] != obj["sha256"]:
        raise RuntimeError(f"generation-qualified readback mismatch: {obj['key']}")
    return result


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

def build_dataset(source_paths: dict[str, Path], output: Path) -> dict[str, Any]:
    sample_meta = parse_soft(source_paths["soft"])
    accession_by_dataset = sample_accession_map(sample_meta)
    with gzip.open(source_paths["matrix"], "rb") as stream:
        matrix = scipy_io.mmread(stream).tocsr().T.astype(np.uint32)
    with gzip.open(source_paths["barcodes"], "rt") as stream:
        barcodes = stream.read().splitlines()
    with gzip.open(source_paths["features"], "rt") as stream:
        feature_ids = stream.read().splitlines()
    obs = pd.read_csv(source_paths["metadata"], sep="\t", compression="gzip", index_col=0)
    obs.index = pd.Index(obs.index.astype(str), name="cell_id")
    if len(barcodes) != len(obs):
        raise RuntimeError("metadata/barcode denominator mismatch")
    if list(obs.index) != barcodes:
        raise RuntimeError("metadata and barcode files are not in identical cell order")
    if matrix.shape != (len(obs), len(feature_ids)):
        raise RuntimeError(f"matrix axis mismatch: {matrix.shape}")
    if set(obs["Dataset"].astype(str)) != set(accession_by_dataset):
        raise RuntimeError("metadata datasets do not match the eight SOFT samples")
    if any(not cell_id.startswith(f"{dataset}_") for cell_id, dataset in zip(obs.index, obs["Dataset"].astype(str), strict=True)):
        raise RuntimeError("metadata cell identifiers do not match dataset labels")
    obs.insert(0, "source_accession", ACCESSION)
    obs.insert(1, "organoiddb_id", ORGANOIDDB_ID)
    obs.insert(2, "sample_accession", obs["Dataset"].astype(str).map(accession_by_dataset))
    obs.insert(3, "source_cell_barcode", barcodes)
    obs["development_stage"] = obs["stage"].astype(str)
    obs["timepoint"] = obs["stage"].astype(str).str.extract(r"Day(\d+)", expand=False).astype(int)
    obs["timepoint_unit"] = "day"
    obs["organism"] = "Mus musculus"
    obs["assay"] = "10x Genomics scRNA-seq"
    obs["tissue"] = "gastruloid / embryonic heart organoid model"
    obs["is_control"] = False
    obs["trajectory_id"] = f"{ACCESSION}:gastruloid-cardiogenesis"
    obs["source_matrix_semantics"] = "raw UMI count matrix"
    var_index = pd.Index(feature_ids, name="feature_id")
    if var_index.has_duplicates:
        raise RuntimeError("source gene-symbol feature axis is not unique")
    var = pd.DataFrame({"feature_id": feature_ids, "gene_symbol": feature_ids, "feature_type": "Gene Expression", "feature_namespace": "MGI gene symbol", "organism": "Mus musculus", "genome_build": "not supplied by GEO"}, index=var_index)
    if matrix.shape != (EXPECTED_OBS, EXPECTED_VARS) or len(obs) != EXPECTED_OBS or obs.index.has_duplicates:
        raise RuntimeError(f"resolved denominator mismatch: matrix={matrix.shape}, obs={len(obs)}")
    obs_path, x_path, var_path = output / "obs.parquet", output / "X.h5ad", output / "var.parquet"
    obs.to_parquet(obs_path)
    var.to_parquet(var_path)
    ad.AnnData(X=matrix, obs=pd.DataFrame(index=obs.index), var=pd.DataFrame(index=var.index)).write_h5ad(x_path, compression="gzip")
    return {"obs": obs_path, "X": x_path, "var": var_path, "obs_frame": obs, "var_frame": var, "matrix": {"shape": list(matrix.shape), "nnz_stored": int(matrix.nnz), "finite_value_sum": int(matrix.sum(dtype=np.uint64)), "max_count": int(matrix.data.max(initial=0))}}


def validate_triplet(paths: dict[str, Path]) -> dict[str, Any]:
    obs, var = pd.read_parquet(paths["obs"]), pd.read_parquet(paths["var"])
    adata = ad.read_h5ad(paths["X"], backed="r")
    if adata.shape != (EXPECTED_OBS, EXPECTED_VARS) or not adata.obs_names.equals(obs.index) or not adata.var_names.equals(var.index):
        raise RuntimeError("triplet ordered-axis readback mismatch")
    adata.file.close()
    return {"shape": [len(obs), len(var)], "obs_columns": list(obs.columns), "var_columns": list(var.columns), "obs_index_sha256": index_sha(obs.index), "var_index_sha256": index_sha(var.index), "sample_count": int(obs["sample_accession"].nunique()), "timepoints_days": sorted(map(int, obs["timepoint"].unique())), "timepoint_counts": {str(key): int(value) for key, value in obs["timepoint"].value_counts().sort_index().items()}, "control_observations": int(obs["is_control"].sum()), "source_qc_columns": [column for column in ("nCount_RNA", "nFeature_RNA", "percent.mito", "doublet_score") if column in obs.columns], "cell_type_annotation_count": int(obs["celltype"].nunique()), "matrix": matrix_inventory(paths["X"])}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if socket.gethostname().split(".")[0] != EXPECTED_HOST:
        raise RuntimeError("payload may run only on pert-gym-worker-eu")
    args.output.mkdir(parents=True, exist_ok=False)
    record, assignment = validate_controls(args.input)
    script_sha = sha256_file(Path(__file__))
    source_hashes = {role: sha for role, (_, _, sha) in SOURCE_FILES.items()}
    fingerprint = hashlib.sha256(json_bytes({"task_id": TASK_ID, "record_id": RECORD_ID, "script_sha256": script_sha, "source_manifest_sha256": SOURCE_MANIFEST_SHA, "source_sha256": source_hashes, "wave": WAVE})).hexdigest()
    revision = f"temporal-v4-095-wave10-{fingerprint[:16]}"
    revision_prefix = f"{BUCKET_ROOT}/{LOGICAL_KEY}/revisions/{revision}"
    dataset_prefix = f"{revision_prefix}/datasets/{ACCESSION}"
    fs = fsspec.filesystem("gcs", version_aware=True, project=BILLING_PROJECT, requester_pays=BILLING_PROJECT)
    if fs.exists(revision_prefix):
        raise RuntimeError(f"immutable revision already exists: gs://{revision_prefix}")
    started = time.time()
    lock_metadata = {"task_id": TASK_ID, "record_id": RECORD_ID, "revision": revision, "pid": os.getpid(), "started_at": started}
    with ExitStack() as locks:
        for path in (Path("/tmp/pert-gym-vm-global-lamin-writer.lock"), Path("/tmp/pert-gym-global-lamin-writer.lock"), Path.home() / ".cache/pert-gym/lamin-writer.lock"):
            locks.enter_context(exclusive_lock(path, lock_metadata))
        with tempfile.TemporaryDirectory(prefix=f"{TASK_ID}-") as raw_tmp:
            tmp = Path(raw_tmp)
            source_paths: dict[str, Path] = {}
            source_inputs: dict[str, dict[str, Any]] = {}
            for role, (filename, expected_size, expected_sha) in SOURCE_FILES.items():
                path = tmp / filename
                url = SOFT_URL if role == "soft" else f"{SOURCE_BASE}/{filename}"
                source_paths[role] = path
                source_inputs[role] = download(url, path, expected_sha, expected_size)
            built_dir = tmp / "built"
            built_dir.mkdir()
            built = build_dataset(source_paths, built_dir)
            local_paths = {role: built[role] for role in ("obs", "X", "var")}
            source_semantics = validate_triplet(local_paths)
            objects: dict[str, dict[str, Any]] = {}
            readback_paths: dict[str, Path] = {}
            for role, filename in (("obs", "obs.parquet"), ("X", "X.h5ad"), ("var", "var.parquet")):
                obj = remote_upload(fs, local_paths[role], f"{dataset_prefix}/{filename}")
                obj.update({"role": role, "filename": filename})
                readback = tmp / f"readback-{filename}"
                obj["producer_generation_readback"] = remote_download(fs, obj, readback)
                objects[role], readback_paths[role] = obj, readback
            readback_semantics = validate_triplet(readback_paths)
            if readback_semantics != source_semantics:
                raise RuntimeError("semantic generation readback mismatch")
            missingness = {"non_excluding": True, "excluded_records": 0, "excluded_observations": 0, "dependency_created": False, "affected_observations": EXPECTED_OBS, "catalogue_cells_field": 0, "catalogue_cells_field_state": "missing/unknown denominator in frozen catalogue; resolved from the complete GEO matrix and metadata", "absent_source_fields": ["donor_age", "donor_sex", "donor_ethnicity", "disease", "source_low_quality_flag", "accepted_cell_exclusion_threshold"], "age_state": "donor age is absent; gastruloid developmental days 4/5/6/7 are retained and are not donor ages; no observation excluded", "metadata_state": "all GEO metadata rows and 43 source columns are retained, including cell type, sample/batch, stage, embeddings, clusters, and transferred annotation; absent donor/disease fields remain explicit missingness", "quality_state": "source nCount_RNA, nFeature_RNA, percent.mito, and doublet_score are retained, but GEO supplies no accepted low-quality flag or exclusion threshold; all 30,496 cells are retained"}
            ledger = {"schema_version": "pert-gym.proposed-publication-ledger/v2", "submission_id": f"pert-gym-ledger:materialized-build:{TASK_ID}:{RECORD_ID}", "submission_status": "submitted_materialized_build_entry_zero_product_credit", "record_id": RECORD_ID, "catalogue_row_ids": [ROW], "target_logical_key": LOGICAL_KEY, "wave": WAVE, "input_accounting": {"expected_inputs": 8, "accounted_inputs": 8, "controls": 3, "upstream_source_objects": 5, "source_payload_objects": 5}, "output_accounting": {"expected_outputs": 5, "accounted_outputs": 5, "triplet_artifacts": 3, "ledger_objects": 1, "manifest_objects": 1}, "observation_accounting": {"catalogue_source_n_obs_verbatim": 0, "resolved_source_n_obs": EXPECTED_OBS, "materialized_n_obs": EXPECTED_OBS, "excluded_n_obs": 0}, "sample_accounting": {"geo_samples": EXPECTED_SAMPLES, "matrix_objects": 1, "materialized_samples": EXPECTED_SAMPLES}, "debits": {"dropped_observations": 0, "exclusions": 0, "product_credit": 0}, "accepted_delta_at_build": 0, "note": "Build submission only; independent test/review is required before accepted publication credit."}
            ledger_path = tmp / "ledger.json"
            ledger_path.write_bytes(json_bytes(ledger))
            ledger_obj = remote_upload(fs, ledger_path, f"{revision_prefix}/ledger.json")
            manifest = {"schema_version": "pert-gym.materialized-component/v2", "task_id": TASK_ID, "record_id": RECORD_ID, "component": COMPONENT, "catalogue_row_ids": [ROW], "target_logical_key": LOGICAL_KEY, "revision": revision, "revision_prefix": f"gs://{revision_prefix}", "supersedes_revision": SUPERSEDES_REVISION, "supersession_reason": "Correct the prior immutable manifest's hard-coded CSR declaration; physical HDF5 sparse encoding and indptr cardinality are now inspected.", "dataset_count": 1, "triplet_count": 1, "artifact_count": 3, "observation_count": EXPECTED_OBS, "variable_count": EXPECTED_VARS, "sample_count": EXPECTED_SAMPLES, "bounded_wave": WAVE, "bounded_wave_assignment": assignment, "controlling_record": record, "control_inputs": {"publication_manifest": {"sha256": SOURCE_MANIFEST_SHA}, "graph": {"sha256": GRAPH_SHA}, "catalogue": {"sha256": CATALOGUE_SHA}}, "source_identity": {"accession": ACCESSION, "organoiddb_id": ORGANOIDDB_ID, "source_object_identity": f"{ACCESSION};{ORGANOIDDB_ID}", "upstream_objects": source_inputs, "source_payload_object_count": len(source_inputs), "verified": True}, "dataset": {"prefix": f"gs://{dataset_prefix}", "objects": objects, "links": {"obs_to_X": "identical ordered cell_id", "X_to_var": "identical ordered source gene-symbol feature_id"}, "readback": readback_semantics, "matrix_build_inventory": built["matrix"], "consumer_readback_reached_built_data": True, "quality_disposition": "included_exactly_once"}, "actual_artifact_inventory": [objects[role] for role in ("obs", "X", "var")], "missingness": missingness, "ledger": ledger, "ledger_object": ledger_obj, "immutability": {"generation_pinned": True, "overwrite_refused": True, "manifest_written_last": True, "superseded_revision_mutated": False}, "provenance": {"builder_script_sha256": script_sha, "builder_script": str(Path(__file__).resolve()), "host": socket.gethostname(), "command": f"uv run python build_component.py --input {args.input} --output {args.output}", "started_unix": started, "finished_unix": time.time()}}
            manifest_path = tmp / "manifest.json"
            manifest_path.write_bytes(json_bytes(manifest))
            manifest_obj = remote_upload(fs, manifest_path, f"{revision_prefix}/manifest.json")
            remote_manifest = tmp / "manifest-readback.json"
            remote_download(fs, manifest_obj, remote_manifest)
            if remote_manifest.read_bytes() != manifest_path.read_bytes() or int(manifest_obj["generation"]) <= max(int(row["generation"]) for row in manifest["actual_artifact_inventory"] + [ledger_obj]):
                raise RuntimeError("manifest-last generation readback mismatch")
            (args.output / "manifest.json").write_bytes(manifest_path.read_bytes())
            (args.output / "manifest-readback.json").write_bytes(remote_manifest.read_bytes())
            (args.output / "manifest-object.json").write_bytes(json_bytes(manifest_obj))
            (args.output / "ledger.json").write_bytes(json_bytes(ledger))
            result = {"verdict": "PASS", "revision": revision, "manifest_sha256": sha256_file(manifest_path), "manifest_generation_uri": manifest_obj["generation_uri"], "shape": source_semantics["shape"], "nnz_stored": source_semantics["matrix"]["nnz_stored"], "finite_value_sum": source_semantics["matrix"]["finite_value_sum"], "source_sha256": source_hashes, "wave": WAVE}
            (args.output / "writer-result.json").write_bytes(json_bytes(result))
            print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

