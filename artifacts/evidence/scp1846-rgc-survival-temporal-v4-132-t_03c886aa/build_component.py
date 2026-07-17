#!/usr/bin/env python3
"""Build frozen temporal-v4 row 132 from exact GEO count matrices underlying SCP1846."""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import socket
import time
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import h5py
import numpy as np
import pandas as pd
from scipy import sparse

TASK_ID = "t_03c886aa"
RECORD_ID = "temporal_v4_132_overlapping_transcriptional_programs_promote_survival_and_axonal_regeneration_of"
COMPONENT = "Overlapping transcriptional programs promote survival and axonal regeneration of injured retinal ganglion cells"
LOGICAL_KEY = "pert-gym/logical/temporal/overlapping_transcriptional_programs_promote_survival_and_axonal_regeneration_of"
ACCESSION = "SCP1846"
GEO_SUPERSERIES = "GSE202155"
GEO_COMPONENT = "GSE201254"
ROW = 132
EXPECTED = {"n_obs": 129_441, "n_vars": 23_308}
SOURCE_MANIFEST_SHA = "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
GRAPH_SHA = "59c18752f65257270b980353811da5bf554d5ac2b6c11c550a63849664ce9c98"
GENE_AXIS_SHA = "592183b54ba3f8de545620612e6bde4c9b1c225aa79b8c6e143f2bd5bc738ff7"
WAVE = {"id": "publication-wave-13-of-13", "number": 13, "assignment_count": 1, "singular": True, "outcome_task_id": "t_76c7ad2b"}
EXPECTED_HOST = "pert-gym-worker-eu"
BILLING = "jkobject-1549353370965"
BUCKET_ROOT = "scperturb/pert-gym/staging"
GEO_BASE = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE201nnn/GSE201254/suppl"

SAMPLES = [
    ("GSE201254_count_mat_Control-NoCrush.csv.gz", 47_057_425, "2885cda4c82ea6326594bd43dcc1441e57d7b1c3d9a33135bff39c8c4a064d1c", 14_956, "Control", "NoCrush", 0.0),
    ("GSE201254_count_mat_Control-ONC2D.csv.gz", 69_369_454, "e153328e88e80354e02540e673a9e0112da8ce32a7cd5ab3ec522517044cdfd0", 19_562, "Control", "ONC2D", 2.0),
    ("GSE201254_count_mat_Control-ONC7d.csv.gz", 37_050_911, "235516d3b1a78dcaad3304213fa7f038777f8c583a4e07885f1d33e3d6a626d5", 9_154, "Control", "ONC7d", 7.0),
    ("GSE201254_count_mat_Pten-CNTF-NoCrush.csv.gz", 41_693_860, "f01fe15d3b39755c0e9bb244acb3ae14e5a3b23ec719321e53bb9f752e3be356", 8_506, "Pten-CNTF", "NoCrush", 0.0),
    ("GSE201254_count_mat_Pten-CNTF-ONC7d.csv.gz", 28_472_079, "6fd0d322153cd8acbc164df09faf4943a724f93144401aa80db092c2f8e238b1", 6_053, "Pten-CNTF", "ONC7d", 7.0),
    ("GSE201254_count_mat_Pten-NoCrush.csv.gz", 19_676_233, "8d86aae0ee37966321e7a0aca9da1d0c769835ae2f59b237c2da2dc388f87201", 6_780, "Pten", "NoCrush", 0.0),
    ("GSE201254_count_mat_Pten-ONC2D.csv.gz", 40_874_557, "72ace06c22565745be8539fee404bdc5dc4b6605a45a44e54ebd9645bb672817", 12_206, "Pten", "ONC2D", 2.0),
    ("GSE201254_count_mat_Pten-ONC7d.csv.gz", 43_502_228, "0c8352e1cd8e459612409f450eefdcea122fd2ca5861869529d6b1c00b901632", 10_260, "Pten", "ONC7d", 7.0),
    ("GSE201254_count_mat_PtenScos3-CNTF-NoCrush.csv.gz", 22_170_567, "9f6b892eb14cfe1dbe70a26811f6f97e06db3a2779b058e7f6ff7bb68a0c13bc", 6_382, "PtenSocs3-CNTF", "NoCrush", 0.0),
    ("GSE201254_count_mat_PtenScos3-CNTF-ONC21d.csv.gz", 35_675_001, "ed783b132580c01d352de1ecb5180657f247a29c2d0384a380c9b8ba7dfa2dc4", 8_203, "PtenSocs3-CNTF", "ONC21d", 21.0),
    ("GSE201254_count_mat_PtenScos3-CNTF-ONC2D.csv.gz", 21_712_243, "20816fc7c883e9a645ae58eb1cdd9eaa146eb9aab67399310c1347d30e37ee88", 4_994, "PtenSocs3-CNTF", "ONC2D", 2.0),
    ("GSE201254_count_mat_PtenScos3-CNTF-ONC7d.csv.gz", 91_297_922, "a2814f98acf1d56ce17782fe947c6a61fb0956e63ef568f0334384bb5752f5e7", 22_385, "PtenSocs3-CNTF", "ONC7d", 7.0),
]


def jb(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode()


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024**2):
            digest.update(block)
    return digest.hexdigest()


def index_sha(index: pd.Index) -> str:
    return hashlib.sha256(("\n".join(map(str, index)) + "\n").encode()).hexdigest()


def controls(inp: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest_path = inp / "downloadable_logical_publication_manifest_20260713.json"
    graph_path = inp / "kanban_graph_compaction_t_36a3533e_manifest.json"
    ledger_path = inp / "accepted-components-preflight.json"
    if sha(manifest_path) != SOURCE_MANIFEST_SHA or sha(graph_path) != GRAPH_SHA:
        raise RuntimeError("frozen control checksum mismatch")
    rows = [r for r in json.loads(manifest_path.read_text())["records"] if r.get("record_id") == RECORD_ID]
    if len(rows) != 1:
        raise RuntimeError("publication record is not unique")
    row = rows[0]
    expected = {"catalogue_row_ids": [ROW], "target_logical_key": LOGICAL_KEY, "source_object_identity": ACCESSION, "source_n_obs": EXPECTED["n_obs"], "classification": "executable", "downloadable": "yes"}
    if any(row.get(k) != v for k, v in expected.items()):
        raise RuntimeError(f"publication identity drift: {row}")
    if str(row.get("component", "")).strip() != COMPONENT:
        raise RuntimeError(f"publication component drift: {row.get('component')!r}")
    assignments = [x for x in json.loads(graph_path.read_text())["component_assignments"] if x.get("record_id") == RECORD_ID]
    if len(assignments) != 1 or assignments[0].get("wave") != 13 or assignments[0].get("outcome_task_id") != WAVE["outcome_task_id"]:
        raise RuntimeError(f"bounded-wave assignment drift: {assignments}")
    ledger = json.loads(ledger_path.read_text())
    if ledger.get("record_id") != RECORD_ID or ledger.get("component_matches") != 0 or ledger.get("accepted_product_credit") != 0:
        raise RuntimeError("component accepted-ledger precondition is not zero")
    if ledger.get("global_control_plane", {}).get("status") != "unavailable_malformed_administrative_replay_binding":
        raise RuntimeError("global ledger limitation was not captured fail-closed")
    return row, assignments[0], ledger


def parse_csv(path: Path, expected_size: int, expected_sha: str, expected_obs: int, condition: str, raw_time: str, day: float) -> tuple[sparse.csr_matrix, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if path.stat().st_size != expected_size or sha(path) != expected_sha:
        raise RuntimeError(f"source identity mismatch: {path.name}")
    frame = pd.read_csv(path, index_col=0)
    if frame.shape != (EXPECTED["n_vars"], expected_obs) or not frame.index.is_unique or not frame.columns.is_unique:
        raise RuntimeError(f"source shape/axis failure: {path.name} {frame.shape}")
    genes = pd.Index(frame.index.astype(str), name="gene_symbol")
    if index_sha(genes) != GENE_AXIS_SHA:
        raise RuntimeError(f"gene axis drift: {path.name}")
    cells = pd.Index(frame.columns.astype(str), name="cell_id")
    values = frame.to_numpy(dtype=np.int32, copy=True)
    matrix = sparse.csr_matrix(values.T, dtype=np.int32)
    matrix.sort_indices()
    del values, frame
    totals = np.asarray(matrix.sum(axis=1)).ravel().astype(np.int64)
    detected = np.diff(matrix.indptr).astype(np.int32, copy=False)
    mito = np.array([g.upper().startswith("MT-") for g in genes], dtype=bool)
    ribo = np.array([g.upper().startswith(("RPL", "RPS")) for g in genes], dtype=bool)
    mito_counts = np.asarray(matrix[:, mito].sum(axis=1)).ravel()
    ribo_counts = np.asarray(matrix[:, ribo].sum(axis=1)).ravel()
    obs = pd.DataFrame(index=cells)
    obs["source_filename"] = path.name
    obs["source_cell_id"] = cells.to_numpy()
    obs["source_condition"] = condition
    obs["raw_time_label"] = raw_time
    obs["timepoint"] = day
    obs["timepoint_unit"] = "day"
    obs["optic_nerve_crush"] = raw_time != "NoCrush"
    obs["is_control"] = condition == "Control" and raw_time == "NoCrush"
    obs["perturbation"] = condition + (" + optic nerve crush" if raw_time != "NoCrush" else " + no crush")
    obs["total_counts"] = totals
    obs["n_genes_by_counts"] = detected
    obs["pct_counts_mito"] = np.divide(mito_counts * 100.0, totals, out=np.zeros(len(totals), dtype=float), where=totals > 0)
    obs["pct_counts_ribo"] = np.divide(ribo_counts * 100.0, totals, out=np.zeros(len(totals), dtype=float), where=totals > 0)
    obs["organism"] = "Mus musculus"
    obs["tissue"] = "retinal ganglion cells"
    obs["assay"] = "10x Genomics scRNA-seq"
    obs["modality"] = "scRNA-seq"
    obs["source_accession"] = ACCESSION
    obs["geo_accession"] = GEO_COMPONENT
    for column in ("age", "age_unit", "sex", "cell_type", "source_quality_annotation", "is_low_quality"):
        obs[column] = np.nan
    var = pd.DataFrame(index=genes)
    var["gene_symbol"] = genes.to_numpy()
    var["feature_type"] = "Gene Expression"
    inventory = {"filename": path.name, "uri": f"{GEO_BASE}/{path.name}", "size_bytes": expected_size, "sha256": expected_sha, "shape_genes_by_cells": [len(genes), len(cells)], "n_obs": len(cells), "n_vars": len(genes), "nnz": int(matrix.nnz), "raw_count_sum": int(totals.sum()), "gene_axis_sha256": GENE_AXIS_SHA, "condition": condition, "raw_time_label": raw_time, "timepoint_day": day}
    return matrix, obs, var, inventory


def upload(fs: Any, path: Path, key: str, role: str) -> dict[str, Any]:
    if fs.exists(key):
        raise RuntimeError(f"refusing overwrite gs://{key}")
    digest = hashlib.sha256(); size = 0
    with path.open("rb") as src, fs.open(key, "xb") as dst:
        while block := src.read(8 * 1024**2):
            dst.write(block); digest.update(block); size += len(block)
    info = fs.info(key)
    if int(info["size"]) != size:
        raise RuntimeError("upload size mismatch")
    return {"role": role, "filename": path.name, "key": key, "uri": f"gs://{key}", "generation": str(info["generation"]), "generation_uri": f"gs://{key}#{info['generation']}", "size_bytes": size, "sha256": digest.hexdigest()}


def generation_readback(fs: Any, obj: dict[str, Any], path: Path) -> dict[str, Any]:
    digest = hashlib.sha256(); size = 0
    with fs.open(f"{obj['key']}#{obj['generation']}", "rb") as src, path.open("wb") as dst:
        while block := src.read(8 * 1024**2):
            dst.write(block); digest.update(block); size += len(block)
    result = {"generation": obj["generation"], "size_bytes": size, "sha256": digest.hexdigest()}
    if size != obj["size_bytes"] or result["sha256"] != obj["sha256"]:
        raise RuntimeError("generation-qualified readback mismatch")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    started = time.time()
    if socket.gethostname().split(".")[0] != EXPECTED_HOST:
        raise RuntimeError("large build must run on pert-gym-worker-eu")
    row, assignment, ledger_preflight = controls(args.input_dir)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    script_sha = sha(Path(__file__))
    blocks: list[sparse.csr_matrix] = []; obs_parts: list[pd.DataFrame] = []; inventory: list[dict[str, Any]] = []; canonical_var: pd.DataFrame | None = None
    for filename, size, digest, n_obs, condition, raw_time, day in SAMPLES:
        matrix, obs, var, source = parse_csv(args.source_dir / filename, size, digest, n_obs, condition, raw_time, day)
        if canonical_var is None:
            canonical_var = var
        elif not canonical_var.equals(var):
            raise RuntimeError("feature axes differ across source matrices")
        blocks.append(matrix); obs_parts.append(obs); inventory.append(source); gc.collect()
    assert canonical_var is not None
    X = sparse.vstack(blocks, format="csr"); X.sort_indices()
    obs = pd.concat(obs_parts, axis=0); var = canonical_var
    if X.shape != (EXPECTED["n_obs"], EXPECTED["n_vars"]) or len(obs) != EXPECTED["n_obs"] or not obs.index.is_unique or not var.index.is_unique:
        raise RuntimeError(f"final denominator/axis integrity failure: {X.shape}")
    obs_path = args.output_dir / "obs.parquet"; x_path = args.output_dir / "X.h5ad"; var_path = args.output_dir / "var.parquet"
    obs.to_parquet(obs_path); var.to_parquet(var_path)
    ad.AnnData(X=X, obs=pd.DataFrame(index=obs.index.copy()), var=pd.DataFrame(index=var.index.copy())).write_h5ad(x_path, compression="gzip")
    obs_rb = pd.read_parquet(obs_path); var_rb = pd.read_parquet(var_path); x_rb = ad.read_h5ad(x_path, backed="r")
    if list(x_rb.shape) != [len(obs_rb), len(var_rb)] or not x_rb.obs_names.equals(obs_rb.index) or not x_rb.var_names.equals(var_rb.index):
        raise RuntimeError("local artifact readback mismatch")
    x_rb.file.close()
    with h5py.File(x_path, "r") as h5:
        encoding = h5["X"].attrs.get("encoding-type"); written_nnz = int(h5["X/data"].shape[0])
    if isinstance(encoding, bytes): encoding = encoding.decode()
    if written_nnz != int(X.nnz) or encoding != "csr_matrix":
        raise RuntimeError("written sparse matrix encoding/nnz mismatch")
    condition_counts = {str(k): int(v) for k, v in obs["source_condition"].value_counts().sort_index().items()}
    time_counts = {str(k): int(v) for k, v in obs["raw_time_label"].value_counts().sort_index().items()}
    quality = {"verdict": "PASS", "shape": list(X.shape), "nnz": int(X.nnz), "raw_count_sum": int(X.sum()), "matrix_encoding": encoding, "obs_index_sha256": index_sha(obs.index), "var_index_sha256": index_sha(var.index), "gene_axis_sha256": GENE_AXIS_SHA, "condition_counts": condition_counts, "timepoint_counts": time_counts, "control_count": int(obs["is_control"].sum()), "zero_count_cells": int((obs["total_counts"] == 0).sum()), "source_component_n_obs": sum(x[3] for x in SAMPLES), "manifest_source_n_obs": EXPECTED["n_obs"], "source_denominator_parity": True, "all_source_members_consumed": len(inventory) == len(SAMPLES), "excluded_related_ss2_cells": 411, "excluded_related_ss2_reason": "GSE202154 is a distinct Smart-seq2 subseries; frozen SCP1846 denominator equals exactly the twelve GSE201254 matrices"}
    missingness = {"schema_version": "pert-gym.explicit-missingness/v1", "record_id": RECORD_ID, "non_excluding": True, "dependency_created": False, "imputations": 0, "excluded_component_records": 0, "excluded_source_cells": 0, "affected_cells": len(obs), "explicit_all_null_columns": {c: int(obs[c].isna().sum()) for c in ("age", "age_unit", "sex", "cell_type", "source_quality_annotation", "is_low_quality")}, "findings": [{"field_group": "age_sex", "state": "per-cell age and sex are absent from GEO processed matrices", "disposition": "explicit null; no exclusion, imputation, or dependency"}, {"field_group": "cell_type", "state": "SCP per-cell annotations were not published with the immutable GEO matrices", "disposition": "explicit null; all 129441 source cells retained"}, {"field_group": "quality", "state": "processed source matrices define the frozen source-cell universe; deterministic counts, detected genes, mitochondrial and ribosomal fractions were computed without filtering", "disposition": "all source cells retained; no quality rejection"}]}
    ledger = {"schema_version": "pert-gym.proposed-publication-ledger/v2", "submission_id": f"pert-gym-ledger:materialized-build:{TASK_ID}:{RECORD_ID}", "submission_status": "submitted_materialized_build_entry_zero_product_credit", "acceptance_state": "materialized_pending_independent_test_review_and_administrative_acceptance", "accepted_delta_at_build": 0, "accepted_product_credit_pre": 0, "accepted_product_credit_post": 0, "product_credit": 0, "record_id": RECORD_ID, "deduplication_key": f"record_id:{RECORD_ID}", "deduplication_scope_count": 1, "catalogue_row_ids": [ROW], "target_logical_key": LOGICAL_KEY, "wave": WAVE, "global_control_plane": ledger_preflight["global_control_plane"], "record_accounting": {"source_component_records": 1, "included_component_records": 1, "excluded_component_records": 0, "materialized_component_records": 1}, "dataset_accounting": {"source_members": len(SAMPLES), "represented_members": len(inventory), "materialized_triplets": 1, "payload_objects": 3}, "observation_accounting": {"catalogue_source_n_obs_verbatim": EXPECTED["n_obs"], "source_matrix_cells": len(obs), "materialized_cells": len(obs), "excluded_source_cells": 0, "dropped_source_cells": 0}, "quality_missingness_is_not_exclusion": True}
    support_values = {"source-inventory.json": inventory, "quality-readback.json": quality, "missingness.json": missingness, "ledger.json": ledger, "ledger-preflight.json": ledger_preflight}
    for name, value in support_values.items(): (args.output_dir / name).write_bytes(jb(value))
    revision = f"temporal-v4-132-wave13-{hashlib.sha256((script_sha + ''.join(x[2] for x in SAMPLES)).encode()).hexdigest()[:16]}"
    root_prefix = f"{BUCKET_ROOT}/{LOGICAL_KEY}/revisions/{revision}"; dataset_prefix = f"{root_prefix}/datasets/{ACCESSION}"
    fs = fsspec.filesystem("gcs", version_aware=True, project=BILLING, requester_pays=BILLING)
    if fs.exists(root_prefix): raise RuntimeError(f"immutable revision already exists: gs://{root_prefix}")
    objects = []
    for role, path in (("obs", obs_path), ("X", x_path), ("var", var_path)):
        obj = upload(fs, path, f"{dataset_prefix}/{path.name}", role); obj["producer_generation_readback"] = generation_readback(fs, obj, args.output_dir / f"generation-readback-{path.name}"); objects.append(obj)
    support_objects = []
    for name in support_values:
        path = args.output_dir / name; role = name.removesuffix(".json").replace("-", "_")
        obj = upload(fs, path, f"{root_prefix}/{name}", role); obj["producer_generation_readback"] = generation_readback(fs, obj, args.output_dir / f"generation-readback-{name}"); support_objects.append(obj)
    manifest = {"schema_version": "pert-gym.frozen-component-manifest/v1", "task_id": TASK_ID, "record_id": RECORD_ID, "component": COMPONENT, "catalogue_row_ids": [ROW], "target_logical_key": LOGICAL_KEY, "bounded_wave": WAVE, "bounded_wave_assignment": assignment, "bounded_wave_duplicate_check": {"assignment_count": 1, "duplicated_in_other_wave": False}, "source_control": {"manifest_sha256": SOURCE_MANIFEST_SHA, "matched_record_count": 1, "matched_row": row}, "source_identity": {"single_cell_portal": ACCESSION, "geo_superseries": GEO_SUPERSERIES, "geo_component_series": GEO_COMPONENT, "source_members": inventory, "source_members_total_bytes": sum(x[1] for x in SAMPLES)}, "build_identity": {"host": EXPECTED_HOST, "zone": "europe-west1-b", "script_sha256": script_sha, "matrix_semantics": "processed integer count matrices as published", "filtering": "none", "transformations": ["validate exact frozen manifest record and singleton wave assignment", "validate all twelve immutable GEO processed-matrix SHA-256 identities", "transpose each source genes-by-cells matrix and concatenate all source cells", "compute non-filtering quality metrics", "write ordered same-prefix obs/X/var triplet and verify local plus generation-qualified readback"], "lamin_writes": 0, "collection_writes": 0}, "revision": revision, "revision_prefix": f"gs://{root_prefix}", "dataset_count": 1, "triplet_count": 1, "payload_object_count": 3, "observation_count": len(obs), "variable_count": len(var), "shape": list(X.shape), "nnz": int(X.nnz), "raw_count_sum": int(X.sum()), "condition_counts": condition_counts, "timepoint_counts": time_counts, "payload_objects": objects, "support_objects": support_objects, "quality_readback": quality, "missingness": missingness, "ledger": ledger, "manifest_written_last": True, "runtime_seconds_before_manifest": time.time() - started}
    manifest_path = args.output_dir / "manifest.json"; manifest_path.write_bytes(jb(manifest)); manifest_obj = upload(fs, manifest_path, f"{root_prefix}/manifest.json", "manifest")
    if int(manifest_obj["generation"]) <= max(int(x["generation"]) for x in objects + support_objects): raise RuntimeError("manifest was not written last")
    (args.output_dir / "manifest-object.json").write_bytes(jb(manifest_obj)); generation_readback(fs, manifest_obj, args.output_dir / "manifest-readback.json")
    result = {"verdict": "PASS", "record_id": RECORD_ID, "shape": list(X.shape), "nnz": int(X.nnz), "raw_count_sum": int(X.sum()), "manifest_sha256": sha(manifest_path), "manifest_generation_uri": manifest_obj["generation_uri"], "revision": revision, "accepted_product_credit_pre": 0, "accepted_product_credit_post": 0, "lamin_writes": 0, "runtime_seconds": time.time() - started}
    (args.output_dir / "writer-result.json").write_bytes(jb(result)); print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
