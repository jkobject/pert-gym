#!/usr/bin/env python3
"""Tester-authored read-only real-consumer QA for PerturBase GSE107185."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import socket
import subprocess
import tarfile
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
import zarr
from scipy import sparse

TASK_ID = "t_51cc7265"
BUILD_TASK_ID = "t_fe451c4d"
REVIEW_TASK_ID = "t_f196b29f"
RECORD_ID = "temporal_v4_116_perturbase_gse107185"
LOGICAL_KEY = "pert-gym/logical/temporal/perturbase_gse107185"
COMPONENT = "Mapping Cellular Reprogramming via Pooled Overexpression Screens with Paired Fitness and Single Cell RNA-Sequencing Readout"
MANIFEST_URI = "gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/perturbase_gse107185/revisions/perturbase-gse107185-20260717T045017Z-8821ce13/manifest.json#1784263868472118"
MANIFEST_SHA = "87244217d705a74589aad4d28e6e41ed400ddda443aa02f470c706ff7f78893a"
FROZEN_SHA = "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
GRAPH_SHA = "59c18752f65257270b980353811da5bf554d5ac2b6c11c550a63849664ce9c98"
SOURCE_URL = "http://www.perturbase.cn/static/extend_61/extend_61.filter.tar.gz"
SOURCE_SHA = "e5d2fa6f7a3c3faced2649e53a5226a41e4b93b6278b375e5f09346739e0bbfa"
SOURCE_MEMBER = "mixscape_hvg_filter.h5ad"
SOURCE_MEMBER_SHA = "e8e19bf30b6b028d9bb257907f9bb4a0747478c6b5af24405df2a8da15e18e75"
EXPECTED_SHAPE = (8428, 2000)
EXPECTED_NNZ = 6377510
BILLING = "jkobject-1549353370965"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, check=True, text=True, capture_output=True).stdout


def gcs_cp(uri: str, destination: Path) -> None:
    run(["gcloud", "storage", "cp", f"--billing-project={BILLING}", uri, str(destination)])


def read_matrix(path: Path) -> sparse.csr_matrix:
    store = zarr.storage.ZipStore(str(path), mode="r")
    try:
        group = zarr.open_group(store=store, mode="r")
        assert group.attrs["format"] == "csr_matrix"
        matrix = sparse.csr_matrix(
            (np.asarray(group["data"]), np.asarray(group["indices"]), np.asarray(group["indptr"])),
            shape=tuple(group.attrs["shape"]),
        )
        assert int(group.attrs["nnz"]) == matrix.nnz
        assert group.attrs["dtype"] == str(matrix.dtype)
        return matrix
    finally:
        store.close()


def array_sha(value: np.ndarray) -> str:
    value = np.ascontiguousarray(value)
    h = hashlib.sha256()
    h.update(str(value.dtype).encode())
    h.update(json.dumps(list(value.shape)).encode())
    h.update(value.tobytes())
    return h.hexdigest()


def matrix_identity(matrix: sparse.csr_matrix) -> dict[str, Any]:
    matrix.sum_duplicates()
    matrix.sort_indices()
    return {
        "format": "csr_matrix",
        "shape": list(matrix.shape),
        "nnz": int(matrix.nnz),
        "dtype": str(matrix.dtype),
        "sum": float(matrix.sum(dtype=np.float64)),
        "minimum": float(matrix.data.min()),
        "maximum": float(matrix.data.max()),
        "negative_values": int((matrix.data < 0).sum()),
        "data_sha256": array_sha(matrix.data),
        "indices_sha256": array_sha(matrix.indices),
        "indptr_sha256": array_sha(matrix.indptr),
    }


def normalized_source_series(series: pd.Series) -> pd.Series:
    if isinstance(series.dtype, pd.CategoricalDtype) or (
        series.dtype == object and series.dropna().map(lambda x: isinstance(x, str)).all()
    ):
        return series.astype("string")
    return series


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--upstream-source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, observed: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "observed": observed})

    frozen_path = args.input / "downloadable_logical_publication_manifest_20260713.json"
    graph_path = args.input / "kanban_graph_compaction_t_36a3533e_manifest.json"
    frozen_hash, graph_hash = sha(frozen_path), sha(graph_path)
    check("frozen_manifest_checksum", frozen_hash == FROZEN_SHA, frozen_hash)
    check("bounded_wave_graph_checksum", graph_hash == GRAPH_SHA, graph_hash)
    frozen = json.loads(frozen_path.read_text())
    records = [r for r in frozen["records"] if r.get("record_id") == RECORD_ID]
    record_ok = len(records) == 1 and {
        "catalogue_row_ids": records[0].get("catalogue_row_ids"),
        "component": records[0].get("component"),
        "target_logical_key": records[0].get("target_logical_key"),
        "source_n_obs": records[0].get("source_n_obs"),
        "classification": records[0].get("classification"),
    } == {
        "catalogue_row_ids": [116],
        "component": COMPONENT,
        "target_logical_key": LOGICAL_KEY,
        "source_n_obs": 8428,
        "classification": "executable",
    }
    check("frozen_scope_exactly_row_116_component", record_ok, records)
    graph = json.loads(graph_path.read_text())
    assignments = [a for a in graph["component_assignments"] if a.get("record_id") == RECORD_ID]
    check(
        "exactly_one_bounded_wave",
        len(assignments) == 1 and assignments[0].get("wave") == 11 and assignments[0].get("outcome_task_id") == REVIEW_TASK_ID,
        assignments,
    )

    with tempfile.TemporaryDirectory(prefix="qa-gse107185-") as tmp_name:
        tmp = Path(tmp_name)
        manifest_path = tmp / "manifest.json"
        gcs_cp(MANIFEST_URI, manifest_path)
        manifest_size = manifest_path.stat().st_size
        manifest_hash = sha(manifest_path)
        check("manifest_generation_checksum", manifest_hash == MANIFEST_SHA, manifest_hash)
        manifest = json.loads(manifest_path.read_text())
        check(
            "manifest_identity_and_scope",
            manifest["record_id"] == RECORD_ID
            and manifest["target_logical_key"] == LOGICAL_KEY
            and manifest["catalogue_row_ids"] == [116]
            and manifest["component"] == COMPONENT
            and manifest["dataset_count"] == 1
            and manifest["triplet_count"] == 1
            and manifest["dataset"]["prefix"].endswith("/datasets/extend_61"),
            {k: manifest.get(k) for k in ("record_id", "target_logical_key", "catalogue_row_ids", "component", "dataset_count", "triplet_count")},
        )
        check(
            "manifest_controls_match_independent_files",
            manifest["source_identity"]["controlling_publication_manifest_sha256"] == frozen_hash
            and manifest["source_identity"]["bounded_wave_graph_sha256"] == graph_hash
            and manifest["source_identity"]["controlling_record"] == records[0],
            manifest["source_identity"],
        )
        check(
            "manifest_single_wave",
            manifest["bounded_wave"] == {"assignment_count": 1, "id": "publication-wave-11-of-13", "number": 11, "outcome_task_id": REVIEW_TASK_ID, "singular": True}
            and manifest["bounded_wave_duplicate_check"] == {"assignment_count": 1, "duplicated_in_other_wave": False},
            {"wave": manifest["bounded_wave"], "duplicate_check": manifest["bounded_wave_duplicate_check"]},
        )

        objects = manifest["actual_artifact_inventory"]
        roles = {o["role"]: o for o in objects}
        expected_roles = {"canonical_obs", "canonical_X_processed_mixscape_hvg", "canonical_var"}
        check("one_exact_triplet_inventory", len(objects) == 3 and set(roles) == expected_roles, sorted(roles))
        identities: dict[str, Any] = {}
        local_paths = {
            "canonical_obs": tmp / "obs.parquet",
            "canonical_X_processed_mixscape_hvg": tmp / "X.zarr.zip",
            "canonical_var": tmp / "var.parquet",
        }
        for role, path in local_paths.items():
            obj = roles[role]
            gcs_cp(obj["generation_uri"], path)
            identities[role] = {"generation_uri": obj["generation_uri"], "size_bytes": path.stat().st_size, "sha256": sha(path)}
        check(
            "all_payload_generation_size_sha256",
            all(identities[r]["size_bytes"] == roles[r]["size_bytes"] and identities[r]["sha256"] == roles[r]["sha256"] for r in roles),
            identities,
        )

        ledger_path = tmp / "ledger.json"
        ledger_obj = manifest["ledger_object"]
        gcs_cp(ledger_obj["generation_uri"], ledger_path)
        ledger_hash = sha(ledger_path)
        ledger = json.loads(ledger_path.read_text())
        check("ledger_generation_size_sha256", ledger_hash == ledger_obj["sha256"] and ledger_path.stat().st_size == ledger_obj["size_bytes"], {"sha256": ledger_hash, "size_bytes": ledger_path.stat().st_size})
        check("manifest_embeds_exact_ledger", ledger == manifest["ledger"], ledger)
        rb = ledger["record_accounting"]
        db = ledger["dataset_accounting"]
        ob = ledger["observation_accounting"]
        ledger_ok = (
            rb == {"source_component_records": 1, "included_component_records": 1, "excluded_component_records": 0, "materialized_component_records": 1}
            and db == {"source_datasets": 1, "included_datasets": 1, "excluded_datasets": 0, "materialized_triplets": 1}
            and ob == {"catalogue_reported_n_obs": 8428, "resolved_eligible_n_obs": 8428, "materialized_n_obs": 8428, "excluded_n_obs": 0, "dropped_n_obs": 0}
            and ledger["debits"] == {"exclusions": 0, "dropped_observations": 0, "product_credit": 0}
            and ledger["credits"] == {"materialized_component_records": 1, "materialized_triplets": 1, "materialized_observations": 8428}
            and ledger["accepted_delta_at_build"] == 0
            and ledger["deduplication_scope_count"] == 1
            and ledger["quality_missingness_is_not_exclusion"] is True
        )
        check("proposed_ledger_balanced_zero_credit", ledger_ok, ledger)

        revision_prefix = manifest["revision_prefix"]
        listed = sorted(x for x in run(["gcloud", "storage", "ls", f"--billing-project={BILLING}", f"{revision_prefix}/**"]).splitlines() if x)
        suffixes = {x.removeprefix(revision_prefix) for x in listed}
        expected_suffixes = {"/datasets/extend_61/X.zarr.zip", "/datasets/extend_61/obs.parquet", "/datasets/extend_61/var.parquet", "/ledger.json", "/manifest.json", "/source/extend_61.filter.tar.gz"}
        check("complete_revision_inventory_six_objects", suffixes == expected_suffixes, listed)
        described = []
        for uri in listed:
            described.append(json.loads(run(["gcloud", "storage", "objects", "describe", uri, f"--billing-project={BILLING}", "--format=json"])))
        generations = {d["name"]: str(d["generation"]) for d in described}
        pinned = [manifest["source_object"], *objects, ledger_obj, {"uri": MANIFEST_URI.split("#")[0], "generation": MANIFEST_URI.split("#")[1]}]
        check("live_objects_still_equal_pinned_generations", all(generations[o["uri"].removeprefix("gs://scperturb/")] == str(o["generation"]) for o in pinned), generations)
        def parse_gcs_time(value: str) -> datetime:
            value = value.replace("Z", "+00:00")
            if len(value) >= 5 and value[-5] in "+-" and value[-3] != ":":
                value = f"{value[:-2]}:{value[-2:]}"
            return datetime.fromisoformat(value)

        update_times = {d["name"]: parse_gcs_time(d.get("update_time") or d.get("updateTime")) for d in described}
        manifest_name = MANIFEST_URI.split("#")[0].removeprefix("gs://scperturb/")
        check("manifest_written_last", manifest["manifest_last"] is True and update_times[manifest_name] == max(update_times.values()), {k: v.isoformat() for k, v in update_times.items()})

        source_gcs = tmp / "source-gcs.tar.gz"
        gcs_cp(manifest["source_object"]["generation_uri"], source_gcs)
        check("frozen_source_generation_checksum", sha(source_gcs) == SOURCE_SHA and source_gcs.stat().st_size == manifest["source_object"]["size_bytes"], {"sha256": sha(source_gcs), "size_bytes": source_gcs.stat().st_size})
        source_upstream = tmp / "source-upstream.tar.gz"
        if args.upstream_source:
            shutil.copy2(args.upstream_source, source_upstream)
        else:
            shutil.copy2(source_gcs, source_upstream)
        check("source_bytes_match_declared_identity", sha(source_upstream) == SOURCE_SHA and source_upstream.stat().st_size == source_gcs.stat().st_size, {"sha256": sha(source_upstream), "size_bytes": source_upstream.stat().st_size, "independent_upstream_copy_supplied": bool(args.upstream_source)})
        source_h5ad = tmp / SOURCE_MEMBER
        with tarfile.open(source_upstream, "r:gz") as tf:
            members = tf.getmembers()
            member_ok = len(members) == 1 and members[0].name == SOURCE_MEMBER
            extracted = tf.extractfile(members[0]) if member_ok else None
            if extracted is not None:
                with extracted, source_h5ad.open("wb") as target:
                    shutil.copyfileobj(extracted, target, 8 * 1024 * 1024)
        check("source_archive_exact_member", member_ok and sha(source_h5ad) == SOURCE_MEMBER_SHA, {"members": [(m.name, m.size) for m in members], "member_sha256": sha(source_h5ad)})

        obs = pd.read_parquet(local_paths["canonical_obs"])
        var = pd.read_parquet(local_paths["canonical_var"])
        matrix = read_matrix(local_paths["canonical_X_processed_mixscape_hvg"])
        source = ad.read_h5ad(source_h5ad, backed="r")
        try:
            check("real_readback_shape_nnz", tuple(matrix.shape) == EXPECTED_SHAPE and matrix.nnz == EXPECTED_NNZ and len(obs) == 8428 and len(var) == 2000, {"shape": list(matrix.shape), "nnz": matrix.nnz, "obs": len(obs), "var": len(var)})
            check("ordered_unique_axes_equal_source", obs.index.is_unique and var.index.is_unique and obs.index.tolist() == source.obs_names.astype(str).tolist() and var.index.tolist() == source.var_names.astype(str).tolist(), {"obs_unique": obs.index.is_unique, "var_unique": var.index.is_unique})
            source_matrix = sparse.csr_matrix(np.asarray(source.X[:, :], dtype=np.float32))
            source_matrix.sum_duplicates()
            source_matrix.sort_indices()
            observed_matrix_identity = matrix_identity(matrix)
            check("matrix_value_exact_source_parity", observed_matrix_identity == matrix_identity(source_matrix) == manifest["dataset"]["X"], observed_matrix_identity)
            source_obs_parity = all(normalized_source_series(source.obs[c].copy()).reset_index(drop=True).equals(obs[c].reset_index(drop=True)) for c in source.obs.columns)
            source_var_parity = all(normalized_source_series(source.var[c].copy()).reset_index(drop=True).equals(var[c].reset_index(drop=True)) for c in source.var.columns)
            check("source_obs_columns_value_exact_after_declared_normalization", source_obs_parity, list(source.obs.columns))
            check("source_var_columns_value_exact_after_declared_normalization", source_var_parity, list(source.var.columns))
        finally:
            source.file.close()

        metadata_checks = {
            "dataset": set(obs["dataset"].dropna().unique()) == {LOGICAL_KEY},
            "source_component": set(obs["source_component"].dropna().unique()) == {"extend_61"},
            "source_accession": set(obs["source_accession"].dropna().unique()) == {"GSE107185"},
            "source_bioproject": set(obs["source_bioproject"].dropna().unique()) == {"PRJNA419230"},
            "cell_id": obs["cell_id"].tolist() == obs.index.astype(str).tolist(),
            "perturbation": obs["perturbation"].astype(str).tolist() == obs["gene"].astype(str).tolist(),
            "is_control": obs["is_control"].astype(bool).tolist() == obs["gene"].eq("CTRL").astype(bool).tolist(),
            "perturbation_count": obs["perturbation"].nunique() == 61,
        }
        check("enriched_metadata_content_integrity", all(metadata_checks.values()), {"subchecks": metadata_checks, "datasets": obs["dataset"].unique().tolist(), "components": obs["source_component"].unique().tolist(), "perturbations": obs["perturbation"].nunique()})
        missing_cols = {"age": 8428, "donor_id": 8428, "sex": 8428, "ethnicity": 8428, "timepoint": 8428, "is_low_quality": 8428}
        reasons = ["age_missingness_reason", "donor_id_missingness_reason", "sex_missingness_reason", "ethnicity_missingness_reason", "timepoint_missingness_reason", "quality_missingness_reason"]
        missing_ok = all(int(obs[c].isna().sum()) == n for c, n in missing_cols.items()) and all(obs[c].notna().all() and obs[c].astype(str).str.len().gt(0).all() for c in reasons)
        check("missing_metadata_explicit_non_excluding", missing_ok and manifest["missingness"]["non_excluding"] is True and manifest["missingness"]["excluded_observations"] == 0, {"null_counts": {c: int(obs[c].isna().sum()) for c in missing_cols}, "manifest_missingness": manifest["missingness"]})
        check("var_ensembl_missingness_reported", int(var["gene_id"].isna().sum()) == 361 and manifest["missingness"]["var_ensembl_missing_count"] == 361, {"gene_id_missing": int(var["gene_id"].isna().sum())})

    failed = [c["name"] for c in checks if not c["passed"]]
    stable = {
        "manifest_sha256": manifest_hash,
        "source_sha256": SOURCE_SHA,
        "source_member_sha256": SOURCE_MEMBER_SHA,
        "output_sha256": {r: identities[r]["sha256"] for r in sorted(identities)},
        "ledger_sha256": ledger_hash,
        "matrix_identity": observed_matrix_identity,
        "counts": {"datasets": 1, "triplets": 1, "observations": len(obs), "variables": len(var), "nnz": int(matrix.nnz), "perturbations": int(obs["perturbation"].nunique())},
        "checks": {c["name"]: c["passed"] for c in checks},
    }
    stable_fingerprint = hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    result = {
        "schema_version": "pert-gym.independent-consumer-qa/v1",
        "task_id": TASK_ID,
        "build_task_id": BUILD_TASK_ID,
        "review_task_id": REVIEW_TASK_ID,
        "record_id": RECORD_ID,
        "target_logical_key": LOGICAL_KEY,
        "host": socket.gethostname(),
        "platform": "GCE pert-gym-worker-eu, europe-west1-b; real Python consumer via gcloud, pandas, zarr, scipy, anndata",
        "mode": "read-only exact-generation GCS payloads including frozen source archive; no mocks/testMode; tester-authored",
        "upstream_source_note": "PerturBase origin advertised the expected 65,772,471-byte object but delivered only about 10 KB/s during QA, so the aborted direct refresh was not used as evidence. Reproducibility was exercised from the immutable generation-pinned source archive, including member hash and value-exact source-to-output parity.",
        "command": "cd ~/work/pert-gym && uv run python /tmp/gse107185-qa-t_51cc7265/qa_gse107185.py --input /tmp/gse107185-qa-t_51cc7265/input --output <result.json>",
        "executed_script_sha256": sha(Path(__file__)),
        "runtime_seconds": time.monotonic() - started,
        "checks_total": len(checks),
        "checks_passed": len(checks) - len(failed),
        "checks_failed": len(failed),
        "failed_checks": failed,
        "verdict": "PASS" if not failed else "FAIL",
        "stable_reproducibility_fingerprint": stable_fingerprint,
        "bytes_streamed_from_gcs": sum(v["size_bytes"] for v in identities.values()) + ledger_obj["size_bytes"] + manifest_size + manifest["source_object"]["size_bytes"],
        "bytes_streamed_from_upstream_source": 0,
        "stable_evidence": stable,
        "quality_findings": {
            "non_excluding": True,
            "missing_age_donor_sex_ethnicity_timepoint_quality_rows_each": 8428,
            "missing_var_ensembl_identifiers": 361,
        },
        "recommendation": "PASS: advance to terminal reviewer; retain zero product credit until reviewer and administrative acceptance." if not failed else "FAIL: return to implementation owner before terminal review.",
        "checks": checks,
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: result[k] for k in ("verdict", "checks_total", "checks_passed", "checks_failed", "failed_checks", "stable_reproducibility_fingerprint", "bytes_streamed_from_gcs", "bytes_streamed_from_upstream_source", "runtime_seconds", "executed_script_sha256")}, indent=2, sort_keys=True))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
