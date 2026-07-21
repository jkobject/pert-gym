#!/usr/bin/env python3
"""Append-only source-exhaustive OBS+VAR curation for SchiebingerLander2019."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_fbb1d519"
DATASET_ID = "SchiebingerLander2019"
PREFIX = DATASET_ID
EXPECTED_N_OBS = 327_494
EXPECTED_N_VARS = 27_998
X_UID = "FM4BeGN5qFCdMNNW0000"
X_HASH = "OlaGUd5hIy9w8RhI_B6704"
FROZEN_INPUT_BINDINGS_PATH = Path(__file__).with_name("frozen_inputs") / "bindings.json"
SOURCE_SPECS = (
    {
        "key": "scperturb/records/13350497/files/SchiebingerLander2019_GSE115943.h5ad",
        "uid": "mu3fdERgFC8l58p40000",
        "suffix": "SchiebingerLander2019_GSE115943",
        "source_accession": "GSE115943",
        "n_obs": 259_155,
        "sha256": "393f4a8a171c5aadc38ed37f69bcccd03777d4d4175e5476fa363d49c4277144",
    },
    {
        "key": "scperturb/records/13350497/files/SchiebingerLander2019_GSE106340.h5ad",
        "uid": "bFKFIjLPCPZbAqHL0000",
        "suffix": "SchiebingerLander2019_GSE106340",
        "source_accession": "GSE106340",
        "n_obs": 68_339,
        "sha256": "fcc4da2d1f24566d926e5c61a7bf1dc8a362b7aa4d07e1061434dab2e2845ff9",
    },
)

CANONICAL_OBS_FIELDS = (
    "dataset", "sample", "cell_id", "donor_id", "batch", "cell_type", "cell_line",
    "disease", "tissue_type", "organism", "sex", "age", "ethnicity", "sequencer",
    "technology", "assay", "modality", "media", "is_bulk", "is_pseudobulk",
    "perturbation", "perturbation_type", "perturbation_technology",
    "perturbation_library", "guide_id", "guide_sequence", "perturbation_target",
    "perturbation_target_id", "is_control", "dose", "dose_unit", "timepoint",
    "trajectory_id", "pseudotime", "is_baseline", "sensitivity", "response_metric",
    "response_value", "response_source", "n_counts", "n_genes", "pct_mito", "pct_ribo",
    "is_low_quality", "source", "source_accession", "control_availability", "x_semantics",
)
NOT_APPLICABLE_FIELDS = {
    "perturbation_library", "guide_id", "guide_sequence", "perturbation_target",
    "perturbation_target_id", "sensitivity", "response_metric", "response_value",
    "response_source",
}
UNKNOWN_FIELDS = {
    "donor_id", "cell_line", "sex", "age", "ethnicity", "media", "dose", "dose_unit",
    "pseudotime", "is_low_quality", "x_semantics",
}
PARTIAL_FIELDS = {"perturbation", "perturbation_type", "perturbation_technology", "is_control", "timepoint"}
MAPPING_COUNTS = {
    "mapped_exact_external_gene_name_unique": 23_795,
    "ambiguous_multiple_ensembl_ids": 54,
    "unmapped_symbol": 4_149,
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_values_sha256(values: pd.Index) -> str:
    return sha256_bytes("\n".join(values.astype(str)).encode())


def load_frozen_input_bindings() -> dict[str, Any]:
    manifest = json.loads(FROZEN_INPUT_BINDINGS_PATH.read_text())
    if manifest.get("format") != "pert-gym.frozen-input-bindings/v1":
        raise AssertionError("frozen input format drift")
    root = Path(__file__).parents[5]
    for entry in manifest["inputs"]:
        compressed = (root / entry["binding_path"]).read_bytes()
        if sha256_bytes(compressed) != entry["gzip_sha256"]:
            raise AssertionError("frozen gzip hash drift")
        raw = gzip.decompress(compressed)
        if len(raw) != entry["uncompressed_bytes"] or sha256_bytes(raw) != entry["uncompressed_sha256"]:
            raise AssertionError("frozen input identity drift")
    if len(manifest["inputs"]) != 2:
        raise AssertionError("frozen input coverage drift")
    return manifest


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid), "key": str(artifact.key), "hash": str(artifact.hash),
        "version": str(artifact.version), "size": int(artifact.size),
        "created_at": str(artifact.created_at), "description": str(artifact.description),
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"ordered newest Artifact is not latest: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    if by_uid:
        raise AssertionError(f"duplicate Artifact uid: {value}")
    return latest_artifact(ln, value)[0]


def download_source(ln: Any, spec: dict[str, Any], root: Path) -> tuple[Path, dict[str, Any]]:
    records = list(ln.Artifact.filter(key=spec["key"]).all())
    if len(records) != 1 or str(records[0].uid) != spec["uid"]:
        raise AssertionError(f"source Artifact identity drift: {spec['key']}")
    artifact = records[0]
    url = str(artifact.path)
    if not url.startswith("https://zenodo.org/records/13350497/files/"):
        raise AssertionError(f"source URL drift: {url}")
    path = root / f"{spec['uid']}.h5ad"
    if not path.exists() or path.stat().st_size != int(artifact.size):
        subprocess.run(
            ["curl", "--location", "--fail", "--retry", "3", "--output", str(path), url],
            check=True, timeout=7200,
        )
    if path.stat().st_size != int(artifact.size) or sha256_file(path) != spec["sha256"]:
        raise AssertionError(f"source payload identity drift: {spec['key']}")
    return path, {"artifact": artifact_identity(artifact), "url": url, "sha256": spec["sha256"]}


def load_sources(ln: Any) -> tuple[pd.DataFrame, pd.Index, list[dict[str, Any]]]:
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-schiebinger-sources"
    root.mkdir(parents=True, exist_ok=True)
    frames: list[pd.DataFrame] = []
    var_axes: list[pd.Index] = []
    receipts: list[dict[str, Any]] = []
    for spec in SOURCE_SPECS:
        path, receipt = download_source(ln, spec, root)
        backed = ad.read_h5ad(path, backed="r")
        if (backed.n_obs, backed.n_vars) != (spec["n_obs"], EXPECTED_N_VARS):
            raise AssertionError("source shape drift")
        frame = backed.obs.copy()
        frame.index = pd.Index(frame.index.astype(str) + "-" + spec["suffix"])
        frame["_source_accession"] = spec["source_accession"]
        frames.append(frame)
        var_axes.append(backed.var_names.astype(str).copy())
        receipt.update({"shape": [backed.n_obs, backed.n_vars], "obs_index_sha256": ordered_values_sha256(frame.index), "var_index_sha256": ordered_values_sha256(var_axes[-1])})
        backed.file.close()
        receipts.append(receipt)
    if not var_axes[0].equals(var_axes[1]):
        raise AssertionError("source VAR axes differ")
    source = pd.concat(frames, axis=0, join="outer")
    if len(source) != EXPECTED_N_OBS or not source.index.is_unique:
        raise AssertionError("source OBS union identity drift")
    return source, var_axes[0], receipts


def exact_source_join(obs: pd.DataFrame, source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if "original_obs_index" not in obs:
        raise AssertionError("current OBS lacks original_obs_index")
    identities = pd.Index(obs["original_obs_index"].astype(str))
    if set(identities) != set(source.index) or not identities.is_unique:
        raise AssertionError("source/current cell identity set mismatch")
    joined = source.reindex(identities)
    joined.index = obs.index
    mismatches: dict[str, int] = {}
    for column in source.columns:
        if column == "_source_accession" or column not in obs:
            continue
        left = joined[column].astype("string")
        preserved_aliases = {
            "age": "source_age_label",
            "organism": "source_organism",
            "tissue_type": "source_tissue_type",
            "perturbation_type": "source_original_perturbation_type",
        }
        alias = preserved_aliases.get(str(column))
        comparison_column = alias if alias in obs else column
        right = obs[comparison_column].astype("string")
        equal = (left.isna() & right.isna()) | (left.fillna("") == right.fillna(""))
        mismatches[str(column)] = int((~equal).sum())
    if any(mismatches.values()):
        raise AssertionError(f"source OBS semantic mismatch: {mismatches}")
    return joined, {
        "source_rows": len(source), "current_rows": len(obs), "identity_set_match": True,
        "joined_order_sha256": ordered_values_sha256(pd.Index(identities)),
        "column_mismatches": mismatches, "join_mismatch_count": sum(mismatches.values()),
        "join_semantics": "append exact source filename suffix to source obs_names; set-equal then reindex by original_obs_index",
    }


def source_age_to_minutes(values: pd.Series) -> pd.Series:
    extracted = values.astype("string").str.extract(r"^D(\d+(?:\.\d+)?)$", expand=False)
    return (pd.to_numeric(extracted, errors="coerce") * 24 * 60).astype("Float64")


def set_field(frame: pd.DataFrame, field: str, values: Any, state: Any, source: str) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def curate_obs(obs: pd.DataFrame, source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    original = obs.copy(deep=True)
    joined, join_receipt = exact_source_join(obs, source)
    curated = obs.copy(deep=True)
    source_age = joined["age"].astype("string")
    curated["source_age_label"] = source_age
    curated["source_original_perturbation_type"] = joined[
        "perturbation_type"
    ].astype("string")
    set_field(curated, "dataset", DATASET_ID, "known", "canonical dataset identity")
    set_field(curated, "sample", joined["GSM"].astype("string"), "known", "source H5AD GSM")
    set_field(curated, "cell_id", original["original_obs_index"].astype("string"), "known", "source H5AD normalized cell identity")
    set_field(curated, "batch", joined["GSM"].astype("string"), "known", "GEO sample accession")
    cell_type = pd.Series("MEF-derived reprogramming cell", index=curated.index, dtype="string")
    cell_type.loc[source_age.eq("D0")] = "mouse embryonic fibroblast"
    cell_type.loc[source_age.isin(["iPSC", "iPSCs"])] = "induced pluripotent stem cell"
    set_field(curated, "cell_type", cell_type, "known", "publication time-course design plus source age label")
    set_field(curated, "tissue_type", "cell culture", "known", "publication experimental design")
    set_field(curated, "organism", "Mus musculus", "known", "GEO GSE115943/GSE106340")
    set_field(curated, "disease", "healthy", "known", "source H5AD")
    set_field(curated, "age", pd.Series(pd.NA, index=curated.index, dtype="string"), "unknown", "source age label is experimental time, preserved in source_age_label")
    set_field(curated, "sequencer", "Illumina HiSeq X Ten", "known", "GEO platform GPL21273")
    set_field(curated, "technology", "10x Genomics", "known", "GEO processed 10X payload")
    set_field(curated, "assay", "scRNA-seq", "known", "GEO experiment type")
    set_field(curated, "modality", "scRNA-seq", "known", "GEO experiment type")
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    perturbation = joined["perturbation"].astype("string")
    set_field(curated, "perturbation", perturbation, np.where(perturbation.notna(), "known", "unknown"), "source H5AD")
    perturbation_type = pd.Series(pd.NA, index=curated.index, dtype="string")
    perturbation_type.loc[perturbation.eq("control")] = "none"
    perturbation_type.loc[perturbation.notna() & ~perturbation.eq("control")] = "drug"
    set_field(curated, "perturbation_type", perturbation_type, np.where(perturbation_type.notna(), "known", "unknown"), "source H5AD perturbation")
    technology = pd.Series(pd.NA, index=curated.index, dtype="string")
    technology.loc[perturbation.notna() & ~perturbation.eq("control")] = "chemical induction"
    set_field(curated, "perturbation_technology", technology, np.where(technology.notna(), "known", "unknown"), "publication design")
    controls = pd.Series(pd.NA, index=curated.index, dtype="boolean")
    controls.loc[perturbation.eq("control")] = True
    controls.loc[perturbation.notna() & ~perturbation.eq("control")] = False
    set_field(curated, "is_control", controls, np.where(controls.notna(), "known", "unknown"), "source H5AD perturbation")
    timepoint = source_age_to_minutes(source_age)
    set_field(curated, "timepoint", timepoint, np.where(timepoint.notna(), "known", "unknown"), "source H5AD age label converted days to minutes")
    set_field(curated, "timepoint_unit", "minute", "known", "canonical unit")
    set_field(curated, "trajectory_id", "MEF_to_iPSC_reprogramming", "known", "publication design")
    baseline = source_age.eq("D0").astype("boolean")
    set_field(curated, "is_baseline", baseline, "known", "source H5AD day label")
    set_field(curated, "n_counts", joined["ncounts"], "known", "source H5AD ncounts")
    set_field(curated, "n_genes", joined["ngenes"], "known", "source H5AD ngenes")
    set_field(curated, "pct_mito", joined["percent_mito"], "known", "source H5AD percent_mito")
    set_field(curated, "pct_ribo", joined["percent_ribo"], "known", "source H5AD percent_ribo")
    set_field(curated, "source", "scPerturb", "known", "source Artifact namespace")
    set_field(curated, "source_accession", joined["_source_accession"].astype("string"), "known", "exact source H5AD")
    set_field(curated, "control_availability", "strict_control_available", "known", "source control rows")
    for field in NOT_APPLICABLE_FIELDS:
        set_field(curated, field, pd.Series(pd.NA, index=curated.index, dtype="string"), "not_applicable", "dataset design")
    for field in UNKNOWN_FIELDS - {"age"}:
        dtype = "Float64" if field in {"dose", "pseudotime"} else "string"
        set_field(curated, field, pd.Series(pd.NA, index=curated.index, dtype=dtype), "unknown", "source-exhaustive search found no defensible value")
    if not curated.index.equals(original.index) or len(curated) != EXPECTED_N_OBS:
        raise AssertionError("OBS row order/count drift")
    preserved_names = {
        "GSM", "replicate", "celltype", "ncounts", "ngenes", "percent_mito",
        "percent_ribo", "nperts", "original_obs_index", "obs_uuid",
        "source_replicate", "source_celltype", "source_tissue_type",
        "source_source_tissue_type", "source_organism", "source_cancer",
    }
    preserved = [column for column in original.columns if column in preserved_names]
    assert_frame_equal(curated.loc[:, preserved], original.loc[:, preserved], check_categorical=True)
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    return curated, join_receipt


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        if field in NOT_APPLICABLE_FIELDS:
            disposition = "not_applicable"
        elif field in UNKNOWN_FIELDS:
            disposition = "unknown"
        elif field in PARTIAL_FIELDS:
            disposition = "materialized_partial"
        elif field not in frame:
            disposition = "unknown"
        else:
            disposition = "materialized_complete"
        known = 0 if field not in frame else int(frame[field].notna().sum())
        result[field] = {"disposition": disposition, "materialized": field in frame, "known_rows": known, "unknown_rows": len(frame) - known, "source_bound": disposition.startswith("materialized")}
    if set(result) != set(CANONICAL_OBS_FIELDS):
        raise AssertionError("canonical OBS disposition coverage drift")
    return result


def verify_obs_semantics(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(expected.columns):
        raise AssertionError("OBS schema/order mismatch")
    try:
        assert_frame_equal(actual, expected, check_categorical=True)
    except AssertionError as error:
        raise AssertionError("OBS source semantic mismatch") from error


def curate_var(var: pd.DataFrame) -> pd.DataFrame:
    original = var.copy(deep=True)
    curated = var.copy(deep=True)
    curated["stable_feature_id_namespace"] = "Ensembl stable gene ID"
    curated["organism"] = "Mus musculus"
    assert_frame_equal(curated.loc[:, original.columns], original)
    if not curated.index.equals(original.index):
        raise AssertionError("VAR order drift")
    return curated


def verify_var(var: pd.DataFrame, source_axis: pd.Index, x_axis: pd.Index) -> dict[str, Any]:
    required = {"stable_feature_id", "stable_feature_id_mapping_status", "stable_feature_id_candidate_count", "stable_feature_id_mapping_release", "stable_feature_id_mapping_dataset", "stable_feature_id_mapping_response_sha256", "stable_feature_id_namespace", "organism"}
    if required - set(var.columns):
        raise AssertionError(f"VAR contract absent: {sorted(required - set(var.columns))}")
    if len(var) != EXPECTED_N_VARS or not var.index.equals(source_axis) or not var.index.equals(x_axis):
        raise AssertionError("VAR independent feature-axis count/order drift")
    status = var["stable_feature_id_mapping_status"].astype("string")
    counts = {str(key): int(value) for key, value in status.value_counts().to_dict().items()}
    if counts != MAPPING_COUNTS:
        raise AssertionError(f"VAR mapping counts drift: {counts}")
    stable = var["stable_feature_id"].astype("string")
    mapped = status.eq("mapped_exact_external_gene_name_unique")
    if not stable[mapped].str.fullmatch(r"ENSMUSG\d{11}", na=False).all() or stable[mapped].duplicated().any():
        raise AssertionError("VAR mouse ENSMUSG exact-unique contract drift")
    if stable[~mapped].notna().any():
        raise AssertionError("VAR unresolved symbols must remain null")
    if not var["stable_feature_id_namespace"].astype("string").eq("Ensembl stable gene ID").all():
        raise AssertionError("VAR namespace drift")
    if not var["organism"].astype("string").eq("Mus musculus").all():
        raise AssertionError("VAR organism drift")
    if not var["stable_feature_id_mapping_release"].astype("string").eq("Ensembl 116").all() or not var["stable_feature_id_mapping_dataset"].astype("string").eq("mmusculus_gene_ensembl").all():
        raise AssertionError("VAR Ensembl provenance drift")
    return {"rows": len(var), "mapping_counts": counts, "mapped_exact_unique": int(mapped.sum()), "unresolved_preserved": int((~mapped).sum()), "axis_order_sha256": ordered_values_sha256(var.index), "axis_count_parity": True, "axis_order_parity": True, "organism_values": ["Mus musculus"], "needs_revision": False, "mismatch_count": 0}


def x_axis(artifact: Any) -> tuple[pd.Index, dict[str, Any]]:
    if str(artifact.uid) != X_UID or str(artifact.hash) != X_HASH:
        raise AssertionError("accepted X identity drift")
    path = Path(artifact.cache())
    backed = ad.read_h5ad(path, backed="r")
    if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
        raise AssertionError("X shape drift")
    axis = backed.var_names.astype(str).copy()
    receipt = {"uid": X_UID, "hash": X_HASH, "shape": [backed.n_obs, backed.n_vars], "var_names_sha256": ordered_values_sha256(axis), "backed_only": True}
    backed.file.close()
    return axis, receipt


def collection_membership(ln: Any) -> dict[str, Any]:
    snapshots = {}
    for key in ("pert-gym/additions/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [str(item.uid) for item in members if str(item.key) == f"{PREFIX}/obs.parquet"]
        if matches != ["trSdGyVkTDn5ZkaY0000"]:
            raise AssertionError(f"Collection base member drift: {key}")
        snapshots[key] = {"uid": str(collection.uid), "hash": str(collection.hash), "member_count": len(members), "target_key_matches": matches}
    return snapshots


def verify_current(ln: Any) -> tuple[dict[str, Any], bool]:
    source, source_var_axis, source_receipts = load_sources(ln)
    obs_artifact, obs_history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    x_var_axis, x_receipt = x_axis(x_artifact)
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    curated_obs, join_receipt = curate_obs(obs, source)
    curated_var = curate_var(var)
    obs_curated = str(obs_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive SchiebingerLander2019 OBS"
    ) and "source_original_perturbation_type" in obs
    var_curated = {"stable_feature_id_namespace", "organism"}.issubset(var.columns)
    if obs_curated:
        verify_obs_semantics(obs, curated_obs)
    if var_curated:
        var_verdict = verify_var(var, source_var_axis, x_var_axis)
    else:
        var_verdict = {"needs_revision": True, "missing_columns": sorted({"stable_feature_id_namespace", "organism"} - set(var.columns)), "mapping_counts": var["stable_feature_id_mapping_status"].astype(str).value_counts().to_dict()}
    return {
        "obs_before": artifact_identity(obs_artifact), "obs_history_count": len(obs_history),
        "x": artifact_identity(x_artifact), "x_axis": x_receipt,
        "var_before": artifact_identity(var_artifact), "rows": len(obs),
        "source_objects": source_receipts, "source_join": join_receipt,
        "canonical_field_dispositions": field_dispositions(obs if obs_curated else curated_obs),
        "var_verdict": var_verdict, "already_curated_obs": obs_curated,
        "already_curated_var": var_curated, "curated_obs": curated_obs, "curated_var": curated_var,
        "obs_artifact": obs_artifact, "x_artifact": x_artifact, "var_artifact": var_artifact,
        "source_var_axis": source_var_axis, "x_var_axis": x_var_axis,
    }, obs_curated and var_curated


def publish(ln: Any, result: dict[str, Any], helper_sha256: str) -> dict[str, list[Any]]:
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-schiebinger-publish-"))
    ln.track(key=f"pert-gym/real-dataset-curation/{DATASET_ID}/{TASK_ID}", kind="script", params={"task_id": TASK_ID, "helper_sha256": helper_sha256}, new_run=True, pypackages=False, stream_tracking=False)
    if not result["already_curated_var"]:
        path = root / "var.parquet"
        result["curated_var"].to_parquet(path)
        var = ln.Artifact.from_dataframe(path, key=f"{PREFIX}/var.parquet", revises=result["var_artifact"], description=f"{TASK_ID}: SchiebingerLander2019 mouse VAR species/namespace annotation; preserves 23795 exact unique ENSMUSG, 54 ambiguous and 4149 unmapped symbols").save()
        result["x_artifact"].features.set_values({"var": var})
        writes["var"].append(var)
    if not result["already_curated_obs"]:
        path = root / "obs.parquet"
        result["curated_obs"].to_parquet(path)
        obs = ln.Artifact.from_dataframe(path, key=f"{PREFIX}/obs.parquet", revises=result["obs_artifact"], description=f"{TASK_ID}: source-exhaustive SchiebingerLander2019 OBS; exact two-H5AD cell join; GEO/paper temporal semantics; sources=GSE115943,GSE106340").save()
        obs.features.set_values({"X": result["x_artifact"]})
        writes["obs"].append(obs)
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()
    return writes


def strip_runtime(result: dict[str, Any]) -> dict[str, Any]:
    hidden = {"curated_obs", "curated_var", "obs_artifact", "x_artifact", "var_artifact", "source_var_axis", "x_var_axis"}
    return {key: value for key, value in result.items() if key not in hidden}


def emit_product(phase: str, current: int) -> None:
    print("PRODUCT_EXECUTION=" + canonical({"product_execution": {"host": os.uname().nodename, "pid": os.getpid(), "phase": phase, "payload_heartbeat_at": int(time.time()), "metric": "real_dataset_obs_var", "current": current, "denominator": 1, "unit": "biological_dataset"}}), flush=True)


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    helper_sha256 = sha256_file(Path(__file__))
    frozen = load_frozen_input_bindings()
    capacity = preflight()
    emit_product("preflight", 0)
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    result, all_curated = verify_current(ln)
    collections_before = collection_membership(ln)
    counts_before = {"artifacts": ln.Artifact.filter().count(), "collections": ln.Collection.filter().count()}
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
    if mode == "mutate" and not all_curated:
        metadata = {"run_id": TASK_ID, "pid": os.getpid(), "host": capacity.hostname, "project": capacity.project, "zone": capacity.zone, "branch": ln.setup.settings.branch.name, "started_at": time.time()}
        with ExitStack() as stack:
            stack.enter_context(lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity))
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh, fresh_all = verify_current(ln)
            if fresh_all:
                result, all_curated = fresh, True
            else:
                result = fresh
                writes = publish(ln, fresh, helper_sha256)
    elif mode == "verify" and not all_curated:
        raise AssertionError("verify requested before exact OBS+VAR revisions exist")
    final, final_all = verify_current(ln)
    if mode in {"mutate", "verify"} and not final_all:
        raise AssertionError("terminal OBS+VAR readback failed")
    collections_after = collection_membership(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    counts_after = {"artifacts": ln.Artifact.filter().count(), "collections": ln.Collection.filter().count()}
    write_receipts = {role: [artifact_identity(item) for item in items] for role, items in writes.items()}
    receipt = {
        "format": "pert-gym.real-dataset-obs-var-curation/v2", "task_id": TASK_ID,
        "dataset_id": DATASET_ID, "status": "PASS", "mode": mode,
        "helper_sha256": helper_sha256, "frozen_inputs": frozen["inputs"],
        "source_denominator": {"biological_datasets": 1, "logical_families": 1, "physical_members": 1, "observations": EXPECTED_N_OBS, "features": EXPECTED_N_VARS},
        "member_before": strip_runtime(result), "member_after": strip_runtime(final),
        "collections": collections_after,
        "writes": {"obs_revisions": len(writes["obs"]), "var_revisions": len(writes["var"]), "x_revisions": 0, "collection_writes": 0, "deletions": 0, "artifacts": write_receipts},
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {"hostname": capacity.hostname, "available_memory_bytes": capacity.available_memory_bytes, "free_disk_bytes": capacity.free_disk_bytes},
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    emit_product("checkpointing", 1)
    print("SCHIEBINGER_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
