#!/usr/bin/env python3
"""Curate and publish C. elegans embryogenesis OBS/VAR completion revisions.

The accepted sparse X artifact and Collection memberships are structural anchors and are
never rewritten. Live execution is restricted to the EU worker and the jkobject branch.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
import time
import uuid
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_a62edaa7"
DATASET_ID = "temporal/c_elegans_embryogenesis"
LOGICAL_KEY = "pert-gym/logical/temporal/c_elegans_embryogenesis"
SOURCE_URL = "https://ndownloader.figshare.com/files/39943585"
SOURCE_SIZE = 365_498_906
SOURCE_MD5 = "c3a37ca238921fcec7bd5e9faa6118f1"
CANONICAL_PREFIX = "gs://scperturb/data/cleaned/c_elegans_embryogenesis"
SUCCESSOR_COLLECTION_KEY = "pert-gym/additions/20260802-c-elegans-embryogenesis-e2e"
EXPECTED = {
    "obs": {
        "uid": "5EOAVNZfpqU7u1TX0000",
        "sha256": "ac36e96828a9d30ce69e95fc5fcea2d471f5cfa1e7482d4d3ae74fe90703f957",
        "size": 2_177_270,
        "key": "data/cleaned/c_elegans_embryogenesis/obs.parquet",
    },
    "X": {
        "uid": "BcReMLnW6XOb8tvS0000",
        "sha256": "50b28ef17d02bcdf670fd2552ed0129645bd66bf8d4a7d97fc3ef6308efb360c",
        "size": 354_987_544,
        "key": "data/cleaned/c_elegans_embryogenesis/X.h5ad",
    },
    "var": {
        "uid": "7sZcoxxd0LMj1DeI0000",
        "sha256": "7e31d2f5a962b14e11ce2b697121e18b8cd61156d832ff0b157fd6884bb00ade",
        "size": 850_046,
        "key": "data/cleaned/c_elegans_embryogenesis/var.parquet",
    },
}
EXPECTED_SHAPE = (46_151, 20_222)
EXPECTED_NNZ = 43_994_050
EXPECTED_MATRIX_ARRAYS = {
    "data": "04bb0298f73b227c54dc3ae40c407ad22f3dff3ee04526e7c1e77020afddd1c0",
    "indices": "6a4bc363dbc2184a50fc0edb15f1e702a3b05661bfb36fec07f0f09307f41478",
    "indptr": "f8a94f8f2f04d328d3ec601e5f8ef642df6fb041a59de616c5a2d083c9ea2c8c",
}
WORMBASE_GENE_RE = re.compile(r"^WBGene\d{8}$")
UUID_NAMESPACE = uuid.UUID("5fce87f5-d398-4c68-8233-dcc6c4dd4b3e")

CANONICAL_OBS_FIELDS = (
    "dataset",
    "sample",
    "cell_id",
    "donor_id",
    "batch",
    "cell_type",
    "cell_line",
    "disease",
    "tissue_type",
    "organism",
    "sex",
    "age",
    "ethnicity",
    "sequencer",
    "technology",
    "assay",
    "modality",
    "media",
    "is_bulk",
    "is_pseudobulk",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "perturbation_library",
    "guide_sequence",
    "molecule_sequence",
    "is_control",
    "dose",
    "dose_unit",
    "timepoint",
    "trajectory_id",
    "pseudotime",
    "is_baseline",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
    "n_counts",
    "n_genes",
    "pct_mito",
    "pct_ribo",
    "is_low_quality",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _md5(path: Path) -> str:
    digest = hashlib.md5()  # noqa: S324 - immutable source identity, not security
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def matrix_receipt(path: Path) -> dict[str, Any]:
    """Hash every stored sparse X array without materializing the matrix."""
    receipt: dict[str, Any] = {"arrays": {}}
    with h5py.File(path, "r") as handle:
        matrix = handle["X"]
        if not isinstance(matrix, h5py.Group):
            raise AssertionError("X is not a sparse HDF5 group")
        encoding_type = matrix.attrs.get("encoding-type", "")
        if isinstance(encoding_type, bytes):
            encoding_type = encoding_type.decode()
        receipt["encoding_type"] = str(encoding_type)
        receipt["shape"] = list(map(int, matrix.attrs["shape"]))
        for name in ("data", "indices", "indptr"):
            dataset = matrix[name]
            digest = hashlib.sha256()
            step = max(1, 8_000_000 // max(1, dataset.dtype.itemsize))
            for start in range(0, len(dataset), step):
                digest.update(dataset[start : start + step].tobytes(order="C"))
            receipt["arrays"][name] = {
                "sha256": digest.hexdigest(),
                "dtype": str(dataset.dtype),
                "length": len(dataset),
            }
    receipt["nnz"] = receipt["arrays"]["data"]["length"]
    return receipt


def _state_source(frame: pd.DataFrame, field: str, state: str, source: str) -> None:
    frame[f"{field}__state"] = state
    frame[f"{field}__source"] = source


def _unknown(frame: pd.DataFrame, field: str, source: str) -> None:
    frame[field] = pd.Series(pd.NA, index=frame.index, dtype="string")
    _state_source(frame, field, "unknown", source)


def _not_applicable(frame: pd.DataFrame, field: str, source: str) -> None:
    frame[field] = pd.Series(pd.NA, index=frame.index, dtype="string")
    _state_source(frame, field, "not_applicable", source)


def _known(frame: pd.DataFrame, field: str, values: Any, source: str) -> None:
    if isinstance(values, pd.Series):
        frame[field] = values.to_numpy()
    else:
        frame[field] = values
    _state_source(frame, field, "known", source)


def curate_obs(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Retain raw columns and materialize canonical fields without invention."""
    required_raw = {
        "batch",
        "cell",
        "cell.subtype",
        "cell.type",
        "plot.cell.type",
        "lineage",
        "embryo.time",
        "pseudotime",
        "timepoint",
        "trajectory_id",
        "is_baseline",
        "n.umi",
        "passed_initial_QC_or_later_whitelisted",
        "organism",
    }
    missing = sorted(required_raw - set(raw.columns))
    if missing:
        raise AssertionError(f"accepted obs is missing source fields: {missing}")
    if len(raw) != EXPECTED_SHAPE[0] or not raw.index.is_unique:
        raise AssertionError("accepted obs row identity is invalid")

    result = raw.copy()
    original_index = raw.index.astype(str)
    result["original_obs_index"] = original_index
    result["obs_uuid"] = [
        str(uuid.uuid5(UUID_NAMESPACE, f"{DATASET_ID}:{value}"))
        for value in original_index
    ]
    _known(result, "dataset", DATASET_ID, "accepted dataset identity")
    _known(result, "sample", raw["batch"].astype("string"), "source obs.batch")
    _known(result, "cell_id", raw["cell"].astype("string"), "source obs.cell")
    _not_applicable(
        result, "donor_id", "pooled C. elegans embryos; donor concept not applicable"
    )
    _known(result, "batch", raw["batch"].astype("string"), "source obs.batch")
    cell_type = (
        raw["cell.subtype"]
        .astype("string")
        .fillna(raw["cell.type"].astype("string"))
        .fillna(raw["plot.cell.type"].astype("string"))
        .fillna(raw["lineage"].astype("string"))
    )
    _known(
        result,
        "cell_type",
        cell_type,
        "source obs cell.subtype > cell.type > plot.cell.type > lineage fallback",
    )
    _not_applicable(result, "cell_line", "whole-organism embryo atlas; no cell line")
    _not_applicable(
        result, "disease", "developmental reference atlas; disease not applicable"
    )
    _known(
        result,
        "tissue_type",
        "whole embryo",
        "Figshare article 22491340 and Packer 2019",
    )
    _known(result, "organism", raw["organism"].astype("string"), "source obs.organism")
    _unknown(result, "sex", "not reported by Figshare article 22491340 or source obs")
    _known(
        result,
        "age",
        raw["embryo.time"].astype("Float64").astype("string")
        + " min post-fertilization",
        "source obs.embryo.time",
    )
    _not_applicable(result, "ethnicity", "non-human organism")
    _unknown(
        result, "sequencer", "sequencing instrument not reported in retained source"
    )
    _known(
        result,
        "technology",
        "10x Genomics Chromium 3' v2",
        "Figshare article 22491340 reports 10x Genomics v2 chemistry",
    )
    _known(result, "assay", "single-cell RNA sequencing", "Figshare article 22491340")
    _known(result, "modality", raw["modality"].astype("string"), "source obs.modality")
    _unknown(result, "media", "embryo handling medium not reported in retained source")
    _known(result, "is_bulk", False, "Figshare article 22491340: scRNA-seq")
    _known(result, "is_pseudobulk", False, "one source cell per observation")
    for field in (
        "perturbation",
        "perturbation_type",
        "perturbation_technology",
        "perturbation_library",
        "guide_sequence",
        "molecule_sequence",
        "dose",
        "dose_unit",
    ):
        _not_applicable(
            result, field, "observational embryogenesis time series; no perturbation"
        )
    _known(
        result,
        "is_control",
        False,
        "observational time series; no perturbation/control arm",
    )
    _known(
        result,
        "timepoint",
        raw["timepoint"].astype("Float64"),
        "source obs.timepoint in minutes",
    )
    _known(
        result,
        "trajectory_id",
        raw["trajectory_id"].astype("string"),
        "source obs.trajectory_id",
    )
    _known(
        result,
        "pseudotime",
        raw["pseudotime"].astype("Float64"),
        "source obs.pseudotime",
    )
    _known(
        result, "is_baseline", raw["is_baseline"].astype(bool), "source obs.is_baseline"
    )
    for field in (
        "sensitivity",
        "response_metric",
        "response_value",
        "response_source",
    ):
        _not_applicable(result, field, "no perturbation-response endpoint")
    _known(result, "n_counts", raw["n.umi"].astype("Int64"), "source obs.n.umi")
    _unknown(
        result,
        "n_genes",
        "per-cell detected-gene counts absent from retained source obs",
    )
    _unknown(
        result,
        "pct_mito",
        "per-cell mitochondrial fraction absent from retained source obs",
    )
    _unknown(
        result,
        "pct_ribo",
        "per-cell ribosomal fraction absent from retained source obs",
    )
    _known(
        result,
        "is_low_quality",
        ~raw["passed_initial_QC_or_later_whitelisted"].astype(bool),
        "inverse source obs.passed_initial_QC_or_later_whitelisted",
    )
    _known(result, "source", "Figshare", "source manifest")
    _known(result, "source_accession", "figshare:22491340", "source manifest")
    _known(
        result, "control_availability", "no_control_found", "observational time series"
    )
    _known(result, "x_semantics", "raw_counts", "Figshare article 22491340")

    if not result["obs_uuid"].is_unique:
        raise AssertionError("obs_uuid is not unique")
    if not result["cell_type"].notna().all():
        raise AssertionError("source-backed cell_type hierarchy did not cover all rows")
    if not result.index.equals(raw.index):
        raise AssertionError("obs row order changed")
    disposition = {
        field: {
            "states": result[f"{field}__state"].value_counts(dropna=False).to_dict(),
            "sources": result[f"{field}__source"].value_counts(dropna=False).to_dict(),
            "non_null_rows": int(result[field].notna().sum()),
            "total_rows": len(result),
        }
        for field in CANONICAL_OBS_FIELDS
    }
    return result, disposition


def curate_var(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    required = {"id", "gene_short_name", "organism"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise AssertionError(f"accepted var is missing source fields: {missing}")
    if len(raw) != EXPECTED_SHAPE[1] or not raw.index.is_unique:
        raise AssertionError("accepted var row identity is invalid")
    ids = raw["id"].astype("string")
    valid = ids.str.match(WORMBASE_GENE_RE, na=False)
    if not valid.all() or not ids.is_unique:
        raise AssertionError("not every feature has a unique stable WormBase gene ID")
    organism_ok = raw["organism"].astype("string").eq("Caenorhabditis elegans")
    if not organism_ok.all():
        raise AssertionError("feature species is not uniformly C. elegans")

    result = raw.copy()
    result["original_var_index"] = raw.index.astype(str)
    result["stable_feature_id"] = ids
    result["ensembl_gene_id"] = ids
    result["gene_symbol"] = raw["gene_short_name"].astype("string")
    result["feature_namespace"] = "WormBase Gene ID / Ensembl Metazoa"
    result["ensembl_species"] = "caenorhabditis_elegans"
    result["stable_feature_id_state"] = "known"
    result["stable_feature_id_source"] = "source var.id (WBGene stable identifier)"
    result["species_validation_state"] = "known"
    result["species_validation_source"] = "source var.organism + WBGene namespace"
    result["feature_contract_class"] = "species_correct_stable_gene_identifier"
    if not result.index.equals(raw.index):
        raise AssertionError("var row order changed")
    evidence = {
        "status": "pass",
        "biological_features_total": len(result),
        "stable_ensembl_id_features": int(valid.sum()),
        "correct_species_features": int(organism_ok.sum()),
        "stable_identifier_namespace": "WormBase Gene IDs as used by Ensembl Metazoa for C. elegans",
        "ordered_stable_feature_id_sha256": hashlib.sha256(
            "\n".join(ids.tolist()).encode()
        ).hexdigest(),
        "provenance": [
            "source var.id",
            "source var.organism",
            "Figshare article 22491340",
            "Packer et al. 2019 DOI 10.1126/science.aax1971",
        ],
    }
    return result, evidence


def validate_axes(
    raw_obs: pd.DataFrame,
    raw_var: pd.DataFrame,
    source: ad.AnnData,
    accepted_x: ad.AnnData,
) -> dict[str, Any]:
    if (
        tuple(source.shape) != EXPECTED_SHAPE
        or tuple(accepted_x.shape) != EXPECTED_SHAPE
    ):
        raise AssertionError("source or accepted X shape mismatch")
    source_obs_names = pd.Index(source.obs_names.astype(str))
    source_var_names = pd.Index(source.var_names.astype(str))
    source_obs_match = source_obs_names.equals(raw_obs.index.astype(str))
    source_var_match = source_var_names.equals(raw_var.index.astype(str))
    source_cell_match = pd.Index(source.obs["cell"].astype(str)).equals(
        pd.Index(raw_obs["cell"].astype(str))
    )
    source_gene_match = pd.Index(source.var["id"].astype(str)).equals(
        pd.Index(raw_var["id"].astype(str))
    )
    x_obs_match = pd.Index(accepted_x.obs_names.astype(str)).equals(
        raw_obs.index.astype(str)
    )
    x_var_match = pd.Index(accepted_x.var_names.astype(str)).equals(
        raw_var.index.astype(str)
    )
    checks = {
        "source_obs_index_order_equals_accepted_obs_index": source_obs_match,
        "source_var_index_order_equals_accepted_var_index": source_var_match,
        "source_cell_order_equals_accepted_cell": source_cell_match,
        "source_gene_id_order_equals_accepted_var_id": source_gene_match,
        "accepted_x_obs_order_equals_obs_index": x_obs_match,
        "accepted_x_var_order_equals_var_index": x_var_match,
    }
    if not all(checks.values()):
        raise AssertionError(f"axis identity failed: {checks}")
    return checks


def _gcloud_cp(uri: str, target: Path) -> None:
    subprocess.run(
        [
            "gcloud",
            "storage",
            "cp",
            "--billing-project=jkobject-1549353370965",
            uri,
            str(target),
        ],
        check=True,
    )


def _download_source(target: Path) -> None:
    subprocess.run(
        [
            "curl",
            "-fL",
            "--retry",
            "5",
            "--retry-delay",
            "2",
            "-o",
            str(target),
            SOURCE_URL,
        ],
        check=True,
    )
    if target.stat().st_size != SOURCE_SIZE or _md5(target) != SOURCE_MD5:
        raise AssertionError("Figshare source identity mismatch")


def _artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "branch_id": int(artifact.branch_id),
        "created_on_id": int(artifact.created_on_id),
        "is_latest": bool(artifact.is_latest),
        "description": str(artifact.description),
        "collections": [
            {"uid": str(item.uid), "key": str(item.key)}
            for item in artifact.collections.only("uid", "key").all()
        ],
        "features": {
            str(key): str(value)
            for key, value in artifact.features.get_values().items()
        },
    }


def _membership_identity(artifacts: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in artifacts),
        key=lambda item: (item["key"], item["uid"]),
    )


def _membership_sha256(artifacts: list[Any]) -> str:
    payload = json.dumps(
        _membership_identity(artifacts), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _replacement_membership(
    before: list[Any], old_obs: Any, new_obs: Any
) -> list[Any]:
    matches = [item for item in before if str(item.key) == str(old_obs.key)]
    if len(matches) != 1 or str(matches[0].uid) != str(old_obs.uid):
        raise AssertionError("predecessor Collection target OBS identity drift")
    after = [item for item in before if str(item.key) != str(old_obs.key)]
    after.append(new_obs)
    keys = [str(item.key) for item in after]
    if len(after) != len(before) or len(keys) != len(set(keys)):
        raise AssertionError("successor Collection key uniqueness/count drift")
    return after


def _latest_global_successor(ln: Any) -> Any:
    candidates: dict[str, tuple[Any, dict[str, Any]]] = {}
    for collection in ln.Collection.filter().all():
        try:
            description = json.loads(str(collection.description or ""))
        except json.JSONDecodeError:
            continue
        if description.get("format") == "pert-gym.append-only-dataset-e2e-successor/v1":
            required = {
                "predecessor_uid",
                "predecessor_membership_sha256",
                "resulting_membership_sha256",
                "member_count_before",
                "member_count_after",
            }
            if not required.issubset(description):
                raise AssertionError(
                    f"global successor contract incomplete: {collection.uid}"
                )
            ln.Collection.get(uid=description["predecessor_uid"])
            candidates[str(collection.uid)] = (collection, description)
    if not candidates:
        raise AssertionError("global append-only Collection chain absent")
    predecessor_uids = {
        str(description["predecessor_uid"])
        for _, description in candidates.values()
        if str(description["predecessor_uid"]) in candidates
    }
    tips = [
        collection
        for uid, (collection, _) in candidates.items()
        if uid not in predecessor_uids
    ]
    if not tips:
        raise AssertionError("global append-only Collection graph has no acyclic tip")
    # Historical dataset workers produced multiple immutable tips before the
    # single-writer successor protocol was standardized. Extend the newest
    # exact tip deterministically; the task-owned global writer lease prevents
    # a concurrent successor race at this boundary.
    tips.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    tip = tips[-1]
    tip_description = candidates[str(tip.uid)][1]
    actual = list(tip.artifacts.all())
    if (
        len(actual) != int(tip_description["member_count_after"])
        or _membership_sha256(actual)
        != tip_description["resulting_membership_sha256"]
    ):
        raise AssertionError(f"global successor tip membership drift: {tip.uid}")
    predecessor = ln.Collection.get(uid=tip_description["predecessor_uid"])
    predecessor_members = list(predecessor.artifacts.all())
    if (
        len(predecessor_members) != int(tip_description["member_count_before"])
        or _membership_sha256(predecessor_members)
        != tip_description["predecessor_membership_sha256"]
    ):
        raise AssertionError(f"global successor tip predecessor drift: {tip.uid}")
    return tip


def _ensure_collection_successor(
    ln: Any, old_obs: Any, new_obs: Any
) -> tuple[Any, dict[str, Any]]:
    existing = list(ln.Collection.filter(key=SUCCESSOR_COLLECTION_KEY).all())
    if len(existing) > 1:
        raise AssertionError("successor Collection key collision")
    if existing:
        recorded = json.loads(str(existing[0].description))
        if (
            recorded.get("format")
            != "pert-gym.append-only-dataset-e2e-successor/v1"
            or recorded.get("task_id") != TASK_ID
            or recorded.get("dataset_id") != DATASET_ID
        ):
            raise AssertionError("successor Collection provenance drift")
        validated_tip = _latest_global_successor(ln)
        if str(validated_tip.uid) != str(existing[0].uid):
            raise AssertionError("recovered successor is not the validated global tip")
        predecessor = ln.Collection.get(uid=recorded["predecessor_uid"])
    else:
        predecessor = _latest_global_successor(ln)
    before = list(predecessor.artifacts.all())
    after = _replacement_membership(before, old_obs, new_obs)
    description = json.dumps(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_membership_sha256": _membership_sha256(before),
            "member_count_before": len(before),
            "member_count_after": len(after),
            "resulting_membership_sha256": _membership_sha256(after),
            "replacements": [
                {
                    "key": str(old_obs.key),
                    "replaced_uid": str(old_obs.uid),
                    "added_uid": str(new_obs.uid),
                }
            ],
            "membership_rule": "immutable predecessor with the exact C. elegans OBS anchor replaced by its source-curated revision; X and VAR are reached through explicit feature links",
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    created = False
    if existing:
        successor = existing[0]
    else:
        successor = ln.Collection(
            after,
            key=SUCCESSOR_COLLECTION_KEY,
            description=description,
            skip_hash_lookup=True,
        ).save()
        created = True
    actual = list(successor.artifacts.all())
    if str(successor.description) != description or _membership_identity(
        actual
    ) != _membership_identity(after):
        raise AssertionError("successor Collection readback drift")
    if _membership_sha256(list(predecessor.artifacts.all())) != _membership_sha256(before):
        raise AssertionError("immutable predecessor Collection membership drift")
    return successor, {
        "status": "pass_append_only_successor",
        "created": created,
        "predecessor": {
            "uid": str(predecessor.uid),
            "key": str(predecessor.key),
            "member_count": len(before),
            "membership_sha256": _membership_sha256(before),
        },
        "successor": {
            "uid": str(successor.uid),
            "key": str(successor.key),
            "member_count": len(actual),
            "membership_sha256": _membership_sha256(actual),
        },
        "replacements": json.loads(description)["replacements"],
    }


def scientific_axis_evidence(raw_obs: pd.DataFrame) -> dict[str, Any]:
    """Classify source time/stage coordinates without conflating pseudotime."""
    required = {
        "time.point",
        "raw.embryo.time",
        "embryo.time",
        "embryo.time.bin",
        "raw.embryo.time.bin",
        "timepoint",
        "pseudotime",
    }
    missing = sorted(required - set(raw_obs.columns))
    if missing:
        raise AssertionError(f"source temporal evidence is incomplete: {missing}")

    def frequency(column: str) -> dict[str, int]:
        return {
            str(key): int(value)
            for key, value in raw_obs[column]
            .value_counts(dropna=False)
            .sort_index()
            .items()
        }

    biological_levels = sorted(
        float(value) for value in raw_obs["timepoint"].dropna().unique()
    )
    if len(biological_levels) <= 1:
        raise AssertionError("source supports no varying biological time axis")
    timepoint = raw_obs["timepoint"].astype(float)
    embryo_time = raw_obs["embryo.time"].astype(float)
    if not timepoint.equals(embryo_time):
        raise AssertionError("canonical timepoint and source embryo.time disagree")
    baseline = raw_obs["is_baseline"].astype(bool)
    if not baseline.equals(timepoint.eq(biological_levels[0])):
        raise AssertionError("baseline labels disagree with minimum developmental time")
    if raw_obs["pseudotime"].astype(float).equals(timepoint):
        raise AssertionError("pseudotime is indistinguishable from elapsed time")
    for values, labels, axis in (
        (timepoint, raw_obs["embryo.time.bin"], "developmental"),
        (
            raw_obs["raw.embryo.time"].astype(float),
            raw_obs["raw.embryo.time.bin"],
            "raw developmental",
        ),
    ):
        for value, label in zip(values, labels, strict=True):
            text = str(label).strip()
            if text.startswith("<"):
                valid = value < float(text.removeprefix("<").strip())
            elif text.startswith(">"):
                valid = value > float(text.removeprefix(">").strip())
            else:
                lower, upper = (float(part.strip()) for part in text.split("-", 1))
                valid = lower <= value <= upper
            if not valid:
                raise AssertionError(
                    f"{axis} time {value} disagrees with stage bin {text!r}"
                )
    return {
        "scientific_modality": "single-cell RNA expression developmental atlas",
        "perturbation_status": "observational_unperturbed",
        "experimental_unit": {
            "observation": "cell",
            "sample": "source batch of pooled C. elegans embryos",
            "dataset": "one embryogenesis atlas",
        },
        "experimental_axes": {
            "biological_developmental_time": {
                "verdict": "multitimepoint_biological_axis",
                "canonical_field": "timepoint",
                "unit": "minutes post-fertilization",
                "distinct_levels": len(biological_levels),
                "minimum": biological_levels[0],
                "maximum": biological_levels[-1],
                "row_frequencies": frequency("timepoint"),
                "source": "source obs.embryo.time/timepoint; Figshare 22491340 and Packer 2019 assigned embryo-time labels",
            },
            "collection_label": {
                "verdict": "source_collection_label_not_cell_age",
                "raw_field": "time.point",
                "row_frequencies": frequency("time.point"),
            },
            "developmental_stage_bin": {
                "verdict": "derived_developmental_stage_bin",
                "raw_fields": ["embryo.time.bin", "raw.embryo.time.bin"],
                "row_frequencies": frequency("embryo.time.bin"),
            },
            "pseudotime": {
                "verdict": "computed_trajectory_coordinate_not_elapsed_time",
                "distinct_levels": int(raw_obs["pseudotime"].nunique(dropna=True)),
                "present_rows": int(raw_obs["pseudotime"].notna().sum()),
            },
            "batch": {
                "verdict": "technical_or_collection_grouping",
                "row_frequencies": frequency("batch"),
            },
        },
        "outcomes_endpoints": {
            "verdict": "none",
            "note": "expression and developmental annotations only; no perturbation-response, viability, survival, or disease endpoint",
        },
        "classification": "temporal_developmental_expression_atlas",
    }


def _scientific_equivalence_gate(ln: Any, accepted: dict[str, Any]) -> dict[str, Any]:
    expected_lineage = {item["uid"] for item in EXPECTED.values()} | {
        "Mhw9jYDtVSGL0niy0000",
        "NHLwhKq7PHtr2Nf40000",
        "3HFHNsfi4VX80xG40000",
        "DHXE2aYWf1ir671K0000",
    }
    found: dict[str, Any] = {}
    for term in (
        "c_elegans_embryogenesis",
        "22491340",
        "39943585",
        "c_elegans.h5ad",
        "aax1971",
    ):
        for field in ("key", "description"):
            try:
                records = ln.Artifact.filter(**{f"{field}__icontains": term}).all()
            except Exception:
                records = []
            for record in records:
                found[str(record.uid)] = record
    candidates = [_artifact_identity(found[uid]) for uid in sorted(found)]
    unexpected = [
        item
        for item in candidates
        if item["uid"] not in expected_lineage
        and not item["description"].startswith(f"{TASK_ID}:")
    ]
    if unexpected:
        raise AssertionError(
            f"unexpected scientifically equivalent candidates: {unexpected}"
        )
    for role, artifact in accepted.items():
        expected = EXPECTED[role]
        identity = _artifact_identity(artifact)
        if identity["uid"] != expected["uid"] or identity["key"] != expected["key"]:
            raise AssertionError(f"accepted {role} identity drift: {identity}")
    return {
        "status": "pass",
        "search_terms": [
            "c_elegans_embryogenesis",
            "22491340",
            "39943585",
            "c_elegans.h5ad",
            "aax1971",
        ],
        "visible_candidates": candidates,
        "unexpected_non_lineage_candidates": [],
        "decision": "revise the accepted obs/var lineage; reuse accepted X; create no duplicate family",
    }


def _task_revision(ln: Any, key: str) -> Any | None:
    records = [
        item
        for item in ln.Artifact.filter(key=key).all()
        if str(item.description).startswith(f"{TASK_ID}:")
    ]
    if len(records) > 1:
        raise AssertionError(
            f"multiple task revisions already exist for {key}: {records}"
        )
    return records[0] if records else None


def _validate_task_revision(
    ln: Any,
    artifact: Any,
    predecessor: Any,
    expected_path: Path,
) -> None:
    if (
        str(artifact.key) != str(predecessor.key)
        or int(artifact.branch_id) != int(predecessor.branch_id)
        or not bool(artifact.is_latest)
        or not str(artifact.description).startswith(f"{TASK_ID}:")
    ):
        raise AssertionError("task revision catalog identity drift")
    history = list(ln.Artifact.filter(key=str(predecessor.key)).all())
    history_uids = {str(item.uid) for item in history}
    if str(predecessor.uid) not in history_uids or str(artifact.uid) not in history_uids:
        raise AssertionError("task revision lineage is incomplete")
    task_revisions = [
        item
        for item in history
        if str(item.description).startswith(f"{TASK_ID}:")
    ]
    if len(task_revisions) != 1 or str(task_revisions[0].uid) != str(artifact.uid):
        raise AssertionError("task revision lineage is ambiguous")
    actual = pd.read_parquet(artifact.cache())
    expected = pd.read_parquet(expected_path)
    try:
        pd.testing.assert_frame_equal(actual, expected, check_categorical=True)
    except AssertionError as error:
        raise AssertionError("task revision payload readback drift") from error


def _validate_receipt_artifact(
    artifact: Any,
    receipt_path: Path,
    receipt_key: str,
    branch_id: int,
) -> None:
    if (
        str(artifact.key) != receipt_key
        or int(artifact.branch_id) != branch_id
        or not bool(artifact.is_latest)
        or not str(artifact.description).startswith(f"{TASK_ID}:")
    ):
        raise AssertionError("receipt artifact catalog identity drift")
    cached = Path(artifact.cache())
    try:
        actual = json.loads(cached.read_text())
        expected = json.loads(receipt_path.read_text())
    except (json.JSONDecodeError, OSError) as error:
        raise AssertionError("receipt artifact payload is unreadable") from error
    stable_fields = {
        "schema_version",
        "task_id",
        "dataset_id",
        "logical_key",
        "instance",
        "branch",
        "source_identity",
        "scientific_classification",
        "counts",
        "axis_checks",
        "source_matrix",
        "accepted_matrix",
        "before",
        "after",
        "obs",
        "var",
        "x",
        "collection",
        "transactional_protocol",
    }
    if {field: actual.get(field) for field in stable_fields} != {
        field: expected.get(field) for field in stable_fields
    }:
        raise AssertionError("receipt artifact stable payload readback drift")


def run(output_dir: Path, *, dry_run: bool = False) -> dict[str, Any]:
    if platform.system() == "Darwin" and not dry_run:
        raise RuntimeError("live curation is restricted to the EU worker")
    output_dir.mkdir(parents=True, exist_ok=True)
    if dry_run:
        raise RuntimeError("use curate_obs/curate_var directly for fixture tests")

    capacity = preflight()
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")

    with tempfile.TemporaryDirectory(prefix=f"{TASK_ID}-") as temp:
        root = Path(temp)
        accepted = {
            role: ln.Artifact.get(uid=spec["uid"]) for role, spec in EXPECTED.items()
        }
        equivalence = _scientific_equivalence_gate(ln, accepted)
        before = {role: _artifact_identity(item) for role, item in accepted.items()}

        paths = {role: root / Path(spec["key"]).name for role, spec in EXPECTED.items()}
        for role, path in paths.items():
            _gcloud_cp(f"{CANONICAL_PREFIX}/{path.name}", path)
            if (
                path.stat().st_size != EXPECTED[role]["size"]
                or _sha256(path) != EXPECTED[role]["sha256"]
            ):
                raise AssertionError(
                    f"accepted canonical {role} payload identity mismatch"
                )
        source_path = root / "c_elegans.h5ad"
        _download_source(source_path)

        raw_obs = pd.read_parquet(paths["obs"])
        raw_var = pd.read_parquet(paths["var"])
        source = ad.read_h5ad(source_path, backed="r")
        accepted_x = ad.read_h5ad(paths["X"], backed="r")
        try:
            axis_checks = validate_axes(raw_obs, raw_var, source, accepted_x)
        finally:
            source.file.close()
            accepted_x.file.close()
        source_matrix = matrix_receipt(source_path)
        accepted_matrix = matrix_receipt(paths["X"])
        for matrix_name, matrix in (
            ("Figshare source", source_matrix),
            ("accepted X", accepted_matrix),
        ):
            if (
                tuple(matrix["shape"]) != EXPECTED_SHAPE
                or matrix["nnz"] != EXPECTED_NNZ
            ):
                raise AssertionError(f"{matrix_name} sparse shape/nnz mismatch")
            observed = {
                name: matrix["arrays"][name]["sha256"]
                for name in EXPECTED_MATRIX_ARRAYS
            }
            if observed != EXPECTED_MATRIX_ARRAYS:
                raise AssertionError(
                    f"{matrix_name} sparse-array hash mismatch: {observed}"
                )

        curated_obs, field_dispositions = curate_obs(raw_obs)
        curated_var, var_evidence = curate_var(raw_var)
        axes = scientific_axis_evidence(raw_obs)
        obs_path = root / "curated_obs.parquet"
        var_path = root / "curated_var.parquet"
        curated_obs.to_parquet(obs_path)
        curated_var.to_parquet(var_path)
        obs_readback = pd.read_parquet(obs_path)
        var_readback = pd.read_parquet(var_path)
        if not obs_readback.index.equals(
            raw_obs.index
        ) or not var_readback.index.equals(raw_var.index):
            raise AssertionError("local parquet round-trip changed axis identity")

        lease_metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": ln.setup.settings.branch.name,
            "started_at": time.time(),
        }
        with ExitStack() as leases:
            leases.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            distributed = leases.enter_context(
                distributed_lamin_writer_lease(lease_metadata)
            )
            obs_artifact = _task_revision(ln, accepted["obs"].key)
            if obs_artifact is None:
                obs_artifact = ln.Artifact.from_dataframe(
                    obs_path,
                    key=accepted["obs"].key,
                    description=(
                        f"{TASK_ID}: C. elegans embryogenesis OBS_COMPLETED; "
                        "source-exhaustive Figshare 22491340 / Packer 2019 curation"
                    ),
                    revises=accepted["obs"],
                ).save()
            _validate_task_revision(
                ln, obs_artifact, accepted["obs"], obs_path
            )
            obs_artifact.features.set_values({"X": accepted["X"]})

            var_artifact = _task_revision(ln, accepted["var"].key)
            if var_artifact is None:
                var_artifact = ln.Artifact.from_dataframe(
                    var_path,
                    key=accepted["var"].key,
                    description=(
                        f"{TASK_ID}: C. elegans embryogenesis species-correct VAR; "
                        "20,222 source-native WormBase/Ensembl Metazoa stable gene identifiers"
                    ),
                    revises=accepted["var"],
                ).save()
            _validate_task_revision(
                ln, var_artifact, accepted["var"], var_path
            )
            accepted["X"].features.set_values({"var": var_artifact})

        x_after = _artifact_identity(ln.Artifact.get(uid=EXPECTED["X"]["uid"]))
        if (
            x_after["uid"] != before["X"]["uid"]
            or x_after["hash"] != before["X"]["hash"]
        ):
            raise AssertionError("accepted X identity drifted")
        with ExitStack() as leases:
            leases.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            leases.enter_context(distributed_lamin_writer_lease(lease_metadata))
            _, collection_evidence = _ensure_collection_successor(
                ln, accepted["obs"], obs_artifact
            )
        after_obs = _artifact_identity(obs_artifact)
        after_var = _artifact_identity(var_artifact)

        receipt = {
            "schema_version": "pert-gym.dataset-completion-receipt/v2",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "logical_key": LOGICAL_KEY,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "instance": ln.setup.settings.instance.slug,
            "branch": ln.setup.settings.branch.name,
            "writer_lease": {
                "dataset_id": DATASET_ID,
                "run_id": TASK_ID,
                "global_local_lock": True,
                "global_distributed_lock": True,
                "distributed_lease_id": distributed.lease_id,
                "host": capacity.hostname,
                "project": capacity.project,
                "zone": capacity.zone,
            },
            "scientific_equivalence_gate": equivalence,
            "source_identity": {
                "figshare_article_id": 22491340,
                "figshare_file_id": 39943585,
                "figshare_file_size": SOURCE_SIZE,
                "figshare_file_md5": SOURCE_MD5,
                "publication_doi": "10.1126/science.aax1971",
            },
            "scientific_classification": axes,
            "counts": {
                "biological_datasets": 1,
                "logical_families": 1,
                "physical_members": 1,
                "observations": EXPECTED_SHAPE[0],
                "variables": EXPECTED_SHAPE[1],
                "matrix_nonzeros": EXPECTED_NNZ,
            },
            "axis_checks": axis_checks,
            "source_matrix": source_matrix,
            "accepted_matrix": accepted_matrix,
            "before": before,
            "after": {"obs": after_obs, "X": x_after, "var": after_var},
            "obs": {
                "status": "pass",
                "canonical_field_count": len(CANONICAL_OBS_FIELDS),
                "retained_raw_columns": list(map(str, raw_obs.columns)),
                "field_dispositions": field_dispositions,
                "obs_uuid_unique": bool(curated_obs["obs_uuid"].is_unique),
                "row_order_preserved": True,
            },
            "var": var_evidence,
            "x": {
                "status": "pass_reused_accepted_payload",
                "uid": EXPECTED["X"]["uid"],
                "sha256": EXPECTED["X"]["sha256"],
                "shape": list(EXPECTED_SHAPE),
                "nnz": EXPECTED_NNZ,
                "rewritten": False,
            },
            "collection": collection_evidence,
            "transactional_protocol": {
                "append_only_revisions": True,
                "x_reused": True,
                "collection_membership_mutations": 0,
                "append_only_collection_creations": int(collection_evidence["created"]),
                "artifact_deletions": 0,
                "rollback_identity": {
                    "obs_uid": EXPECTED["obs"]["uid"],
                    "var_uid": EXPECTED["var"]["uid"],
                    "x_uid": EXPECTED["X"]["uid"],
                },
            },
        }
        receipt_path = output_dir / "verification_receipt.json"
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")

        receipt_key = f"data/cleaned/c_elegans_embryogenesis/curation/{TASK_ID}/verification_receipt.json"
        with ExitStack() as leases:
            leases.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            leases.enter_context(distributed_lamin_writer_lease(lease_metadata))
            receipt_artifact = _task_revision(ln, receipt_key)
            if receipt_artifact is None:
                receipt_artifact = ln.Artifact(
                    receipt_path,
                    key=receipt_key,
                    description=f"{TASK_ID}: C. elegans embryogenesis dataset-completion verification receipt",
                ).save()
            _validate_receipt_artifact(
                receipt_artifact,
                receipt_path,
                receipt_key,
                int(accepted["obs"].branch_id),
            )
            receipt_artifact.features.set_values(
                {"obs": obs_artifact, "X": accepted["X"], "var": var_artifact}
            )
        receipt["receipt_artifact"] = _artifact_identity(receipt_artifact)
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/dataset_completion/temporal__c_elegans_embryogenesis"),
    )
    args = parser.parse_args()
    result = run(args.output_dir)
    print("C_ELEGANS_COMPLETION=" + json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
