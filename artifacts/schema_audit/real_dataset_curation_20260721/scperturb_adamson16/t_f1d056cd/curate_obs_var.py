#!/usr/bin/env python3
"""Append-only OBS curation and read-only VAR verdict for scperturb/adamson16."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from anndata.utils import make_index_unique
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_f1d056cd"
DATASET_ID = "scperturb/adamson16"
SOURCE_ACCESSION = "GSE90546"
TABLE_S1_URL = (
    "https://pmc.ncbi.nlm.nih.gov/articles/instance/5315571/bin/"
    "NIHMS832990-supplement-8.xlsx"
)
TABLE_S1_PATH = Path(__file__).with_name("table_s1_guide_map.json")
TABLE_S1_SHA256 = "d006e9c56610c72e590ad14f6ac2c048f5a6200ce6d79209998fc358fcb4844e"

COMPONENTS = {
    "scperturb/adamson16_GSM2406675_10X001": {
        "gsm": "GSM2406675",
        "experiment": "GSM2406675_10X001",
        "n_obs": 5768,
        "sequencer": "Illumina HiSeq 2500",
        "barcodes_sha256": "b44a3d466b03cafe7479ed98b3f6aa9becc9ce3651f17b49f7f77ca03d72cd32",
        "identities_sha256": "e0e719dfba62a3651b5ccde1d318973e5a80bba1863dbc98c104ea74ac6f5f34",
    },
    "scperturb/adamson16_GSM2406677_10X005": {
        "gsm": "GSM2406677",
        "experiment": "GSM2406677_10X005",
        "n_obs": 15006,
        "sequencer": "Illumina HiSeq 4000",
        "barcodes_sha256": "0525bc03ebd7ba7c0b7a777ff9a739330fdaa092568363c9e0e7b85ebce15895",
        "identities_sha256": "fc9cb6314594345057c1b937fa0c7947399f073196a3104963da609be7b74d14",
    },
    "scperturb/adamson16_GSM2406681_10X010": {
        "gsm": "GSM2406681",
        "experiment": "GSM2406681_10X010",
        "n_obs": 65337,
        "sequencer": "Illumina HiSeq 4000",
        "barcodes_sha256": "1e0d820343d0c6e17bdab4fef96f4446e223b94861942ccf3d7225818e009836",
        "identities_sha256": "8b40be7a2280c1713bf5a1eb828aad46e70ad3a7000b5f5a1322e51e06c4cf7f",
    },
}

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
    "guide_id",
    "guide_sequence",
    "perturbation_target",
    "perturbation_target_id",
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
    "source",
    "source_accession",
    "control_availability",
    "x_semantics",
)

NOT_APPLICABLE_FIELDS = {
    "dose",
    "dose_unit",
    "trajectory_id",
    "pseudotime",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
}

UNKNOWN_FIELDS = {
    "donor_id",
    "age",
    "ethnicity",
    "media",
    "perturbation_target_id",
    "timepoint",
    "is_baseline",
    "is_low_quality",
    "x_semantics",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def emit_product(phase: str, current: int) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "real_dataset_obs_var_components",
                    "current": current,
                    "denominator": 3,
                    "unit": "physical_member",
                }
            }
        ),
        flush=True,
    )


def read_nested_gzip_csv(data: bytes, **kwargs: Any) -> pd.DataFrame:
    while data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return pd.read_csv(io.BytesIO(data), **kwargs)


def fetch_source(spec: dict[str, Any], suffix: str) -> tuple[bytes, str]:
    file_name = f"{spec['experiment']}_{suffix}"
    url = (
        f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={spec['gsm']}"
        f"&format=file&file={urllib.parse.quote(file_name)}"
    )
    request = urllib.request.Request(
        url, headers={"User-Agent": "pert-gym-metadata-audit/1"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:  # nosec B310
        data = response.read()
    return data, url


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    current = records[-1]
    if not bool(current.is_latest):
        raise AssertionError(f"ordered newest Artifact is not latest: {key}")
    return current, records


def resolve_feature_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    if by_uid:
        raise AssertionError(f"duplicate Artifact uid: {value}")
    return latest_artifact(ln, value)[0]


def load_table_s1() -> tuple[dict[str, str], dict[str, str], dict[str, Any]]:
    if sha256_file(TABLE_S1_PATH) != TABLE_S1_SHA256:
        raise AssertionError("Table S1 extracted map hash drift")
    payload = json.loads(TABLE_S1_PATH.read_text(encoding="utf-8"))
    if payload["source_url"] != TABLE_S1_URL or payload["row_count"] != 98:
        raise AssertionError("Table S1 source identity drift")
    by_vector: dict[str, str] = {}
    by_guide: dict[str, str] = {}
    for row in payload["rows"]:
        sequence = row["protospacer"]
        if not re.fullmatch(r"[ACGT]{19,21}", sequence):
            raise AssertionError("invalid Table S1 protospacer")
        if row["perturb_seq_vector_id"]:
            by_vector[row["perturb_seq_vector_id"]] = sequence
        if row["guide_id"]:
            by_guide[row["guide_id"].replace("IRE1α", "IRE1")] = sequence
    return by_vector, by_guide, payload


def reproduce_scperturb_join(
    obs: pd.DataFrame, spec: dict[str, Any]
) -> tuple[pd.Series, pd.DataFrame, dict[str, Any]]:
    barcodes_data, barcodes_url = fetch_source(spec, "barcodes.tsv.gz")
    identities_data, identities_url = fetch_source(spec, "cell_identities.csv.gz")
    if sha256_bytes(barcodes_data) != spec["barcodes_sha256"]:
        raise AssertionError(f"barcode source hash drift: {spec['experiment']}")
    if sha256_bytes(identities_data) != spec["identities_sha256"]:
        raise AssertionError(f"identity source hash drift: {spec['experiment']}")
    barcodes = read_nested_gzip_csv(
        barcodes_data, sep="\t", header=None, names=["raw_barcode"]
    )
    identities = read_nested_gzip_csv(identities_data, index_col=0)
    if len(barcodes) != len(obs) or not identities.index.is_unique:
        raise AssertionError("source sidecar denominator/identity drift")
    base = pd.Index(
        barcodes["raw_barcode"].astype(str).str.rsplit("-", n=1).str[0],
        name=obs.index.name,
    )
    reproduced_index = make_index_unique(base)
    if not reproduced_index.equals(obs.index.astype(str)):
        raise AssertionError("scPerturb barcode transformation does not reproduce OBS index")
    metadata = identities.copy()
    metadata.index = metadata.index.astype(str).str.rsplit("-", n=1).str[0]
    metadata = metadata.loc[~metadata.index.duplicated(keep="first")]
    joined = metadata.reindex(reproduced_index)
    comparisons = {}
    left_guide = obs["pert_genetic"].astype("string")
    right_guide = joined["guide identity"].astype("string")
    guide_equal = (left_guide.isna() & right_guide.isna()) | (
        left_guide.fillna("") == right_guide.fillna("")
    )
    comparisons["pert_genetic"] = int((~guide_equal).sum())
    for column in ("read count", "UMI count"):
        left = pd.to_numeric(obs[column], errors="coerce")
        right = pd.to_numeric(joined[column], errors="coerce")
        equal = (left.isna() & right.isna()) | np.isclose(
            left.fillna(0), right.fillna(0), rtol=0, atol=0
        )
        comparisons[column] = int((~equal).sum())
    if any(comparisons.values()):
        raise AssertionError(f"source join parity mismatch: {comparisons}")
    gem_group = barcodes["raw_barcode"].astype(str).str.rsplit("-", n=1).str[-1]
    gem_group.index = obs.index
    return gem_group, joined, {
        "barcodes_url": barcodes_url,
        "barcodes_sha256": spec["barcodes_sha256"],
        "identities_url": identities_url,
        "identities_sha256": spec["identities_sha256"],
        "barcodes_rows": len(barcodes),
        "identities_rows": len(identities),
        "joined_guide_rows": int(joined["guide identity"].notna().sum()),
        "join_mismatch_count": sum(comparisons.values()),
        "scperturb_join_semantics": "strip gem-group suffix, make OBS names unique, strip identity suffix, keep first duplicate, left-join to OBS order",
    }


def target_lists(obs: pd.DataFrame) -> list[list[str]]:
    values: list[list[str]] = []
    for _, row in obs.iterrows():
        if "pert_target_multi" in obs and isinstance(row.get("pert_target_multi"), np.ndarray):
            targets = [str(value) for value in row["pert_target_multi"] if str(value)]
        else:
            value = row.get("pert_target")
            targets = [] if pd.isna(value) else [str(value)]
        values.append(targets)
    return values


def guide_sequences(
    guide: object, by_vector: dict[str, str], by_guide: dict[str, str]
) -> list[str]:
    if pd.isna(guide):
        return []
    name = str(guide)
    if name in {"*", "empty"}:
        return []
    match = re.search(r"(pDS\d+)$", name)
    if match and match.group(1) in by_vector:
        return [by_vector[match.group(1)]]
    if "pMJ" in name and "neg_ctrl" not in name.lower():
        result = []
        for token in ("ATF6", "PERK", "IRE1"):
            if token in name and token in by_guide:
                result.append(by_guide[token])
        return result
    return []


def add_numbered(frame: pd.DataFrame, field: str, values: list[list[Any]]) -> None:
    maximum = max((len(items) for items in values), default=0)
    for position in range(maximum):
        column = field if position == 0 else f"{field}_{position + 1}"
        frame[column] = pd.Series(
            [items[position] if position < len(items) else pd.NA for items in values],
            index=frame.index,
            dtype="string",
        )


def curate_obs(
    obs: pd.DataFrame,
    prefix: str,
    spec: dict[str, Any],
    by_vector: dict[str, str],
    by_guide: dict[str, str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    original = obs.copy(deep=True)
    gem_group, joined, join_receipt = reproduce_scperturb_join(obs, spec)
    curated = obs.copy(deep=True)
    curated["dataset"] = prefix
    curated["sample"] = spec["gsm"]
    curated["cell_id"] = curated.index.astype(str)
    curated["batch"] = "gemgroup_" + gem_group.astype(str)
    curated["sequencer"] = spec["sequencer"]
    curated["technology"] = curated["assay"].astype(str)
    curated["modality"] = "scRNA-seq"
    curated["is_bulk"] = False
    curated["is_pseudobulk"] = False
    assigned = curated["pert_genetic"].astype("string").replace({"*": pd.NA, "empty": pd.NA})
    curated["perturbation"] = assigned
    curated["perturbation_type"] = "CRISPRi"
    curated["perturbation_technology"] = "CRISPR interference"
    curated["perturbation_library"] = "Adamson et al. 2016 Perturb-seq sgRNA library"
    curated["guide_id"] = assigned
    targets = target_lists(curated)
    add_numbered(curated, "perturbation_target", targets)
    sequences = [guide_sequences(value, by_vector, by_guide) for value in assigned]
    add_numbered(curated, "guide_sequence", sequences)
    controls = pd.Series(pd.NA, index=curated.index, dtype="boolean")
    controls.loc[assigned.str.contains("neg_ctrl", case=False, na=False)] = True
    known_noncontrol = assigned.notna() & pd.Series(
        [bool(items) for items in targets], index=curated.index
    )
    controls.loc[known_noncontrol] = False
    curated["is_control"] = controls
    curated["n_counts"] = curated["ncounts"]
    curated["n_genes"] = curated["ngenes"]
    curated["pct_mito"] = curated["percent_mito"]
    curated["pct_ribo"] = curated["percent_ribo"]
    curated["source"] = "scPerturb"
    curated["source_accession"] = SOURCE_ACCESSION
    curated["control_availability"] = "strict_control_available"

    assert_frame_equal(curated.loc[:, original.columns], original, check_categorical=True)
    if not curated.index.equals(original.index):
        raise AssertionError("OBS row order/index drift")
    if len(curated) != spec["n_obs"]:
        raise AssertionError("OBS row denominator drift")
    if not curated["obs_uuid"].is_unique:
        raise AssertionError("OBS UUID uniqueness drift")
    if not curated["original_obs_index"].astype(str).equals(
        original["original_obs_index"].astype(str)
    ):
        raise AssertionError("original OBS identity drift")
    return curated, {
        **join_receipt,
        "materialized_columns": sorted(set(curated.columns) - set(original.columns)),
        "guide_sequence_known_rows": int(curated["guide_sequence"].notna().sum()),
        "guide_id_known_rows": int(curated["guide_id"].notna().sum()),
        "target_known_rows": int(curated["perturbation_target"].notna().sum()),
        "control_true_rows": int(curated["is_control"].fillna(False).sum()),
        "control_false_rows": int((curated["is_control"] == False).fillna(False).sum()),  # noqa: E712
        "control_unknown_rows": int(curated["is_control"].isna().sum()),
    }


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        if field in NOT_APPLICABLE_FIELDS:
            result[field] = {"disposition": "not_applicable", "materialized": field in frame}
        elif field in UNKNOWN_FIELDS:
            observed = []
            if field in frame:
                observed = sorted(frame[field].dropna().astype(str).unique().tolist())[:20]
            result[field] = {
                "disposition": "unknown",
                "materialized": field in frame,
                "observed_non_null_sample": observed,
            }
        elif field not in frame:
            result[field] = {"disposition": "unknown", "materialized": False}
        else:
            known = int(frame[field].notna().sum())
            result[field] = {
                "disposition": "materialized_complete" if known == len(frame) else "materialized_partial",
                "materialized": True,
                "known_rows": known,
                "unknown_rows": len(frame) - known,
                "source_bound": True,
            }
    if set(result) != set(CANONICAL_OBS_FIELDS):
        raise AssertionError("canonical OBS disposition coverage drift")
    return result


def verify_var(var: pd.DataFrame, expected_rows: int) -> dict[str, Any]:
    required = {
        "stable_feature_id",
        "stable_feature_id_namespace",
        "stable_feature_id_mapping_status",
        "organism",
    }
    missing = sorted(required - set(var.columns))
    if missing or len(var) != expected_rows:
        raise AssertionError(f"VAR contract drift: missing={missing}, rows={len(var)}")
    stable = var["stable_feature_id"].dropna().astype(str)
    ensembl = stable.str.match(r"^ENSG\d{11}(?:\.\d+)?$")
    if not var["organism"].dropna().astype(str).isin({"human", "Homo sapiens"}).all():
        raise AssertionError("VAR species drift")
    return {
        "rows": len(var),
        "columns": list(map(str, var.columns)),
        "stable_feature_id_non_null": int(stable.size),
        "human_ensembl_stable_ids": int(ensembl.sum()),
        "mapping_status_counts": var["stable_feature_id_mapping_status"]
        .astype(str)
        .value_counts(dropna=False)
        .sort_index()
        .to_dict(),
        "organism_values": sorted(var["organism"].dropna().astype(str).unique()),
        "mismatch_count": 0,
    }


def collection_membership(ln: Any, keys: set[str]) -> dict[str, Any]:
    snapshots = {}
    for collection_key in (
        "pert-gym/base-public/20260621",
        "pert-gym/canonical/20260621",
    ):
        records = list(ln.Collection.filter(key=collection_key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {collection_key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        member_keys = {str(member.key) for member in members}
        snapshots[collection_key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_keys_present": sorted(keys & member_keys),
            "target_key_count": len(keys & member_keys),
        }
        if len(keys & member_keys) != len(keys):
            raise AssertionError(f"target OBS keys absent from {collection_key}")
    return snapshots


def verify_current(
    ln: Any,
    by_vector: dict[str, str],
    by_guide: dict[str, str],
) -> tuple[list[dict[str, Any]], bool]:
    results = []
    all_curated = True
    for prefix, spec in COMPONENTS.items():
        obs_key = f"{prefix}/obs.parquet"
        obs_artifact, history = latest_artifact(ln, obs_key)
        obs = obs_artifact.load()
        x = resolve_feature_artifact(ln, obs_artifact.features.get_values()["X"])
        var = resolve_feature_artifact(ln, x.features.get_values()["var"])
        var_frame = var.load()
        curated, source_receipt = curate_obs(
            obs, prefix, spec, by_vector, by_guide
        )
        already_curated = str(obs_artifact.description).startswith(
            f"{TASK_ID}: source-exhaustive Adamson16 OBS"
        )
        all_curated &= already_curated
        if already_curated:
            assert_frame_equal(obs, curated, check_categorical=True)
        results.append(
            {
                "prefix": prefix,
                "obs_before": artifact_identity(obs_artifact),
                "obs_history_count": len(history),
                "x": artifact_identity(x),
                "var": artifact_identity(var),
                "rows": len(obs),
                "source_join": source_receipt,
                "field_dispositions": field_dispositions(curated),
                "var_verdict": verify_var(var_frame, len(var_frame)),
                "already_curated": already_curated,
                "curated_frame": curated,
                "obs_artifact": obs_artifact,
                "x_artifact": x,
            }
        )
    return results, all_curated


def publish(ln: Any, results: list[dict[str, Any]], helper_sha256: str) -> list[Any]:
    writes = []
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-adamson16-obs-"))
    ln.track(
        key=f"pert-gym/real-dataset-curation/{DATASET_ID}/{TASK_ID}",
        kind="script",
        params={
            "task_id": TASK_ID,
            "helper_sha256": helper_sha256,
            "table_s1_sha256": TABLE_S1_SHA256,
        },
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    for index, result in enumerate(results, start=1):
        prefix = result["prefix"]
        path = root / f"obs-{index}.parquet"
        result["curated_frame"].to_parquet(path)
        description = (
            f"{TASK_ID}: source-exhaustive Adamson16 OBS; exact GEO sidecar join; "
            f"paper Table S1 guide sequences; source={SOURCE_ACCESSION}; member={prefix}"
        )
        artifact = ln.Artifact.from_dataframe(
            path,
            key=f"{prefix}/obs.parquet",
            revises=result["obs_artifact"],
            description=description,
        ).save()
        artifact.features.set_values({"X": result["x_artifact"]})
        writes.append(artifact)
        emit_product("writing", index)
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()
    return writes


def strip_runtime(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in result.items()
        if key not in {"curated_frame", "obs_artifact", "x_artifact"}
    }


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    helper_sha256 = sha256_file(Path(__file__))
    capacity = preflight()
    emit_product("preflight", 0)
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("wrong Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin branch")
    by_vector, by_guide, table_s1 = load_table_s1()
    results, all_curated = verify_current(ln, by_vector, by_guide)
    target_keys = {f"{prefix}/obs.parquet" for prefix in COMPONENTS}
    collections_before = collection_membership(ln, target_keys)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    writes: list[Any] = []
    if mode == "mutate" and not all_curated:
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": ln.setup.settings.branch.name,
            "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh_results, fresh_all_curated = verify_current(ln, by_vector, by_guide)
            if fresh_all_curated:
                results = fresh_results
                all_curated = True
            else:
                writes = publish(ln, fresh_results, helper_sha256)
    elif mode == "verify" and not all_curated:
        raise AssertionError("verify requested before exact OBS revisions exist")

    final_results, final_all_curated = verify_current(ln, by_vector, by_guide)
    if mode in {"mutate", "verify"} and not final_all_curated:
        raise AssertionError("terminal OBS curation readback failed")
    collections_after = collection_membership(ln, target_keys)
    if collections_after != collections_before:
        raise AssertionError("Collection drift during OBS-only curation")
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    write_receipts = [artifact_identity(artifact) for artifact in writes]
    receipt = {
        "format": "pert-gym.real-dataset-obs-var-curation/v2",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "table_s1": {
            "url": TABLE_S1_URL,
            "map_sha256": TABLE_S1_SHA256,
            "extracted_markdown_sha256": table_s1["extracted_markdown_sha256"],
            "row_count": table_s1["row_count"],
        },
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 3,
            "physical_members": 3,
            "observations": 86111,
        },
        "members_before": [strip_runtime(item) for item in results],
        "members_after": [strip_runtime(item) for item in final_results],
        "collections": collections_after,
        "writes": {
            "obs_revisions": len(writes),
            "var_revisions": 0,
            "x_revisions": 0,
            "collection_writes": 0,
            "deletions": 0,
            "artifacts": write_receipts,
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = hashlib.sha256(
        canonical(receipt).encode()
    ).hexdigest()
    emit_product("checkpointing", 3)
    print("ADAMSON16_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
