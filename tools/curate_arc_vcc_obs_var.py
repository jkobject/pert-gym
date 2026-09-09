#!/usr/bin/env python3
"""Source-bound Arc VCC 2025 OBS curation and VAR no-replay verifier.

Run on the EU worker only.  Verify mode is read-only.  Apply mode appends an OBS
revision only when the source-bound canonical projection differs; it never writes
X, VAR, or Collections.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.lamin_context import connect_pertdata  # noqa: E402

DATASET_ID = "arc-vcc/2025"
SOURCE = "Arc VCC 2025 H5AD OBS plus official Arc README/article"
OFFICIAL = "https://storage.googleapis.com/arc-institute-virtual-cell-atlas/virtual-cell-challenge/2025/"
CANONICAL_FIELDS = tuple(
    json.loads((ROOT / "config/obs_completed_contract_v1.json").read_text())["canonical_obs_columns"]
)
ENSG = re.compile(r"^ENSG\d{11}$")


@dataclass(frozen=True)
class Member:
    split: str
    chunk: int
    start: int
    stop: int
    obs_uid: str
    key: str
    source_file: str


MEMBERS = (
    Member("test", 0, 0, 100_000, "mGQwo6Kqs9hOdzLl0000", "arc_vcc/2025/test/chunk_0000/obs.parquet", "adata_Test.h5ad"),
    Member("test", 1, 100_000, 170_846, "jO1U5UVWKJ6gpS0H0000", "arc_vcc/2025/test/chunk_0001/obs.parquet", "adata_Test.h5ad"),
    Member("train", 0, 0, 100_000, "kcoW2Hh7iua05uC30000", "arc_vcc/2025/train/chunk_0000/obs.parquet", "adata_Training.h5ad"),
    Member("train", 1, 100_000, 200_000, "GRgJf9TdsDZqOqGU0000", "arc_vcc/2025/train/chunk_0001/obs.parquet", "adata_Training.h5ad"),
    Member("train", 2, 200_000, 221_273, "RJ3rYBXHNl8c4Lym0000", "arc_vcc/2025/train/chunk_0002/obs.parquet", "adata_Training.h5ad"),
    Member("validation", 0, 0, 98_927, "Zk9b1xHT1OX9cUgZ0000", "arc_vcc/2025/validation/chunk_0000/obs.parquet", "adata_Validation.h5ad"),
)
EXPECTED_ROWS = 491_046

NOT_APPLICABLE = {
    "donor_id": "H1 cell-line experiment has no donor design",
    "tissue_type": "cell-line assay, not donor tissue",
    "sex": "no donor-level design",
    "age": "no donor-level design",
    "ethnicity": "no donor-level design",
    "molecule_sequence": "genetic CRISPRi perturbation, not chemical molecule",
    "dose": "low MOI is not a per-cell dose",
    "dose_unit": "low MOI is not a per-cell dose",
    "trajectory_id": "not a trajectory experiment",
    "pseudotime": "not a trajectory experiment",
    "is_baseline": "no longitudinal baseline axis",
    "sensitivity": "no per-cell drug-sensitivity endpoint",
    "response_metric": "canonical X is expression, not scalar response",
    "response_value": "canonical X is expression, not scalar response",
    "response_source": "canonical X is expression, not scalar response",
}
UNKNOWN = {
    "sample": "no source sample sheet supports a cell-level sample join",
    "sequencer": "Flex chemistry does not identify instrument model",
    "media": "official release has no media formulation",
    "guide_sequence": "no published VCC protospacer table",
    "timepoint": "official release has no collection endpoint",
    "n_counts": "requires proof that current linked X is source-identical raw counts",
    "n_genes": "requires proof that current linked X is source-identical raw counts",
    "pct_mito": "requires proof that current linked X is source-identical raw counts",
    "pct_ribo": "requires proof that current linked X is source-identical raw counts",
    "is_low_quality": "no source QC flag or reviewed threshold",
}
CONSTANTS: dict[str, tuple[Any, str]] = {
    "technology": ("10x Genomics Flex", f"{SOURCE}; {OFFICIAL}"),
    "is_bulk": (False, "one H5AD observation is one single cell"),
    "is_pseudobulk": (False, "one H5AD observation is one single cell"),
    "perturbation_technology": ("dual-guide CRISPRi (dCas9-KRAB), lentiviral", SOURCE),
    "perturbation_library": ("Arc VCC 2025 dual-guide CRISPRi library", SOURCE),
}


def _sha(values: list[str]) -> str:
    return hashlib.sha256("\n".join(values).encode()).hexdigest()


def _artifact_value(ln: Any, value: Any) -> Any:
    if hasattr(value, "uid") and hasattr(value, "key"):
        return value
    if isinstance(value, str):
        for field in ("uid", "key"):
            found = list(ln.Artifact.filter(**{field: value}).all())
            if found:
                return found[-1]
    raise TypeError(f"unresolvable artifact link {value!r}")


def _series_equal(left: pd.Series, right: pd.Series) -> bool:
    return left.astype("string").fillna("<NA>").tolist() == right.astype("string").fillna("<NA>").tolist()


def _state(out: pd.DataFrame, field: str, disposition: str, source: str) -> None:
    out[f"{field}__state"] = disposition
    out[f"{field}__source"] = source


def _set_na(out: pd.DataFrame, field: str, disposition: str, source: str) -> None:
    out[field] = pd.Series(pd.NA, index=out.index, dtype="string")
    _state(out, field, disposition, source)


def _require_source_alignment(source_obs: pd.DataFrame, obs: pd.DataFrame, member: Member) -> None:
    if "original_obs_index" not in obs:
        raise ValueError(f"{member.key}: original_obs_index is absent")
    expected = source_obs.index.astype(str)
    actual = obs["original_obs_index"].astype(str)
    if not actual.is_unique or actual.tolist() != expected.tolist():
        raise ValueError(f"{member.key}: source-to-Lamin original_obs_index join is not exact ordered 1:1")
    if not obs.index.is_unique:
        raise ValueError(f"{member.key}: Lamin OBS index is not unique")


def curate_obs(source_obs: pd.DataFrame, obs: pd.DataFrame, member: Member) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Project only supported constants/direct source identity, preserving raw OBS columns."""
    _require_source_alignment(source_obs, obs, member)
    out = obs.copy()
    out["cell_id"] = source_obs.index.astype("string").to_numpy()
    _state(out, "cell_id", "materialized", "source H5AD obs index; exact ordered 1:1 join")

    for field, (value, source) in CONSTANTS.items():
        out[field] = value
        _state(out, field, "materialized", source)
    for field, source in NOT_APPLICABLE.items():
        _set_na(out, field, "not_applicable", source)
    for field, source in UNKNOWN.items():
        _set_na(out, field, "unknown", source)

    direct = ("dataset", "batch", "cell_type", "cell_line", "disease", "organism", "assay", "modality", "perturbation", "perturbation_type", "is_control")
    for field in direct:
        if field not in out or out[field].isna().any():
            raise ValueError(f"{member.key}: required pre-existing materialized field {field!r} is incomplete")
        if field in source_obs and not _series_equal(out[field], source_obs[field]):
            raise ValueError(f"{member.key}: direct source field {field!r} does not match Lamin OBS")
        _state(out, field, "materialized", f"existing OBS; source-aligned {SOURCE}")

    missing = [field for field in CANONICAL_FIELDS if field not in out]
    if missing:
        raise ValueError(f"curation omitted canonical fields: {missing}")
    disposition = {
        field: {
            "state": str(out[f"{field}__state"].iloc[0]),
            "source": str(out[f"{field}__source"].iloc[0]),
            "non_null_rows": int(out[field].notna().sum()),
            "total_rows": len(out),
        }
        for field in CANONICAL_FIELDS
    }
    return out, disposition


def _frame_changed(before: pd.DataFrame, after: pd.DataFrame) -> bool:
    if list(before.columns) != list(after.columns):
        return True
    for name in before.columns:
        if not _series_equal(before[name], after[name]):
            return True
    return False


def _var_receipt(var: pd.DataFrame) -> dict[str, Any]:
    columns = [str(name) for name in var.columns]
    candidates = [name for name in ("ensembl_gene_id", "stable_feature_id", "gene_id") if name in var]
    if not candidates:
        candidates = [str(var.index.name)] if var.index.name else []
    ids = var[candidates[0]].astype(str).tolist() if candidates else var.index.astype(str).tolist()
    valid = [bool(ENSG.fullmatch(value)) for value in ids]
    if len(ids) != 18_080 or not all(valid) or len(set(ids)) != len(ids):
        raise ValueError("VAR fails 18,080 unique human ENSG stable-ID verdict")
    return {"rows": len(ids), "id_column": candidates[0] if candidates else "index", "stable_ensg_rows": sum(valid), "stable_id_sha256": _sha(ids), "columns": columns}


def run(source_root: Path, *, apply: bool) -> dict[str, Any]:
    if platform.system() == "Darwin":
        raise RuntimeError("Arc VCC source audit must run on pert-gym-worker-eu, never macOS")
    ln = connect_pertdata()
    target = {"instance": ln.setup.settings.instance.slug, "branch": ln.setup.settings.branch.name}
    if target != {"instance": "laminlabs/pertdata", "branch": "jkobject"}:
        raise RuntimeError(f"wrong Lamin target: {target}")
    if apply:
        ln.track(path=str(Path(__file__).relative_to(ROOT)))

    sources: dict[str, ad.AnnData] = {}
    receipt: dict[str, Any] = {"schema": "arc_vcc_obs_var_curation/v1", "mode": "apply" if apply else "verify", "dataset_id": DATASET_ID, "created_at": datetime.now(timezone.utc).isoformat(), "host": platform.node(), "pid": os.getpid(), "lamin": target, "members": [], "writes": {"obs": 0, "var": 0, "X": 0, "collections": 0}}
    all_rows = 0
    all_uuids: list[str] = []
    for member in MEMBERS:
        source = sources.get(member.source_file)
        if source is None:
            source_path = source_root / member.source_file
            if not source_path.is_file():
                raise FileNotFoundError(source_path)
            source = ad.read_h5ad(source_path, backed="r")
            sources[member.source_file] = source
        source_obs = source.obs.iloc[member.start:member.stop].copy()
        obs_artifact = ln.Artifact.get(uid=member.obs_uid)
        if obs_artifact.key != member.key:
            raise RuntimeError(f"OBS identity drift: {obs_artifact.uid} -> {obs_artifact.key}")
        x_artifact = _artifact_value(ln, obs_artifact.features.get_values()["X"])
        var_artifact = _artifact_value(ln, x_artifact.features.get_values()["var"])
        obs = obs_artifact.load()
        var = var_artifact.load()
        curated, fields = curate_obs(source_obs, obs, member)
        if "obs_uuid" not in curated or not curated["obs_uuid"].is_unique:
            raise ValueError(f"{member.key}: invalid obs_uuid")
        all_uuids.extend(curated["obs_uuid"].astype(str).tolist())
        if len(obs) != member.stop - member.start or len(var) != 18_080:
            raise ValueError(f"{member.key}: axis count drift")
        changed = _frame_changed(obs, curated)
        successor: dict[str, Any] | None = None
        if apply and changed:
            local = Path("/tmp") / f"arc-vcc-{member.split}-{member.chunk:04d}-obs.parquet"
            curated.to_parquet(local, index=True)
            successor_artifact = ln.Artifact.from_dataframe(local, key=member.key, revises=obs_artifact).save()
            successor_artifact.features.set_values({"X": x_artifact})
            successor = {"uid": successor_artifact.uid, "hash": successor_artifact.hash, "key": successor_artifact.key}
            receipt["writes"]["obs"] += 1
        receipt["members"].append({"key": member.key, "source_file": member.source_file, "source_slice": [member.start, member.stop], "obs_before": {"uid": obs_artifact.uid, "hash": obs_artifact.hash, "rows": len(obs)}, "x": {"uid": x_artifact.uid, "hash": x_artifact.hash, "rows": x_artifact.n_observations}, "var": {"uid": var_artifact.uid, "hash": var_artifact.hash, "verdict": _var_receipt(var)}, "obs_changed": changed, "obs_successor": successor, "fields": fields})
        all_rows += len(obs)
    for source in sources.values():
        source.file.close()
    if all_rows != EXPECTED_ROWS or len(set(all_uuids)) != EXPECTED_ROWS:
        raise ValueError(f"physical denominator/UUID failure: rows={all_rows}, unique={len(set(all_uuids))}")
    receipt["physical_rows"] = all_rows
    receipt["global_obs_uuid_unique"] = True
    receipt["var_action"] = "no_change: all live VAR axes are exact 18,080 unique human ENSG sequences"
    receipt["obs_action"] = "append_successors" if receipt["writes"]["obs"] else "no_change"
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    receipt = run(args.source_root, apply=args.apply)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({"receipt": str(args.receipt), "physical_rows": receipt["physical_rows"], "writes": receipt["writes"], "obs_action": receipt["obs_action"], "var_action": receipt["var_action"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
