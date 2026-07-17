#!/usr/bin/env python3
"""Register the first bounded MOSTA E10.5_E1S1 same-prefix triplet.

Guarded for Kanban t_23151d8d. Must run on pert-gym-worker-eu near the
Requester Pays GCS bucket. The schema-smoke parent established n_obs=18,408 <=
100,000, so this writes one full same-prefix triplet, not a row chunk.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_23151d8d"
SOURCE_URI = "gs://scperturb/pert-gym/staging/manual_temporal/2026-06-25/STDS0000058/stomics/E10.5_E1S1.MOSTA.h5ad"
BILLING_PROJECT = "jkobject-1549353370965"
PREFIX = "temporal_pretraining/mosta_stds0000058/E10.5_E1S1"
EXPECTED_N_OBS = 18408
EXPECTED_N_VARS = 25201
DATE = datetime.now(timezone.utc).strftime("%Y%m%d")
REPORT_JSON = ROOT / f"artifacts/schema_audit/mosta_E10p5_E1S1_first_triplet_{DATE}.json"
REPORT_MD = ROOT / f"artifacts/schema_audit/mosta_E10p5_E1S1_first_triplet_{DATE}.md"
LOCAL_SOURCE_DEFAULT = Path("/tmp/pert-gym-mosta-schema/E10.5_E1S1.MOSTA.h5ad")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=check)


def assert_vm() -> None:
    host = socket.gethostname()
    if "pert-gym-worker-eu" not in host:
        raise RuntimeError(f"Refusing to run outside pert-gym-worker-eu; hostname={host!r}")


def gcs_describe() -> dict[str, Any]:
    cp = run([
        "gcloud", "storage", "objects", "describe", SOURCE_URI,
        "--billing-project", BILLING_PROJECT, "--format=json",
    ])
    return json.loads(cp.stdout)


def stage_source(local_path: Path) -> dict[str, Any]:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists() and local_path.stat().st_size > 0:
        return {"status": "exists", "path": str(local_path), "bytes": local_path.stat().st_size}
    cp = run([
        "gcloud", "storage", "cp", "--billing-project", BILLING_PROJECT,
        SOURCE_URI, str(local_path),
    ])
    return {
        "status": "copied",
        "path": str(local_path),
        "bytes": local_path.stat().st_size,
        "output_tail": cp.stdout[-2000:],
    }


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name} dtype {found[0].dtype}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def exact_key_counts(ln: Any, prefix: str = PREFIX) -> dict[str, int]:
    return {
        name: int(ln.Artifact.filter(key=f"{prefix}/{name}").count())
        for name in ("obs.parquet", "X.h5ad", "var.parquet")
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def hash_index(values: pd.Index) -> str:
    h = hashlib.sha256()
    for value in values.astype(str):
        h.update(value.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def make_obs(source: ad.AnnData) -> pd.DataFrame:
    obs = source.obs.copy()
    obs.index = obs.index.astype(str)
    obs.index.name = obs.index.name or "spot_id"
    if "spatial" not in source.obsm:
        raise ValueError("Expected obsm['spatial'] from schema smoke")
    spatial = np.asarray(source.obsm["spatial"])
    if spatial.shape != (source.n_obs, 2):
        raise ValueError(f"Expected spatial shape {(source.n_obs, 2)}, got {spatial.shape}")
    obs["spatial_x"] = spatial[:, 0]
    obs["spatial_y"] = spatial[:, 1]
    obs["dataset"] = "MOSTA/STDS0000058"
    obs["source_accession"] = "STDS0000058"
    obs["source_sample"] = "E10.5_E1S1"
    obs["source_title"] = "MOSTA mouse embryo E10.5 E1S1 Stereo-seq"
    obs["organism"] = "Mus musculus"
    obs["assay"] = "STOmics/Stereo-seq spatial transcriptomics"
    obs["modality"] = "spatial_transcriptomics"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["developmental_stage"] = "E10.5"
    obs["section_id"] = "E1S1"
    obs["source_gcs_uri"] = SOURCE_URI
    obs["x_semantics"] = "raw_counts_from_count_layer"
    obs["spatial_metadata_policy"] = "obsm['spatial'] preserved as obs.spatial_x/obs.spatial_y"
    return obs


def make_var(source: ad.AnnData) -> pd.DataFrame:
    var = source.var.copy()
    var.index = var.index.astype(str)
    var.index.name = var.index.name or "gene_id"
    var["source_accession"] = "STDS0000058"
    var["organism"] = "Mus musculus"
    return var


def load_source(local_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, sp.csr_matrix, dict[str, Any]]:
    source = ad.read_h5ad(local_path)
    if source.n_obs != EXPECTED_N_OBS or source.n_vars != EXPECTED_N_VARS:
        raise ValueError(f"Unexpected source shape {(source.n_obs, source.n_vars)}")
    if "count" not in source.layers:
        raise ValueError("Schema smoke allowed raw-count claim only because layer 'count' exists; it is missing now")
    x = source.layers["count"]
    if not sp.issparse(x):
        x = sp.csr_matrix(x)
    else:
        x = x.tocsr()
    if x.shape != (source.n_obs, source.n_vars):
        raise ValueError(f"count layer shape {x.shape} != source shape {(source.n_obs, source.n_vars)}")
    obs = make_obs(source)
    var = make_var(source)
    summary = {
        "n_obs": int(source.n_obs),
        "n_vars": int(source.n_vars),
        "X_source": "layers['count']",
        "X_semantics": "raw_counts_from_count_layer",
        "X_dtype": str(x.dtype),
        "X_sparse": True,
        "X_nnz": int(x.nnz),
        "raw_present": source.raw is not None,
        "layers": list(source.layers.keys()),
        "obsm_spatial_shape": list(np.asarray(source.obsm["spatial"]).shape),
        "obs_rows": int(len(obs)),
        "obs_cols": list(obs.columns),
        "var_rows": int(len(var)),
        "var_cols": list(var.columns),
        "var_index_sha256": hash_index(var.index),
    }
    return obs, var, x, summary


def save_triplet(ln: Any, obs: pd.DataFrame, var: pd.DataFrame, x: sp.csr_matrix) -> dict[str, Any]:
    dup = exact_key_counts(ln)
    if any(dup.values()):
        raise RuntimeError(f"Refusing overwrite; exact target keys already exist: {dup}")
    with tempfile.TemporaryDirectory(prefix="mosta_e10p5_e1s1_") as tmp:
        tmp_path = Path(tmp)
        x_path = tmp_path / "X.h5ad"
        ad.AnnData(
            X=x.copy(),
            obs=pd.DataFrame(index=obs.index.astype(str).copy()),
            var=pd.DataFrame(index=var.index.astype(str).copy()),
        ).write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(
            obs.copy(),
            key=f"{PREFIX}/obs.parquet",
            description="MOSTA STDS0000058 E10.5_E1S1 obs metadata with spatial_x/spatial_y",
        ).save()
        x_art = ln.Artifact.from_anndata(
            str(x_path),
            key=f"{PREFIX}/X.h5ad",
            description="MOSTA STDS0000058 E10.5_E1S1 raw count layer as CSR X",
        ).save()
        var_art = ln.Artifact.from_dataframe(
            var.copy(),
            key=f"{PREFIX}/var.parquet",
            description="MOSTA STDS0000058 E10.5_E1S1 var metadata in source order",
            skip_hash_lookup=True,
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_uid": obs_art.uid, "x_uid": x_art.uid, "var_uid": var_art.uid}


def verify_triplet(ln: Any) -> dict[str, Any]:
    counts = exact_key_counts(ln)
    obs_art = ln.Artifact.get(key=f"{PREFIX}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    return {
        "exact_key_counts": counts,
        "keys": {"obs": obs_art.key, "X": x_art.key, "var": var_art.key},
        "uids": {"obs": obs_art.uid, "X": x_art.uid, "var": var_art.uid},
        "obs_rows": int(obs.shape[0]),
        "obs_cols": list(obs.columns),
        "var_rows": int(var.shape[0]),
        "var_cols": list(var.columns),
        "x_n_observations": int(x_art.n_observations or 0),
        "x_n_vars_expected": EXPECTED_N_VARS,
        "same_prefix_link_ok": (
            x_art.key == f"{PREFIX}/X.h5ad" and var_art.key == f"{PREFIX}/var.parquet"
        ),
        "row_count_ok": int(obs.shape[0]) == int(x_art.n_observations or 0) == EXPECTED_N_OBS,
        "var_count_ok": int(var.shape[0]) == EXPECTED_N_VARS,
    }


def write_reports(report: dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    v = report.get("verification", {})
    lines = [
        "# MOSTA E10.5_E1S1 first triplet report",
        "",
        f"Generated: {report['generated_at']}",
        f"Task: `{TASK_ID}`",
        f"Host: `{report['host']}`",
        f"Source: `{SOURCE_URI}`",
        f"Target prefix: `{PREFIX}/`",
        f"Dry run: {report['dry_run']}",
        f"Write: {report['write']}",
        "",
        "## Safety and duplicate probe",
        "- Ran on `pert-gym-worker-eu`; no Mac full download.",
        "- Connected via `tools.lamin_context.connect_pertdata()` to `laminlabs/pertdata` branch `jkobject`; global Lamin CLI not used.",
        f"- Requester Pays billing project: `{BILLING_PROJECT}`",
        f"- Exact-key duplicate probe before write: `{report.get('duplicate_probe_before_write')}`",
        "- Sidecars not in scope: Image/TIFF/Bin1/barcode were not read.",
        "",
        "## Source/schema",
        f"- Object bytes: `{report.get('gcs_object', {}).get('size')}`; generation `{report.get('gcs_object', {}).get('generation')}`",
        f"- Local VM stage: `{report.get('local_source')}`",
        f"- Source summary: `{report.get('source_summary')}`",
        "- X semantics: `raw_counts_from_count_layer` because schema smoke and this run observed `layers['count']`; raw `.raw` is not used/claimed.",
        "- Spatial metadata policy: `obsm['spatial']` preserved as `obs.spatial_x` and `obs.spatial_y`.",
        "",
        "## Written keys and readback",
        f"- Saved: `{report.get('saved')}`",
        f"- Verification: `{v}`",
        "",
        "## Commands",
    ]
    lines.extend([f"- `{cmd}`" for cmd in report.get("commands", [])])
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-source", type=Path, default=LOCAL_SOURCE_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    assert_vm()
    ensure_project_cache()
    report: dict[str, Any] = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "host": socket.gethostname(),
        "cwd": str(Path.cwd()),
        "source_uri": SOURCE_URI,
        "target_prefix": PREFIX,
        "billing_project": BILLING_PROJECT,
        "dry_run": bool(args.dry_run),
        "write": bool(args.write),
        "verify_only": bool(args.verify_only),
        "commands": [
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_mosta_e10p5_e1s1_first_triplet.py --write'",
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_mosta_e10p5_e1s1_first_triplet.py --verify-only'",
        ],
    }

    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    if not args.verify_only:
        ln.track(path="tools/ingest_mosta_e10p5_e1s1_first_triplet.py")
        ensure_link_features(ln)
    report["lamin"] = {
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "branch_uid": ln.setup.settings.branch.uid,
    }
    report["duplicate_probe_before_write"] = exact_key_counts(ln)

    if args.verify_only:
        report["verification"] = verify_triplet(ln)
        write_reports(report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    if any(report["duplicate_probe_before_write"].values()):
        raise RuntimeError(f"Refusing overwrite; duplicate probe found {report['duplicate_probe_before_write']}")

    desc = gcs_describe()
    report["gcs_object"] = {
        "size": int(desc.get("size", 0)),
        "crc32c": desc.get("crc32c"),
        "md5Hash": desc.get("md5Hash"),
        "generation": desc.get("generation"),
    }
    report["local_source"] = stage_source(args.local_source)
    obs, var, x, source_summary = load_source(args.local_source)
    report["source_summary"] = source_summary

    if args.write:
        report["saved"] = save_triplet(ln, obs, var, x)
        report["verification"] = verify_triplet(ln)
    else:
        report["saved"] = None
        report["verification"] = {"not_run": "pass --write to register, or --verify-only after write"}
    write_reports(report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
