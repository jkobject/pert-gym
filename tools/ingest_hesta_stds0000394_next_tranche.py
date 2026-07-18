#!/usr/bin/env python3
"""VM-only bounded ingestion for the next HESTA/STDS0000394 residual tranche.

Kanban t_dc659a0e: ingest exactly one additional bounded normal HESTA processed
file after the accepted CS12-13_E2S6 tranche, with obs/X/var same-prefix triplet
plus a typed obsm_spatial sidecar. Must run on pert-gym-worker-eu and connect to
laminlabs/pertdata branch jkobject through tools.lamin_context.connect_pertdata().
"""
from __future__ import annotations

import argparse
import json
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

TASK_ID = "t_dc659a0e"
BILLING_PROJECT = "jkobject-1549353370965"
BASE_URL = "https://ftp.cngb.org/pub/SciRAID/stomics/STDS0000394/stomics"
DATE = datetime.now(timezone.utc).strftime("%Y%m%d")
REPORT_JSON = ROOT / f"artifacts/schema_audit/hesta_stds0000394_CS12-13_E2S7_tranche_{DATE}.json"
REPORT_MD = ROOT / f"artifacts/schema_audit/hesta_stds0000394_CS12-13_E2S7_tranche_{DATE}.md"

# Selected from continuation t_dc659a0e after accepted CS12-13_E2S6 ingestion.
# Organ/pathway/regulon/substructure extras and >=10GB/high-risk tail files are
# excluded.
SELECTED = {
    "sample": "CS12-13_E2S7",
    "file_name": "CS12-13_E2S7_HESTA.h5ad",
    "api_file_size": "source size re-verified by HTTP HEAD during this continuation",
    "stage": "CS12-13",
    "size_order_reason": "first exact-clear residual normal processed HESTA file in bounded CS12-13/CS14-15 source/default order after reviewer-accepted CS12-13_E2S6 tranche; reviewer live preflight showed CS12-13_E2S7 source-present with 0/0/0/0 target duplicates; exact duplicate re-probe required immediately before writing; bounded plan keeps CS12-13/CS14-15 normal processed files in scope and excludes organ/tail extras",
}
PREFIX = f"temporal_pretraining/stomics/hesta/{SELECTED['sample']}/chunk_0000"
SOURCE_URL = f"{BASE_URL}/{SELECTED['file_name']}"
LOCAL_SOURCE_DEFAULT = Path(f"data/hesta_stds0000394/{SELECTED['file_name']}")

EXCLUDED_PATTERNS = [".gene.h5ad", ".pathway.h5ad", ".regulon.h5ad", ".substructure.h5ad"]
NEXT_RESIDUAL_PLAN = [
    "Continue with the next bounded CS12-13/CS14-15 normal processed file by metadata/source/duplicate evidence.",
    "Continue CS12-13/CS14-15 processed files in bounded size order; keep CS19/CS20/CS23 tail and >=10GB files out of this tranche.",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def assert_vm() -> None:
    host = socket.gethostname()
    if "pert-gym-worker-eu" not in host:
        raise RuntimeError(f"Refusing to run outside pert-gym-worker-eu; hostname={host!r}")


def head_source() -> dict[str, Any]:
    proc = run(["curl", "-I", "-L", "--retry", "3", "--retry-delay", "10", "--max-time", "300", SOURCE_URL])
    headers: dict[str, str] = {}
    status = 0
    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if line.startswith("HTTP/"):
            parts = line.split()
            if len(parts) >= 2:
                status = int(parts[1])
        elif ":" in line:
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()
    return {
        "url": SOURCE_URL,
        "status": status,
        "content_length": int(headers.get("content-length", "0") or 0),
        "last_modified": headers.get("last-modified"),
        "content_type": headers.get("content-type"),
    }


def download_source(path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    head = head_source()
    expected = int(head.get("content_length") or 0)
    existing = path.stat().st_size if path.exists() else 0
    if expected and existing == expected:
        return {"status": "exists_complete", "path": str(path), "bytes": existing, "expected_bytes": expected}
    if expected and existing > expected:
        path.unlink()
        existing = 0
    for attempt in range(1, 21):
        current = path.stat().st_size if path.exists() else 0
        if expected and current == expected:
            break
        if expected and current > expected:
            path.unlink()
            current = 0
        cmd = ["curl", "-L", "--fail", "--retry", "3", "--retry-delay", "10", "--connect-timeout", "60"]
        if current:
            cmd.extend(["-C", "-"])
        cmd.extend(["--output", str(path), SOURCE_URL])
        proc = subprocess.run(cmd, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        final_attempt_size = path.stat().st_size if path.exists() else 0
        if proc.returncode == 0 and (not expected or final_attempt_size == expected):
            break
        if expected and final_attempt_size > expected:
            path.unlink()
            continue
        if expected and final_attempt_size > current:
            continue
        raise RuntimeError(
            f"Download attempt {attempt} failed for {path}: rc={proc.returncode}, "
            f"bytes={final_attempt_size}, expected={expected}, output={proc.stdout[-2000:]}"
        )
    final = path.stat().st_size
    if expected and final != expected:
        raise RuntimeError(f"Incomplete download for {path}: got {final}, expected {expected}")
    return {
        "status": "resumed_downloaded" if existing else "downloaded",
        "path": str(path),
        "bytes": final,
        "expected_bytes": expected,
    }


def ensure_features(ln: Any) -> None:
    for name in ("X", "var", "obsm_spatial"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name} dtype {found[0].dtype}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def exact_key_counts(ln: Any, prefix: str = PREFIX) -> dict[str, int]:
    return {
        name: int(ln.Artifact.filter(key=f"{prefix}/{name}").count())
        for name in ("obs.parquet", "X.h5ad", "var.parquet", "obsm_spatial.parquet")
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def inspect_backed(path: Path) -> dict[str, Any]:
    source = ad.read_h5ad(path, backed="r")
    try:
        spatial = np.asarray(source.obsm["spatial"][:]) if "spatial" in source.obsm else None
        return {
            "shape": [int(source.n_obs), int(source.n_vars)],
            "isbacked": bool(source.isbacked),
            "x_class": type(source.X).__name__,
            "obs_columns": list(source.obs.columns),
            "var_columns": list(source.var.columns),
            "obs_index_name": source.obs.index.name,
            "var_index_name": source.var.index.name,
            "obs_head": source.obs.head(3).astype(str).to_dict(orient="records"),
            "var_head_index": source.var.index[:10].astype(str).tolist(),
            "obsm_keys": list(source.obsm.keys()),
            "uns_keys": list(source.uns.keys()),
            "spatial_shape": list(spatial.shape) if spatial is not None else None,
            "spatial_dtype": str(spatial.dtype) if spatial is not None else None,
            "spatial_sample": spatial[:5].tolist() if spatial is not None else None,
        }
    finally:
        source.file.close()


def make_payloads(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, sp.csr_matrix, pd.DataFrame, dict[str, Any]]:
    source = ad.read_h5ad(path)
    if "spatial" not in source.obsm:
        raise ValueError("Expected obsm['spatial'] per HESTA spatial policy")
    spatial = np.asarray(source.obsm["spatial"])
    if spatial.shape[0] != source.n_obs or spatial.ndim != 2 or spatial.shape[1] < 2:
        raise ValueError(f"Unexpected spatial shape {spatial.shape} for n_obs={source.n_obs}")

    obs = source.obs.copy()
    obs.index = obs.index.astype(str)
    obs.index.name = obs.index.name or "obs_id"
    obs["dataset"] = "HESTA/STDS0000394"
    obs["source_accession"] = "STDS0000394"
    obs["source_sample"] = SELECTED["sample"]
    obs["source_title"] = "Human Embryo SpatioTemporal transcriptomic Atlas HESTA"
    obs["organism"] = "Homo sapiens"
    obs["assay"] = "STOmics/Stereo-seq spatial transcriptomics"
    obs["modality"] = "spatial_transcriptomics"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["developmental_stage"] = SELECTED["stage"]
    obs["source_uri"] = SOURCE_URL
    obs["x_semantics"] = "unknown_or_normalized_expression_pending_source_confirmation"
    obs["spatial_metadata_policy"] = "obsm['spatial'] preserved in same-prefix typed obsm_spatial.parquet sidecar"

    var = source.var.copy()
    var.index = var.index.astype(str)
    var.index.name = var.index.name or "gene_id"
    var["source_accession"] = "STDS0000394"
    var["organism"] = "Homo sapiens"

    x = source.X
    if sp.issparse(x):
        x = x.tocsr()
    else:
        x = sp.csr_matrix(x)

    spatial_df = pd.DataFrame(index=obs.index)
    spatial_df.index.name = obs.index.name
    spatial_df["obs_index"] = obs.index.astype(str)
    for i in range(spatial.shape[1]):
        spatial_df[f"spatial_dim_{i}"] = spatial[:, i]
    spatial_df["source_spatial_key"] = "obsm['spatial']"
    spatial_df["coordinate_system"] = "source_stomics_coordinate_system"
    spatial_df["coordinate_units"] = "source_pixels_or_spot_units_unknown"
    if spatial.shape[1] >= 2:
        spatial_df["source_x"] = spatial[:, 0]
        spatial_df["source_y"] = spatial[:, 1]
    if spatial.shape[1] >= 3:
        spatial_df["source_z"] = spatial[:, 2]

    summary = {
        "n_obs": int(source.n_obs),
        "n_vars": int(source.n_vars),
        "X_dtype": str(x.dtype),
        "X_sparse": True,
        "X_nnz": int(x.nnz),
        "obs_rows": int(len(obs)),
        "var_rows": int(len(var)),
        "spatial_rows": int(len(spatial_df)),
        "spatial_n_dims": int(spatial.shape[1]),
        "obs_cols": list(obs.columns),
        "var_cols": list(var.columns),
        "spatial_cols": list(spatial_df.columns),
        "layers": list(source.layers.keys()),
        "raw_present": source.raw is not None,
    }
    return obs, var, x, spatial_df, summary


def save_payloads(ln: Any, obs: pd.DataFrame, var: pd.DataFrame, x: sp.csr_matrix, spatial: pd.DataFrame) -> dict[str, Any]:
    dup = exact_key_counts(ln)
    if any(dup.values()):
        raise RuntimeError(f"Refusing overwrite; exact target keys already exist: {dup}")
    with tempfile.TemporaryDirectory(prefix="hesta_next_") as tmp:
        x_path = Path(tmp) / "X.h5ad"
        ad.AnnData(
            X=x.copy(),
            obs=pd.DataFrame(index=obs.index.astype(str).copy()),
            var=pd.DataFrame(index=var.index.astype(str).copy()),
        ).write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(
            obs.copy(),
            key=f"{PREFIX}/obs.parquet",
            description=f"HESTA STDS0000394 {SELECTED['sample']} obs metadata",
        ).save()
        x_art = ln.Artifact.from_anndata(
            str(x_path),
            key=f"{PREFIX}/X.h5ad",
            description=f"HESTA STDS0000394 {SELECTED['sample']} expression matrix",
        ).save()
        var_art = ln.Artifact.from_dataframe(
            var.copy(),
            key=f"{PREFIX}/var.parquet",
            description=f"HESTA STDS0000394 {SELECTED['sample']} var metadata",
            skip_hash_lookup=True,
        ).save()
        spatial_art = ln.Artifact.from_dataframe(
            spatial.copy(),
            key=f"{PREFIX}/obsm_spatial.parquet",
            description=f"HESTA STDS0000394 {SELECTED['sample']} obsm['spatial'] sidecar",
            skip_hash_lookup=True,
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art, "obsm_spatial": spatial_art})
    return {
        "obs_uid": obs_art.uid,
        "x_uid": x_art.uid,
        "var_uid": var_art.uid,
        "obsm_spatial_uid": spatial_art.uid,
    }


def verify(ln: Any) -> dict[str, Any]:
    counts = exact_key_counts(ln)
    obs_art = ln.Artifact.get(key=f"{PREFIX}/obs.parquet")
    obs_features = obs_art.features.get_values()
    x_art = resolve_artifact(ln, obs_features["X"])
    spatial_art = resolve_artifact(ln, obs_features["obsm_spatial"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    spatial = spatial_art.load()
    obs_index = obs.index.astype(str).tolist()
    spatial_obs_index = spatial["obs_index"].astype(str).tolist() if "obs_index" in spatial.columns else []
    return {
        "exact_key_counts": counts,
        "keys": {
            "obs": obs_art.key,
            "X": x_art.key,
            "var": var_art.key,
            "obsm_spatial": spatial_art.key,
        },
        "uids": {
            "obs": obs_art.uid,
            "X": x_art.uid,
            "var": var_art.uid,
            "obsm_spatial": spatial_art.uid,
        },
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "spatial_rows": int(spatial.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "obs_x_var_link_ok": x_art.key == f"{PREFIX}/X.h5ad" and var_art.key == f"{PREFIX}/var.parquet",
        "obs_spatial_link_ok": spatial_art.key == f"{PREFIX}/obsm_spatial.parquet",
        "same_prefix_var": var_art.key == f"{PREFIX}/var.parquet",
        "row_count_ok": int(obs.shape[0]) == int(x_art.n_observations or 0) == int(spatial.shape[0]),
        "spatial_order_ok": obs_index == spatial_obs_index,
        "var_count_positive": int(var.shape[0]) > 0,
        "duplicate_status_after": counts,
        "not_collection_or_model_ready": True,
    }


def write_reports(report: dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    v = report.get("verification", {})
    lines = [
        "# HESTA/STDS0000394 next residual bounded tranche ingestion",
        "",
        f"Generated: {report['generated_at']}",
        f"Task: `{TASK_ID}`",
        f"Host: `{report['host']}`",
        f"Source URI: `{SOURCE_URL}`",
        f"Target prefix: `{PREFIX}/`",
        f"Dry run: `{report['dry_run']}`",
        f"Write: `{report['write']}`",
        "",
        "## Selected tranche",
        f"- Selected file: `{SELECTED['file_name']}` ({SELECTED['api_file_size']})",
        f"- Reason: {SELECTED['size_order_reason']}.",
        "- Excluded extras: organ `.gene.h5ad`, `.pathway.h5ad`, `.regulon.h5ad`, and `.substructure.h5ad` payloads were not selected.",
        "- Excluded tails: no CS19/CS20/CS23 broad tail or >=10GB/high-risk file was loaded.",
        "",
        "## Safety and duplicate probes",
        "- Ran on `pert-gym-worker-eu`; no Mac bulk source/Lamin read/write.",
        "- Connected via `tools.lamin_context.connect_pertdata()` to `laminlabs/pertdata` branch `jkobject`; global Lamin CLI not used.",
        f"- Exact-key duplicate probe before write: `{report.get('duplicate_probe_before_write')}`",
        "- No-overwrite behavior: write aborts if any target key already exists.",
        "",
        "## Source/read schema",
        f"- HEAD: `{report.get('source_head')}`",
        f"- Local VM source: `{report.get('local_source')}`",
        f"- Backed inspect: `{report.get('backed_inspect')}`",
        f"- Payload summary: `{report.get('payload_summary')}`",
        "- Spatial policy: same-prefix typed `obsm_spatial.parquet` sidecar linked from obs via `obsm_spatial`; coordinates also retain source x/y obs aliases when present in source obs, but authoritative payload is sidecar.",
        "",
        "## Written keys and producer readback",
        f"- Saved UIDs: `{report.get('saved')}`",
        f"- Verification: `{v}`",
        "",
        "## Residual tranche plan",
    ]
    lines.extend([f"- {item}" for item in NEXT_RESIDUAL_PLAN])
    lines.extend([
        "",
        "## Non-claims",
        "- This is a partial bounded tranche only, not full HESTA/STDS0000394 ingestion.",
        "- No Collection, canonical, model-ready, or final-gate completion claim is made.",
    ])
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
        "source_uri": SOURCE_URL,
        "selected": SELECTED,
        "excluded_patterns": EXCLUDED_PATTERNS,
        "target_prefix": PREFIX,
        "dry_run": bool(args.dry_run),
        "write": bool(args.write),
        "verify_only": bool(args.verify_only),
        "residual_tranche_plan": NEXT_RESIDUAL_PLAN,
        "commands": [
            "gcloud compute scp tools/ingest_hesta_stds0000394_next_tranche.py pert-gym-worker-eu:~/work/pert-gym/tools/ingest_hesta_stds0000394_next_tranche.py --zone europe-west1-b",
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_hesta_stds0000394_next_tranche.py --dry-run'",
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_hesta_stds0000394_next_tranche.py --write'",
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_hesta_stds0000394_next_tranche.py --verify-only'",
            f"uv run python -m json.tool {REPORT_JSON.relative_to(ROOT)}",
        ],
    }

    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    report["lamin"] = {
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "branch_uid": ln.setup.settings.branch.uid,
    }
    if not args.verify_only:
        ln.track(path="tools/ingest_hesta_stds0000394_next_tranche.py")
        ensure_features(ln)
    report["duplicate_probe_before_write"] = exact_key_counts(ln)

    if args.verify_only:
        report["source_head"] = head_source()
        report["local_source"] = (
            {"status": "exists", "path": str(args.local_source), "bytes": args.local_source.stat().st_size}
            if args.local_source.exists()
            else {"status": "missing", "path": str(args.local_source), "bytes": 0}
        )
        if args.local_source.exists():
            report["backed_inspect"] = inspect_backed(args.local_source)
            _obs, _var, _x, _spatial, summary = make_payloads(args.local_source)
            report["payload_summary"] = summary
        report["verification"] = verify(ln)
        write_reports(report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    if any(report["duplicate_probe_before_write"].values()):
        raise RuntimeError(f"Refusing overwrite; duplicate probe found {report['duplicate_probe_before_write']}")

    report["source_head"] = head_source()
    report["local_source"] = download_source(args.local_source)
    report["backed_inspect"] = inspect_backed(args.local_source)
    obs, var, x, spatial, summary = make_payloads(args.local_source)
    report["payload_summary"] = summary

    if args.write:
        report["saved"] = save_payloads(ln, obs, var, x, spatial)
        report["verification"] = verify(ln)
    else:
        report["saved"] = None
        report["verification"] = {"not_run": "pass --write to register, or --verify-only after write"}
    write_reports(report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
