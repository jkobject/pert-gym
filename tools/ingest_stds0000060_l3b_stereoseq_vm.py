#!/usr/bin/env python3
"""VM-only one-file STDS0000060 Stereo-seq H5AD triplet ingestion.

This guarded script is scoped to a single CNGB/STOmics processed H5AD and writes
one same-prefix obs -> X -> var triplet on laminlabs/pertdata branch jkobject.
It is intended to run on pert-gym-worker-eu, not the Mac mini.
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_624e74d0"
URL = "https://ftp.cngb.org/pub/SciRAID/stomics/STDS0000060/stomics/L3_b_count_normal_stereoseq.h5ad"
FILENAME = "L3_b_count_normal_stereoseq.h5ad"
PREFIX = "temporal_pretraining/stomics/STDS0000060_drosophila_stereoseq/L3_b_count_normal_stereoseq"
PRIOR_PREFIXES = [
    "temporal_pretraining/stomics/STDS0000060_drosophila_stereoseq/E14-16h_a_count_normal_stereoseq",
    "temporal_pretraining/stomics/STDS0000060_drosophila_stereoseq/L1_a_count_normal_stereoseq",
]
ARTIFACT_JSON = ROOT / "artifacts/schema_audit/stds0000060_L3_b_lamin_ingestion_20260704.json"
ARTIFACT_MD = ROOT / "artifacts/schema_audit/stds0000060_L3_b_lamin_ingestion_20260704.md"
EXPECTED_HOST = "pert-gym-worker-eu"
EXPECTED_LENGTH = 5662973452


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], *, check: bool = True, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, timeout=timeout, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def curl_head() -> dict[str, Any]:
    cp = run(["curl", "-fsSIL", "--max-time", "180", URL], check=False, timeout=240)
    headers: dict[str, str] = {}
    for line in cp.stdout.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.lower()] = v.strip()
    return {"exit_code": cp.returncode, "headers": headers, "output_tail": cp.stdout[-2000:]}


def curl_download(dest: Path) -> dict[str, Any]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".partial")
    if dest.exists() and dest.stat().st_size == EXPECTED_LENGTH:
        return {"status": "exists", "path": str(dest), "bytes": dest.stat().st_size}
    if dest.exists() and dest.stat().st_size != EXPECTED_LENGTH:
        dest.rename(part)
    cmd = [
        "curl", "-L", "--fail", "--retry", "8", "--retry-all-errors", "--connect-timeout", "30",
        "--speed-time", "120", "--speed-limit", "1024", "-C", "-", "-o", str(part), URL,
    ]
    cp = run(cmd, check=False)
    if cp.returncode != 0:
        return {"status": "failed", "exit_code": cp.returncode, "partial_path": str(part), "partial_bytes": part.stat().st_size if part.exists() else 0, "output_tail": cp.stdout[-4000:]}
    size = part.stat().st_size
    if size != EXPECTED_LENGTH:
        return {"status": "bad_size", "path": str(part), "bytes": size, "expected_bytes": EXPECTED_LENGTH, "output_tail": cp.stdout[-4000:]}
    part.rename(dest)
    return {"status": "downloaded", "path": str(dest), "bytes": size, "output_tail": cp.stdout[-2000:]}


def artifact_keys(prefix: str) -> list[str]:
    return [f"{prefix}/obs.parquet", f"{prefix}/X.h5ad", f"{prefix}/var.parquet"]


def duplicate_keys(ln: Any, prefix: str) -> list[str]:
    keys = artifact_keys(prefix)
    return sorted([a.key for a in ln.Artifact.filter(key__in=keys).all() if a.key])


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {found[0].dtype!r}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def dataframe_from_backed(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.index = out.index.astype(str)
    out = out.loc[:, ~out.columns.duplicated(keep="first")]
    for col in out.columns:
        if pd.api.types.is_categorical_dtype(out[col]):
            out[col] = out[col].astype(str).replace("nan", pd.NA)
    return out


def add_spatial_columns(obs: pd.DataFrame, adata: ad.AnnData) -> dict[str, Any]:
    obsm_keys = list(adata.obsm.keys())
    policy: dict[str, Any] = {"obsm_keys": obsm_keys, "spatial_columns_added": []}
    if "spatial" in obsm_keys:
        spatial = np.asarray(adata.obsm["spatial"])
        policy["spatial_shape"] = list(spatial.shape)
        if spatial.ndim == 2 and spatial.shape[0] == obs.shape[0] and spatial.shape[1] >= 2:
            obs["spatial_x"] = spatial[:, 0]
            obs["spatial_y"] = spatial[:, 1]
            policy["spatial_columns_added"] = ["spatial_x", "spatial_y"]
            if spatial.shape[1] >= 3:
                obs["spatial_z"] = spatial[:, 2]
                policy["spatial_columns_added"].append("spatial_z")
    return policy


def enrich_obs(obs: pd.DataFrame) -> pd.DataFrame:
    obs = obs.copy()
    obs["dataset"] = "STDS0000060"
    obs["source_accession"] = "STDS0000060"
    obs["sample"] = "L3_b_count_normal_stereoseq"
    obs["developmental_stage"] = "L3"
    obs["organism"] = "Drosophila melanogaster"
    obs["assay"] = "Stereo-seq spatial transcriptomics"
    obs["technology"] = "Stereo-seq"
    obs["modality"] = "spatial_transcriptomics"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["source_url"] = URL
    if obs.index.name is None:
        obs.index.name = "cell_id"
    return obs


def enrich_var(var: pd.DataFrame) -> pd.DataFrame:
    var = var.copy()
    var["source_accession"] = "STDS0000060"
    var["organism"] = "Drosophila melanogaster"
    if "gene_symbol" not in var.columns:
        var["gene_symbol"] = var.index.astype(str)
    if var.index.name is None:
        var.index.name = "gene_id"
    return var


def inspect_h5ad(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    adata = ad.read_h5ad(path, backed="r")
    try:
        obs = dataframe_from_backed(adata.obs)
        var = dataframe_from_backed(adata.var)
        spatial_policy = add_spatial_columns(obs, adata)
        smoke = {
            "shape": [int(adata.n_obs), int(adata.n_vars)],
            "obs_columns_before_enrichment": list(adata.obs.columns),
            "var_columns_before_enrichment": list(adata.var.columns),
            "obs_index_name": adata.obs_names.name,
            "var_index_name": adata.var_names.name,
            "obsm_policy": spatial_policy,
            "x_backed_type": type(adata.X).__name__,
        }
    finally:
        adata.file.close()
    obs = enrich_obs(obs)
    var = enrich_var(var)
    smoke["obs_columns_after_enrichment"] = list(obs.columns)
    smoke["var_columns_after_enrichment"] = list(var.columns)
    return obs, var, smoke


def write_triplet(ln: Any, h5ad_path: Path, obs: pd.DataFrame, var: pd.DataFrame) -> dict[str, Any]:
    before = duplicate_keys(ln, PREFIX)
    if before:
        raise RuntimeError(f"Refusing overwrite; exact keys exist: {before}")
    with tempfile.TemporaryDirectory(prefix="stds0000060_l3b_") as td:
        # Keep a tempdir alive while Lamin materializes/uploads the source H5AD.
        # obs/var use from_dataframe to match neighbouring ingestion scripts and
        # to allow skip_hash_lookup on the var parquet.
        _scratch = Path(td)
        obs_art = ln.Artifact.from_dataframe(obs, key=f"{PREFIX}/obs.parquet").save()
        x_art = ln.Artifact.from_anndata(str(h5ad_path), key=f"{PREFIX}/X.h5ad").save()
        var_art = ln.Artifact.from_dataframe(var, key=f"{PREFIX}/var.parquet", skip_hash_lookup=True).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_key": obs_art.key, "x_key": x_art.key, "var_key": var_art.key}


def verify_prefix(ln: Any, prefix: str) -> dict[str, Any]:
    keys = duplicate_keys(ln, prefix)
    out: dict[str, Any] = {"prefix": prefix, "keys": keys, "exists_all": sorted(keys) == sorted(artifact_keys(prefix))}
    if out["exists_all"]:
        obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
        x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
        var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
        obs = obs_art.load()
        var = var_art.load()
        out.update({
            "obs_key": obs_art.key,
            "x_key": x_art.key,
            "var_key": var_art.key,
            "obs_rows": int(obs.shape[0]),
            "obs_cols": list(obs.columns),
            "var_rows": int(var.shape[0]),
            "var_cols": list(var.columns),
            "x_n_observations": int(x_art.n_observations or 0),
            "link_ok": x_art.key == f"{prefix}/X.h5ad" and var_art.key == f"{prefix}/var.parquet",
        })
    return out


def write_reports(out: dict[str, Any]) -> None:
    ARTIFACT_JSON.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_JSON.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# STDS0000060 L3_b Stereo-seq Lamin ingestion report",
        "",
        f"Generated: {out['generated_at']}",
        f"Task: {TASK_ID}",
        f"Host: {out['host']}",
        f"Dry run: {out['dry_run']}",
        f"Write: {out['write']}",
        f"Source URL: {URL}",
        f"Target prefix: `{PREFIX}`",
        "",
        "## Duplicate gates",
        f"- target before: `{out.get('target_existing_before')}`",
        f"- prior STDS0000060 prefixes untouched before: `{out.get('prior_existing_before')}`",
        f"- target after: `{out.get('target_existing_after')}`",
        f"- prior STDS0000060 prefixes untouched after: `{out.get('prior_existing_after')}`",
        "",
        "## Smoke",
        "```json",
        json.dumps(out.get("smoke", {}), indent=2, sort_keys=True),
        "```",
        "",
        "## Write/readback",
        "```json",
        json.dumps(out.get("write_result", {}), indent=2, sort_keys=True),
        json.dumps(out.get("readback", {}), indent=2, sort_keys=True),
        "```",
        "",
        "## Residual risks",
        "- X.h5ad is the source processed H5AD registered as the matrix payload to avoid full matrix materialization; obs/var are separate enriched parquet artifacts and Lamin links resolve obs -> X -> var.",
        "- This report does not claim full STDS0000060 completion, Collection membership, or model-ready promotion.",
    ]
    ARTIFACT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--work-dir", type=Path, default=Path("data/vm_stage/stds0000060/L3_b"))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", action="store_true")
    p.add_argument("--verify-only", action="store_true")
    args = p.parse_args()

    ensure_project_cache()
    host = socket.gethostname()
    if EXPECTED_HOST not in host:
        raise RuntimeError(f"Refusing to run on host {host!r}; expected {EXPECTED_HOST}")

    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    if args.write:
        ln.track(path="tools/ingest_stds0000060_l3b_stereoseq_vm.py")
        ensure_link_features(ln)

    out: dict[str, Any] = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "host": host,
        "dry_run": args.dry_run,
        "write": args.write,
        "verify_only": args.verify_only,
        "source_url": URL,
        "target_prefix": PREFIX,
        "expected_source_length": EXPECTED_LENGTH,
        "lamin_instance": ln.setup.settings.instance.slug,
        "lamin_branch": ln.setup.settings.branch.name,
        "target_existing_before": duplicate_keys(ln, PREFIX),
        "prior_existing_before": {prefix: duplicate_keys(ln, prefix) for prefix in PRIOR_PREFIXES},
    }
    if out["target_existing_before"] and args.write:
        out["status"] = "blocked_duplicate_target"
        write_reports(out)
        raise RuntimeError(f"Target keys already exist before write: {out['target_existing_before']}")

    if args.verify_only:
        out["readback"] = verify_prefix(ln, PREFIX)
        out["prior_readback"] = {prefix: verify_prefix(ln, prefix) for prefix in PRIOR_PREFIXES}
        out["target_existing_after"] = duplicate_keys(ln, PREFIX)
        out["prior_existing_after"] = {prefix: duplicate_keys(ln, prefix) for prefix in PRIOR_PREFIXES}
        out["status"] = "verified" if out["readback"].get("link_ok") else "verify_missing_or_bad"
        write_reports(out)
        print(json.dumps(out, indent=2, sort_keys=True))
        return

    out["head"] = curl_head()
    if out["head"].get("headers", {}).get("accept-ranges") != "bytes":
        out["status"] = "blocked_no_accept_ranges"
        write_reports(out)
        raise RuntimeError("Source does not advertise Accept-Ranges: bytes")

    h5ad_path = args.work_dir / FILENAME
    out["download"] = curl_download(h5ad_path)
    if out["download"]["status"] not in {"exists", "downloaded"}:
        out["status"] = "download_failed"
        write_reports(out)
        raise RuntimeError(f"Download failed: {out['download']}")

    obs, var, smoke = inspect_h5ad(h5ad_path)
    out["smoke"] = smoke
    out["prepared_shapes"] = {"obs": list(obs.shape), "var": list(var.shape)}

    if args.write:
        out["write_result"] = write_triplet(ln, h5ad_path, obs, var)
        out["readback"] = verify_prefix(ln, PREFIX)
        out["status"] = "written_and_verified" if out["readback"].get("link_ok") else "written_but_verify_failed"
    else:
        out["status"] = "dry_run_complete"

    out["target_existing_after"] = duplicate_keys(ln, PREFIX)
    out["prior_existing_after"] = {prefix: duplicate_keys(ln, prefix) for prefix in PRIOR_PREFIXES}
    write_reports(out)
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
