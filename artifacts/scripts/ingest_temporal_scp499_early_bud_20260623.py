#!/usr/bin/env python3
"""Recover SCP499 Early-Bud sidecar equivalents and ingest the expression matrix.

SCP499's direct study-file download endpoints can return HTTP 401 even though the
public visualization APIs expose the same usable contract:
- annotations/Cluster/cell_values -> cell idents
- clusters/tSNE coordinates -> cells, x, y, annotations

This script stages those API-derived sidecars under an explicit api_derived GCS
prefix, verifies remote byte counts, parses the already-staged EB.matrix.txt.gz
without dense-loading, writes a same-prefix obs/X/var Lamin triplet, and verifies
obs -> X -> var links.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import quote

import anndata as ad
import numpy as np
import pandas as pd
import requests
from scipy import sparse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.lamin_context import connect_pertdata  # noqa: E402

SCP = "SCP499"
PREFIX = "temporal_pretraining/gse121737_axolotl_blastema/early_bud_blastema"
MATRIX_GCS = "gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499/EB.matrix.txt.gz"
DERIVED_GCS_PREFIX = "gs://scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499/api_derived"
BARCODE_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE121nnn/GSE121737/suppl/GSE121737_Early_bud_and_medium_bud_cell_name_and_barcode.txt.gz"
CACHE = ROOT / "data/gcs_cache/scperturb/pert-gym/staging/browser_auth_scp/2026-06-22/SCP499"
WORK = ROOT / "data/scp499_tmp"
STATUS_JSON = ROOT / "artifacts/schema_audit/temporal_scp499_early_bud_ingestion_20260623.json"
STATUS_MD = ROOT / "artifacts/schema_audit/temporal_scp499_early_bud_ingestion_20260623.md"


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S %Z")


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.replace(path)


def run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, text=True, capture_output=True, check=True)
    return p.stdout


def gcs_size(uri: str) -> int:
    out = run(["gsutil", "ls", "-l", uri])
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[-1] == uri:
            return int(parts[0])
    raise RuntimeError(f"could not parse gsutil size for {uri}: {out}")


def ensure_matrix_cache() -> Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    dest = CACHE / "EB.matrix.txt.gz"
    expected = gcs_size(MATRIX_GCS)
    if dest.exists() and dest.stat().st_size == expected:
        return dest
    tmp = dest.with_suffix(dest.suffix + ".partial")
    if tmp.exists():
        tmp.unlink()
    run(["gsutil", "cp", MATRIX_GCS, str(tmp)])
    if tmp.stat().st_size != expected:
        raise AssertionError(f"matrix cache size mismatch: {tmp.stat().st_size} != {expected}")
    tmp.replace(dest)
    return dest


def fetch_api_sidecars() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    WORK.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    idents_url = f"https://singlecell.broadinstitute.org/single_cell/api/v1/studies/{SCP}/annotations/Cluster/cell_values"
    idents_r = s.get(idents_url, params={"annotation_type": "group", "annotation_scope": "study"}, timeout=60)
    idents_r.raise_for_status()
    idents_path = WORK / "EB.idents.api_cell_values.txt"
    idents_path.write_text(idents_r.text)
    idents_df = pd.read_csv(idents_path, sep="\t")
    if idents_df.columns.tolist() != ["NAME", "Cluster"]:
        raise AssertionError(f"unexpected idents columns: {idents_df.columns.tolist()}")

    cluster_name = quote("tSNE coordinates", safe="")
    coords_url = f"https://singlecell.broadinstitute.org/single_cell/api/v1/studies/{SCP}/clusters/{cluster_name}"
    coords_r = s.get(coords_url, timeout=90)
    coords_r.raise_for_status()
    cluster_json = coords_r.json()
    (WORK / "scp499_tsne_cluster_api_live.json").write_text(json.dumps(cluster_json, indent=2))
    data = cluster_json["data"]
    coords_df = pd.DataFrame({
        "NAME": data["cells"],
        "tSNE_1": data["x"],
        "tSNE_2": data["y"],
        "Cluster": data["annotations"],
    })
    coords_path = WORK / "EB.coordinates.api_cluster.tsv"
    coords_df.to_csv(coords_path, sep="\t", index=False)

    for label, df in {"idents": idents_df, "coords": coords_df}.items():
        if df["NAME"].duplicated().any():
            raise AssertionError(f"duplicate cells in {label}")
    if idents_df["NAME"].tolist() != coords_df["NAME"].tolist():
        raise AssertionError("idents and coords cell orders differ")
    if idents_df["Cluster"].tolist() != coords_df["Cluster"].tolist():
        raise AssertionError("idents and coords Cluster labels differ")

    uploads = {}
    for local, remote_name in [
        (idents_path, "EB.idents.api_cell_values.txt"),
        (coords_path, "EB.coordinates.api_cluster.tsv"),
        (WORK / "scp499_tsne_cluster_api_live.json", "scp499_tsne_cluster_api_live.json"),
    ]:
        uri = f"{DERIVED_GCS_PREFIX}/{remote_name}"
        run(["gsutil", "cp", str(local), uri])
        remote_size = gcs_size(uri)
        local_size = local.stat().st_size
        if remote_size != local_size:
            raise AssertionError(f"GCS size mismatch for {uri}: {remote_size} != {local_size}")
        uploads[remote_name] = {"gcs_uri": uri, "bytes": local_size, "remote_bytes": remote_size}

    meta = {
        "idents_url": idents_url,
        "coords_url": coords_url,
        "rows": int(len(idents_df)),
        "cluster_counts": Counter(idents_df["Cluster"]).most_common(),
        "uploads": uploads,
        "note": "API-derived sidecars; direct original study-file download endpoints historically returned HTTP 401.",
    }
    return idents_df, coords_df, meta


def ensure_barcode_map() -> tuple[dict[str, str], dict[str, Any]]:
    dest = WORK / "gse121737" / "GSE121737_Early_bud_and_medium_bud_cell_name_and_barcode.txt.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists() or dest.stat().st_size == 0:
        r = requests.get(BARCODE_URL, stream=True, timeout=60)
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".partial")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)
    out: dict[str, str] = {}
    with gzip.open(dest, "rt") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                out[parts[0]] = parts[1]
    return out, {"path": str(dest), "bytes": dest.stat().st_size, "entries": len(out), "source_url": BARCODE_URL}


def read_matrix_header(path: Path) -> list[str]:
    with gzip.open(path, "rt") as f:
        header = f.readline().rstrip("\n").split("\t")
    if not header or header[0] != "GENE":
        raise AssertionError(f"unexpected matrix first column: {header[:3]}")
    return header[1:]


def parse_matrix(path: Path, expected_cells: list[str], max_genes: int | None = None) -> tuple[ad.AnnData, pd.DataFrame, dict[str, Any]]:
    header_cells = read_matrix_header(path)
    if header_cells != expected_cells:
        missing = sorted(set(expected_cells) - set(header_cells))[:10]
        extra = sorted(set(header_cells) - set(expected_cells))[:10]
        raise AssertionError(f"matrix/API cell contract mismatch: same_order={header_cells == expected_cells} missing={missing} extra={extra}")
    data: list[float] = []
    rows: list[int] = []
    cols: list[int] = []
    genes: list[str] = []
    t0 = time.time()
    with gzip.open(path, "rt") as f:
        _header = f.readline()
        for gene_idx, line in enumerate(f):
            if max_genes is not None and gene_idx >= max_genes:
                break
            parts = line.rstrip("\n").split("\t")
            if len(parts) != len(header_cells) + 1:
                raise AssertionError(f"row width mismatch at gene {gene_idx}: {len(parts)} vs {len(header_cells)+1}")
            genes.append(parts[0])
            for cell_idx, val in enumerate(parts[1:]):
                if val and val != "0" and val != "0.0":
                    x = float(val)
                    if x != 0.0:
                        rows.append(cell_idx)
                        cols.append(gene_idx)
                        data.append(x)
            if (gene_idx + 1) % 10000 == 0:
                print(f"PARSE genes={gene_idx+1} nnz={len(data)} elapsed={time.time()-t0:.1f}s", flush=True)
    x = sparse.csr_matrix((np.asarray(data, dtype=np.float32), (rows, cols)), shape=(len(header_cells), len(genes)), dtype=np.float32)
    var = pd.DataFrame(index=pd.Index(genes, name="gene_symbol"))
    var["feature_id"] = genes
    var["gene_symbol"] = genes
    var["feature_name"] = genes
    var["feature_type"] = "Gene Expression"
    x_ad = ad.AnnData(X=x, obs=pd.DataFrame(index=pd.Index([f"{SCP}:{c}" for c in header_cells], name="obs_id")), var=pd.DataFrame(index=var.index))
    meta = {
        "matrix_path": str(path),
        "cells": len(header_cells),
        "genes": len(genes),
        "nnz": int(x.nnz),
        "density": float(x.nnz / (x.shape[0] * x.shape[1])) if x.shape[0] and x.shape[1] else 0.0,
        "elapsed_seconds": round(time.time() - t0, 3),
        "max_genes": max_genes,
    }
    return x_ad, var, meta


def make_obs(cells: list[str], idents: pd.DataFrame, coords: pd.DataFrame, barcode_map: dict[str, str]) -> pd.DataFrame:
    rows = []
    for name, cluster, x, y in zip(idents["NAME"], idents["Cluster"], coords["tSNE_1"], coords["tSNE_2"]):
        rows.append({
            "cell_id": name,
            "scp_cell_name": name,
            "barcode": barcode_map.get(name),
            "cell_type": cluster,
            "cluster": cluster,
            "tSNE_1": float(x),
            "tSNE_2": float(y),
            "timepoint": "early_bud_blastema",
            "raw_time_label": "Early-Bud Blastema",
            "trajectory_id": "axolotl_limb_regeneration_gse121737",
            "organism": "Ambystoma mexicanum",
            "tissue": "limb",
            "disease": "normal",
            "modality": "scRNA-seq",
            "assay": "Drop-seq-like single-cell RNA-seq",
            "source_dataset_id": "GSE121737",
            "source_project_accession": SCP,
            "source_sample_accession": "early_bud_blastema",
            "source_file": "EB.matrix.txt.gz",
            "sidecar_source": "SCP public API: Cluster cell_values + tSNE coordinates cluster endpoint",
            "perturbation": "control",
            "perturbation_type": "none",
            "is_control": True,
        })
    return pd.DataFrame(rows, index=pd.Index([f"{SCP}:{c}" for c in cells], name="obs_id"))


def ensure_link_features(ln: Any) -> None:
    for name in ["X", "var"]:
        if not list(ln.Feature.filter(name=name).all()):
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    hits = list(ln.Artifact.filter(key=value).all())
    if hits:
        return hits[-1]
    hits = list(ln.Artifact.filter(uid=value).all())
    if hits:
        return hits[-1]
    raise KeyError(value)


def duplicate_probe(ln: Any) -> dict[str, Any]:
    planned = [f"{PREFIX}/{x}" for x in ["obs.parquet", "X.h5ad", "var.parquet"]]
    exact = [a.key for a in ln.Artifact.filter(key__in=planned).all() if a.key]
    terms = ["SCP499", "early_bud_blastema", "gse121737_axolotl_blastema", "Early-Bud Blastema"]
    term_hits = {t: [a.key for a in ln.Artifact.filter(key__icontains=t).all()[:40] if a.key] for t in terms}
    return {"planned_keys": planned, "exact_planned_key_hits": exact, "term_hits": term_hits}


def verify_triplet(ln: Any) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{PREFIX}/obs.parquet")
    x_art = ln.Artifact.get(key=f"{PREFIX}/X.h5ad")
    var_art = ln.Artifact.get(key=f"{PREFIX}/var.parquet")
    linked_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
    linked_var = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs_df = obs_art.load()
    var_df = var_art.load()
    x_ad = x_art.load()
    required = ["timepoint", "raw_time_label", "trajectory_id", "organism", "source_dataset_id", "source_project_accession", "cell_type", "tSNE_1", "tSNE_2"]
    missing = [c for c in required if c not in obs_df.columns]
    if missing:
        raise AssertionError(f"missing obs fields: {missing}")
    if linked_x.key != x_art.key or linked_var.key != var_art.key:
        raise AssertionError(f"feature link mismatch: {linked_x.key} {linked_var.key}")
    if len(obs_df) != 2013 or x_ad.n_obs != 2013:
        raise AssertionError(f"obs/X count mismatch: {len(obs_df)} {x_ad.n_obs}")
    if len(var_df) != 59171 or x_ad.n_vars != 59171:
        raise AssertionError(f"var/X count mismatch: {len(var_df)} {x_ad.n_vars}")
    return {
        "prefix": PREFIX,
        "payload_exists": {"obs": bool(obs_art.path.exists()), "X": bool(x_art.path.exists()), "var": bool(var_art.path.exists())},
        "obs_rows": int(len(obs_df)),
        "var_rows": int(len(var_df)),
        "x_shape": [int(x_ad.n_obs), int(x_ad.n_vars)],
        "x_nnz": int(x_ad.X.nnz) if sparse.issparse(x_ad.X) else None,
        "linked_x_key": linked_x.key,
        "linked_var_key": linked_var.key,
        "cluster_counts": obs_df["cluster"].value_counts().to_dict(),
    }


def write_triplet(ln: Any, x_ad: ad.AnnData, obs: pd.DataFrame, var: pd.DataFrame) -> dict[str, Any]:
    planned = [f"{PREFIX}/{x}" for x in ["obs.parquet", "X.h5ad", "var.parquet"]]
    existing = [a.key for a in ln.Artifact.filter(key__in=planned).all() if a.key]
    if existing:
        return {"write": "skipped_existing", "existing": existing, "verification": verify_triplet(ln)}
    with tempfile.TemporaryDirectory(prefix="scp499_early_bud_") as tmp0:
        tmp = Path(tmp0)
        obs_path = tmp / "obs.parquet"
        x_path = tmp / "X.h5ad"
        var_path = tmp / "var.parquet"
        obs.to_parquet(obs_path)
        var.to_parquet(var_path)
        x_ad.write_h5ad(x_path, compression="gzip")
        print(f"LAMIN_SAVE obs bytes={obs_path.stat().st_size}", flush=True)
        obs_art = ln.Artifact.from_dataframe(obs_path, key=planned[0], description="Temporal pretraining SCP499/GSE121737 axolotl early-bud blastema obs from public SCP API sidecars", skip_hash_lookup=True).save()
        print(f"LAMIN_SAVE X bytes={x_path.stat().st_size}", flush=True)
        x_art = ln.Artifact.from_anndata(x_path, key=planned[1], description="Temporal pretraining SCP499/GSE121737 axolotl early-bud blastema expression matrix from browser-auth staged EB.matrix.txt.gz", skip_hash_lookup=True).save()
        print(f"LAMIN_SAVE var bytes={var_path.stat().st_size}", flush=True)
        var_art = ln.Artifact.from_dataframe(var_path, key=planned[2], description="Temporal pretraining SCP499/GSE121737 axolotl early-bud blastema feature metadata", skip_hash_lookup=True).save()
        x_art.features.set_values({"var": var_art})
        obs_art.features.set_values({"X": x_art})
    return {"write": {"obs_key": planned[0], "x_key": planned[1], "var_key": planned[2]}, "verification": verify_triplet(ln)}


def write_markdown(status: dict[str, Any]) -> None:
    write_state = status.get("write", {})
    v = write_state.get("verification") if isinstance(write_state, dict) else {}
    v = v or status.get("verification") or {}
    side = status.get("api_sidecars", {})
    lines = [
        "# SCP499 early-bud blastema ingestion — 2026-06-23",
        "",
        "## Summary",
        "",
        "Recovered SCP499 idents/coordinates through public SCP visualization APIs because direct study-file endpoints returned HTTP 401 in prior probes. The API-derived sidecars were staged under an explicit `api_derived/` GCS prefix with byte-count verification, then the already-staged EB.matrix.txt.gz was ingested as a same-prefix obs/X/var triplet.",
        "",
        "## Contract",
        "",
        f"- Matrix source: `{MATRIX_GCS}`",
        f"- Derived sidecar prefix: `{DERIVED_GCS_PREFIX}`",
        f"- Sidecar rows: {side.get('rows')}",
        f"- Matrix parse: {status.get('matrix_parse')}",
        f"- Lamin prefix: `{PREFIX}`",
        f"- Verification: `{v}`",
        "",
        "## Caveat",
        "",
        "The original SCP files `EB.idents.txt` and `EB.coordinates.txt` were not recovered byte-for-byte. Instead, their equivalent public API payloads (`Cluster/cell_values` and `clusters/tSNE coordinates`) were staged with explicit API-derived names and used for obs annotations/coordinates.",
        "",
    ]
    STATUS_MD.write_text("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-genes", type=int, default=None)
    args = ap.parse_args()
    if args.max_genes is not None:
        args.dry_run = True

    status: dict[str, Any] = {"generated_at": now(), "dry_run": args.dry_run, "max_genes": args.max_genes}
    atomic_json(STATUS_JSON, status)

    matrix = ensure_matrix_cache()
    status["matrix_cache"] = {"path": str(matrix), "bytes": matrix.stat().st_size, "gcs_uri": MATRIX_GCS, "gcs_bytes": gcs_size(MATRIX_GCS)}
    atomic_json(STATUS_JSON, status)

    idents, coords, side_meta = fetch_api_sidecars()
    status["api_sidecars"] = side_meta
    atomic_json(STATUS_JSON, status)

    barcode_map, barcode_meta = ensure_barcode_map()
    status["barcode_map"] = barcode_meta
    atomic_json(STATUS_JSON, status)

    cells = idents["NAME"].tolist()
    x_ad, var, matrix_meta = parse_matrix(matrix, cells, max_genes=args.max_genes)
    obs = make_obs(cells, idents, coords, barcode_map)
    status["matrix_parse"] = matrix_meta
    status["obs_contract"] = {
        "obs_rows": int(len(obs)),
        "barcode_present": int(obs["barcode"].notna().sum()),
        "cluster_counts": obs["cluster"].value_counts().to_dict(),
    }
    atomic_json(STATUS_JSON, status)

    print("CONNECT Lamin", flush=True)
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    try:
        ln.track()
        status["ln_track"] = "ok"
    except Exception as e:  # recovery scripts should not corrupt/half-init on provenance friction
        status["ln_track"] = f"failed_nonfatal:{e!r}"
    ensure_link_features(ln)
    status["duplicate_probe_before_write"] = duplicate_probe(ln)
    atomic_json(STATUS_JSON, status)
    if status["duplicate_probe_before_write"]["exact_planned_key_hits"] and not args.dry_run:
        raise RuntimeError(f"planned keys already exist: {status['duplicate_probe_before_write']['exact_planned_key_hits']}")

    if args.dry_run:
        status["write"] = "dry_run_skipped"
    else:
        status["write"] = write_triplet(ln, x_ad, obs, var)
    status["completed_at"] = now()
    atomic_json(STATUS_JSON, status)
    write_markdown(status)
    print(json.dumps(status, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
