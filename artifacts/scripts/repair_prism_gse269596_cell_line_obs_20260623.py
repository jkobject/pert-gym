#!/usr/bin/env python3
"""Repair GSE269596 chunk obs metadata so source cellline is preserved as cell_line.

This is intentionally obs-only: it opens the staged h5ad in backed mode, reads
source obs metadata, creates revised obs.parquet artifacts for each existing
chunk, and links each revised obs artifact back to the already-ingested X.h5ad.
It does not read or rewrite the expression matrix.
"""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.gcs_cache import ensure_gcs_object_local  # noqa: E402
from tools.ingest_prism_large_h5ad_chunks import standardize_prism_obs_df  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

DATASET = "GSE269596"
SOURCE_URI = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE269596.h5ad"
EXPECTED_BYTES = 2_505_068_264
CHUNK_SIZE = 1000
PREFIX = f"prism_collection/{DATASET}"
OUT = ROOT / "artifacts/schema_audit/prism_GSE269596_cell_line_obs_repair_20260623.json"


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def main() -> int:
    ensure_project_cache()
    local = ensure_gcs_object_local(SOURCE_URI, cache_root=ROOT / "data/gcs_cache")
    size = local.stat().st_size
    if size != EXPECTED_BYTES:
        raise RuntimeError(f"cached byte mismatch: {size} != {EXPECTED_BYTES}")

    source = ad.read_h5ad(local, backed="r")
    try:
        n_obs = int(source.n_obs)
        expected_chunks = math.ceil(n_obs / CHUNK_SIZE)
        obs_all = standardize_prism_obs_df(source.obs.copy(), DATASET)
    finally:
        source.file.close()

    if "cell_line" not in obs_all.columns:
        raise RuntimeError("standardized source obs has no cell_line column")
    non_unknown_total = int(obs_all["cell_line"].astype(str).ne("unknown").sum())
    if non_unknown_total == 0:
        raise RuntimeError("standardized source cell_line is all unknown; refusing repair")

    ln = connect_pertdata()
    ln.track(path="artifacts/scripts/repair_prism_gse269596_cell_line_obs_20260623.py")
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    repaired_chunks: list[dict[str, Any]] = []
    for chunk_i in range(expected_chunks):
        start = chunk_i * CHUNK_SIZE
        end = min(n_obs, start + CHUNK_SIZE)
        chunk_prefix = f"{PREFIX}/chunk_{chunk_i:04d}"
        obs_key = f"{chunk_prefix}/obs.parquet"

        prev_obs = list(ln.Artifact.filter(key=obs_key).all())
        if not prev_obs:
            raise RuntimeError(f"missing existing obs artifact: {obs_key}")
        old_obs_art = prev_obs[-1]
        x_art = resolve_artifact(ln, old_obs_art.features.get_values()["X"])
        if x_art.key != f"{chunk_prefix}/X.h5ad":
            raise RuntimeError(f"unexpected X link for {obs_key}: {x_art.key}")

        obs_df = obs_all.iloc[start:end].copy()
        repaired_obs_art = ln.Artifact.from_dataframe(
            obs_df,
            key=obs_key,
            revises=old_obs_art,
        ).save()
        repaired_obs_art.features.set_values({"X": x_art})

        cell_line_values = obs_df["cell_line"].astype(str)
        repaired_chunks.append(
            {
                "chunk": chunk_i,
                "prefix": chunk_prefix,
                "obs_key": obs_key,
                "old_obs_uid": old_obs_art.uid,
                "new_obs_uid": repaired_obs_art.uid,
                "x_uid": x_art.uid,
                "rows": int(len(obs_df)),
                "cell_line_non_unknown": int(cell_line_values.ne("unknown").sum()),
                "cell_line_unique_sample": sorted(cell_line_values.unique().tolist())[:20],
            }
        )
        print(
            "REPAIRED_OBS",
            chunk_prefix,
            "rows",
            len(obs_df),
            "non_unknown_cell_line",
            repaired_chunks[-1]["cell_line_non_unknown"],
            flush=True,
        )

    result = {
        "dataset": DATASET,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_uri": SOURCE_URI,
        "local_path": str(local),
        "local_bytes": size,
        "chunk_size": CHUNK_SIZE,
        "expected_rows": n_obs,
        "expected_chunks": expected_chunks,
        "chunks_repaired": len(repaired_chunks),
        "cell_line_non_unknown_total": non_unknown_total,
        "cell_line_unique_sample": sorted(obs_all["cell_line"].astype(str).unique().tolist())[:50],
        "obs_only_no_x_rewrite": True,
        "ok": len(repaired_chunks) == expected_chunks,
        "chunks": repaired_chunks,
    }
    OUT.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    print(
        json.dumps(
            {
                "dataset": result["dataset"],
                "ok": result["ok"],
                "chunks_repaired": result["chunks_repaired"],
                "cell_line_non_unknown_total": result["cell_line_non_unknown_total"],
                "cell_line_unique_sample": result["cell_line_unique_sample"],
            },
            indent=2,
        )
    )
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
