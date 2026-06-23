#!/usr/bin/env python3
"""Backed probe for staged PRISM GSE269596 without loading the full matrix."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.gcs_cache import ensure_gcs_object_local  # noqa: E402
from tools.ingest_prism_large_h5ad_chunks import standardize_prism_obs_df  # noqa: E402

DATASET = "GSE269596"
SOURCE_URI = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE269596.h5ad"
EXPECTED_BYTES = 2_505_068_264
OUT = ROOT / "artifacts/schema_audit/prism_GSE269596_source_probe_20260623.json"


def dataset2d_type(x: Any) -> str:
    return f"{type(x).__module__}.{type(x).__name__}"


def main() -> int:
    local = ensure_gcs_object_local(SOURCE_URI, cache_root=ROOT / "data/gcs_cache")
    size = local.stat().st_size
    if size != EXPECTED_BYTES:
        raise RuntimeError(f"cached byte mismatch: {size} != {EXPECTED_BYTES}")
    source = ad.read_h5ad(local, backed="r")
    try:
        obs_cols = list(source.obs.columns)
        var_cols = list(source.var.columns)
        obs_std = standardize_prism_obs_df(source.obs.copy(), DATASET)
        result = {
            "dataset": DATASET,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_uri": SOURCE_URI,
            "expected_gcs_bytes": EXPECTED_BYTES,
            "local_path": str(local),
            "local_bytes": size,
            "n_obs": int(source.n_obs),
            "n_vars": int(source.n_vars),
            "x_backed_type": dataset2d_type(source.X),
            "x_dtype": str(getattr(source.X, "dtype", "unknown")),
            "obs_index_unique": bool(source.obs_names.is_unique),
            "var_index_unique": bool(source.var_names.is_unique),
            "obsm_keys": list(source.obsm.keys()),
            "layers": list(source.layers.keys()),
            "raw_present": source.raw is not None,
            "obs_columns": obs_cols,
            "var_columns": var_cols,
            "standardized_required_obs_fields": {
                field: field in obs_std.columns
                for field in [
                    "perturbation",
                    "is_control",
                    "cell_line",
                    "organism",
                    "perturbation_type",
                    "dataset",
                    "modality",
                    "assay",
                ]
            },
            "controls_after_standardization": int(obs_std["is_control"].sum()) if "is_control" in obs_std.columns else None,
            "unique_perturbations_sample": sorted(obs_std["perturbation"].astype(str).unique()[:20].tolist()) if "perturbation" in obs_std.columns else [],
            "recommended_chunk_size": 1000,
            "recommendation": "safe_to_smoke_chunk_with_backed_reader",
        }
    finally:
        source.file.close()
    OUT.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    print(json.dumps({k: result[k] for k in ["dataset", "local_bytes", "n_obs", "n_vars", "x_backed_type", "x_dtype", "controls_after_standardization", "recommended_chunk_size"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
