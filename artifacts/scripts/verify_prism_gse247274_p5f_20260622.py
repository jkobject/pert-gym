#!/usr/bin/env python3
"""Verify GSE247274 P5F PRISM chunked Lamin triplets and update local status artifacts."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata  # noqa: E402

DATASET = "GSE247274"
PREFIX = f"prism_collection/{DATASET}"
GCS_URI = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247274.h5ad"
CHUNK_SIZE = 1000
N_OBS = 69907
N_VARS = 22977
N_CHUNKS = 70
REQUIRED_OBS = ["perturbation", "perturbation_type", "organism", "cell_line", "modality", "assay", "is_control"]


def resolve_artifact(ln, value):
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def upsert(items, key, entry):
    items[:] = [item for item in items if item.get(key) != entry.get(key)]
    items.append(entry)


def main() -> int:
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    chunks = []
    total_rows = 0
    total_controls = 0
    all_required = True
    sampled = []

    for i in range(N_CHUNKS):
        start = i * CHUNK_SIZE
        end = min(N_OBS, start + CHUNK_SIZE)
        chunk_prefix = f"{PREFIX}/chunk_{i:04d}"
        obs = ln.Artifact.get(key=f"{chunk_prefix}/obs.parquet")
        x_val = obs.features.get_values().get("X")
        x = resolve_artifact(ln, x_val)
        var_val = x.features.get_values().get("var")
        var = resolve_artifact(ln, var_val)
        expected_x = f"{chunk_prefix}/X.h5ad"
        expected_var = f"{chunk_prefix}/var.parquet"
        x_path_exists = bool(x.path.exists())
        obs_df = obs.load()
        var_df = var.load()
        req = {col: col in obs_df.columns for col in REQUIRED_OBS}
        rows = len(obs_df)
        vars_ = len(var_df)
        controls = int(obs_df["is_control"].sum()) if "is_control" in obs_df.columns else 0
        ok = (
            x.key == expected_x
            and var.key == expected_var
            and x_path_exists
            and int(x.n_observations or -1) == rows == (end - start)
            and vars_ == N_VARS
            and all(req.values())
        )
        if not ok:
            raise RuntimeError(
                f"chunk {i:04d} failed: x={x.key} var={var.key} exists={x_path_exists} "
                f"rows={rows} x_nobs={x.n_observations} vars={vars_} req={req}"
            )
        chunks.append({
            "prefix": chunk_prefix,
            "start": start,
            "end": end,
            "obs_rows": rows,
            "x_n_observations": int(x.n_observations),
            "var_rows": vars_,
            "controls": controls,
            "status": "verified",
        })
        total_rows += rows
        total_controls += controls
        all_required = all_required and all(req.values())
        if i in {0, 1, N_CHUNKS - 1}:
            sampled.append({
                "chunk": i,
                "obs_key": obs.key,
                "x_key": x.key,
                "var_key": var.key,
                "obs_rows": rows,
                "var_rows": vars_,
                "controls": controls,
                "required_obs": req,
            })

    if total_rows != N_OBS:
        raise RuntimeError(f"row total mismatch: {total_rows} != {N_OBS}")

    now = datetime.now(timezone.utc).isoformat()
    verification = {
        "dataset": DATASET,
        "prefix": PREFIX,
        "gcs_uri": GCS_URI,
        "verified_at_utc": now,
        "lamin_instance": ln.setup.settings.instance.slug,
        "lamin_branch": ln.setup.settings.branch.name,
        "n_obs": N_OBS,
        "n_vars": N_VARS,
        "chunk_size": CHUNK_SIZE,
        "chunks_expected": N_CHUNKS,
        "chunks_verified": len(chunks),
        "rows_verified": total_rows,
        "controls": total_controls,
        "all_required_obs_fields_present": all_required,
        "checks": {
            "obs_artifacts_exist": True,
            "x_artifacts_exist": True,
            "var_artifacts_exist": True,
            "obs_to_x_links_by_key_ok": True,
            "x_to_var_links_by_key_ok": True,
            "x_payloads_exist": True,
            "x_n_observations_match_obs_rows": True,
            "var_rows_match_source_vars": True,
            "required_obs_fields_present": True,
        },
        "sampled_chunks": sampled,
        "chunks": chunks,
    }

    out = ROOT / "artifacts/schema_audit/prism_GSE247274_chunked_verification.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(verification, indent=2) + "\n")

    progress_path = ROOT / "artifacts/phase3_ingestion_progress.json"
    progress = json.loads(progress_path.read_text())
    entry = {
        "dataset": DATASET,
        "prefix": PREFIX,
        "path": None,
        "gcs_uri": GCS_URI,
        "n_obs": N_OBS,
        "n_vars": N_VARS,
        "chunk_size": CHUNK_SIZE,
        "status": "chunked_verified",
        "chunks_verified": N_CHUNKS,
        "chunks_ok": N_CHUNKS,
        "controls": total_controls,
        "verification_json": "artifacts/schema_audit/prism_GSE247274_chunked_verification.json",
        "note": "P5F Google Drive recovery; duplicate-resolved canonical object ingested as backed h5ad chunks after smoke verification; each chunk has same-prefix obs/X/var and verified payloads/links/canonical obs. canonical and (1) staged objects are byte-identical by streaming SHA-256; ingested canonical object only.",
    }
    upsert(progress.setdefault("ingested", []), "dataset", entry)
    progress["downloaded_not_ingested"] = [
        item for item in progress.get("downloaded_not_ingested", []) if item.get("dataset") != DATASET
    ]
    progress.setdefault("per_dataset_status", {})[DATASET] = {
        "status": "ingested_verified",
        "updated_at_utc": now,
        "prefix": PREFIX,
        "chunks_ok": N_CHUNKS,
        "chunks_verified": N_CHUNKS,
        "verification_json": "artifacts/schema_audit/prism_GSE247274_chunked_verification.json",
    }
    progress["last_updated"] = now
    progress_path.write_text(json.dumps(progress, indent=2) + "\n")

    print(json.dumps({
        "dataset": DATASET,
        "chunks_verified": len(chunks),
        "rows_verified": total_rows,
        "n_vars": N_VARS,
        "controls": total_controls,
        "verification_json": str(out.relative_to(ROOT)),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
