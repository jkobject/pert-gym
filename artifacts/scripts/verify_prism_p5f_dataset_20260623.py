#!/usr/bin/env python3
"""Verify a P5F PRISM chunked Lamin triplet dataset and update local progress."""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata  # noqa: E402

REQUIRED_OBS = [
    "perturbation",
    "perturbation_type",
    "organism",
    "cell_line",
    "modality",
    "assay",
    "is_control",
]


def resolve_artifact(ln, value):
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def upsert(items, key, entry):
    items[:] = [item for item in items if item.get(key) != entry.get(key)]
    items.append(entry)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gcs-uri", required=True)
    parser.add_argument("--n-obs", type=int, required=True)
    parser.add_argument("--n-vars", type=int, required=True)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--note", default="P5F Google Drive recovery; backed h5ad chunked ingestion after smoke verification; each chunk has same-prefix obs/X/var and verified payloads/links/canonical obs.")
    args = parser.parse_args()

    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    dataset = args.dataset
    prefix = f"prism_collection/{dataset}"
    n_chunks = math.ceil(args.n_obs / args.chunk_size)
    chunks = []
    total_rows = 0
    total_controls = 0
    sampled = []

    for i in range(n_chunks):
        start = i * args.chunk_size
        end = min(args.n_obs, start + args.chunk_size)
        chunk_prefix = f"{prefix}/chunk_{i:04d}"
        obs = ln.Artifact.get(key=f"{chunk_prefix}/obs.parquet")
        x = resolve_artifact(ln, obs.features.get_values().get("X"))
        var = resolve_artifact(ln, x.features.get_values().get("var"))
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
            and vars_ == args.n_vars
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
        if i in {0, 1, n_chunks - 1}:
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

    if total_rows != args.n_obs:
        raise RuntimeError(f"row total mismatch: {total_rows} != {args.n_obs}")

    now = datetime.now(timezone.utc).isoformat()
    rel_json = f"artifacts/schema_audit/prism_{dataset}_chunked_verification.json"
    rel_md = f"artifacts/schema_audit/prism_{dataset}_chunked_verification.md"
    verification = {
        "dataset": dataset,
        "prefix": prefix,
        "gcs_uri": args.gcs_uri,
        "verified_at_utc": now,
        "lamin_instance": ln.setup.settings.instance.slug,
        "lamin_branch": ln.setup.settings.branch.name,
        "n_obs": args.n_obs,
        "n_vars": args.n_vars,
        "chunk_size": args.chunk_size,
        "chunks_expected": n_chunks,
        "chunks_verified": len(chunks),
        "rows_verified": total_rows,
        "controls": total_controls,
        "all_required_obs_fields_present": True,
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

    out = ROOT / rel_json
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(verification, indent=2) + "\n")

    md = ROOT / rel_md
    md.write_text(
        f"# PRISM {dataset} chunked verification\n\n"
        f"- verified_at_utc: `{now}`\n"
        f"- Lamin: `{ln.setup.settings.instance.slug}` branch `{ln.setup.settings.branch.name}`\n"
        f"- prefix: `{prefix}`\n"
        f"- source: `{args.gcs_uri}`\n"
        f"- shape: `{args.n_obs} × {args.n_vars}`\n"
        f"- chunks verified: `{len(chunks)}/{n_chunks}` at chunk size `{args.chunk_size}`\n"
        f"- controls: `{total_controls}`\n"
        f"- checks: obs artifacts, X artifacts/payloads, var artifacts, obs→X links, X→var links, X n_observations, var row counts, and required obs fields all passed.\n"
    )

    progress_path = ROOT / "artifacts/phase3_ingestion_progress.json"
    progress = json.loads(progress_path.read_text()) if progress_path.exists() else {}
    entry = {
        "dataset": dataset,
        "prefix": prefix,
        "path": None,
        "gcs_uri": args.gcs_uri,
        "n_obs": args.n_obs,
        "n_vars": args.n_vars,
        "chunk_size": args.chunk_size,
        "status": "chunked_verified",
        "chunks_verified": n_chunks,
        "chunks_ok": n_chunks,
        "controls": total_controls,
        "verification_json": rel_json,
        "note": args.note,
    }
    upsert(progress.setdefault("ingested", []), "dataset", entry)
    progress["downloaded_not_ingested"] = [
        item for item in progress.get("downloaded_not_ingested", []) if item.get("dataset") != dataset
    ]
    progress.setdefault("per_dataset_status", {})[dataset] = {
        "status": "ingested_verified",
        "updated_at_utc": now,
        "prefix": prefix,
        "chunks_ok": n_chunks,
        "chunks_verified": n_chunks,
        "verification_json": rel_json,
    }
    progress["last_updated"] = now
    progress_path.write_text(json.dumps(progress, indent=2) + "\n")

    print(json.dumps({
        "dataset": dataset,
        "chunks_verified": len(chunks),
        "rows_verified": total_rows,
        "n_vars": args.n_vars,
        "controls": total_controls,
        "verification_json": rel_json,
        "verification_md": rel_md,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
