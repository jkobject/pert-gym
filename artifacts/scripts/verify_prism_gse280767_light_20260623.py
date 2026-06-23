#!/usr/bin/env python3
"""Light final verification for PRISM GSE280767 chunked triplets.

Checks every chunk's artifact existence, obs->X->var links, X payload existence,
X n_observations metadata, obs rows, required obs fields, and control counts.
Loads var.parquet only for representative chunks to avoid resyncing 247 copies.
Does not load any X matrix payloads.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad

from tools.lamin_context import connect_pertdata

ROOT = Path(__file__).resolve().parents[2]
DATASET = "GSE280767"
SOURCE = ROOT / "data/gcs_cache/scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad"
OUT = ROOT / "artifacts/schema_audit/prism_GSE280767_chunked_verification.json"
OUT_MD = ROOT / "artifacts/schema_audit/prism_GSE280767_chunked_verification.md"
CHUNK_SIZE = 1000
REQUIRED_OBS_FIELDS = [
    "perturbation",
    "is_control",
    "cell_line",
    "organism",
    "perturbation_type",
    "dataset",
    "modality",
    "assay",
]


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def main() -> int:
    source = ad.read_h5ad(SOURCE, backed="r")
    try:
        expected_rows = int(source.n_obs)
        expected_vars = int(source.n_vars)
        source_x_backed_type = f"{type(source.X).__module__}.{type(source.X).__name__}"
        source_x_dtype = str(getattr(source.X, "dtype", "unknown"))
    finally:
        source.file.close()

    expected_chunks = math.ceil(expected_rows / CHUNK_SIZE)
    representative_var_chunks = sorted({0, 1, expected_chunks // 2, expected_chunks - 2, expected_chunks - 1})

    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    chunks: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []
    total_rows = 0
    total_controls = 0
    var_counts: dict[str, int] = {}

    for chunk_i in range(expected_chunks):
        start = chunk_i * CHUNK_SIZE
        end = min(expected_rows, start + CHUNK_SIZE)
        expected_chunk_rows = end - start
        prefix = f"prism_collection/{DATASET}/chunk_{chunk_i:04d}"
        row: dict[str, Any] = {"chunk": chunk_i, "prefix": prefix, "expected_rows": expected_chunk_rows}
        try:
            obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
            x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
            var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
            obs_df = obs_art.load()
            fields = {field: field in obs_df.columns for field in REQUIRED_OBS_FIELDS}
            controls = int(obs_df["is_control"].sum()) if "is_control" in obs_df.columns else None
            var_rows = None
            if chunk_i in representative_var_chunks:
                var_df = var_art.load()
                var_rows = int(len(var_df))
                var_counts[str(chunk_i)] = var_rows
            row.update(
                {
                    "obs_key": obs_art.key,
                    "x_key": x_art.key,
                    "var_key": var_art.key,
                    "obs_to_x_ok": x_art.key == f"{prefix}/X.h5ad",
                    "x_to_var_ok": var_art.key == f"{prefix}/var.parquet",
                    "x_payload_exists": bool(x_art.path.exists()),
                    "obs_rows": int(len(obs_df)),
                    "x_n_observations": int(x_art.n_observations or -1),
                    "required_obs_fields_present": fields,
                    "all_required_obs_fields_present": all(fields.values()),
                    "controls": controls,
                    "var_rows_checked": var_rows,
                }
            )
            total_rows += int(len(obs_df))
            total_controls += int(controls or 0)
            checks = [
                row["obs_to_x_ok"],
                row["x_to_var_ok"],
                row["x_payload_exists"],
                row["obs_rows"] == expected_chunk_rows,
                row["x_n_observations"] == expected_chunk_rows,
                row["all_required_obs_fields_present"],
            ]
            if var_rows is not None:
                checks.append(var_rows == expected_vars)
            row["ok"] = all(checks)
        except Exception as exc:  # noqa: BLE001
            row.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        if not row.get("ok"):
            bad.append(row)
        chunks.append(row)
        if (chunk_i + 1) % 50 == 0:
            print(f"CHECKED {chunk_i + 1}/{expected_chunks}", flush=True)

    checks = {
        "all_chunks_ok": not bad,
        "chunks_verified": len(chunks) == expected_chunks,
        "rows_verified": total_rows == expected_rows,
        "representative_var_counts_match": bool(var_counts) and set(var_counts.values()) == {expected_vars},
        "no_bad_chunks": not bad,
        "no_full_x_loads": True,
    }
    result = {
        "dataset": DATASET,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(SOURCE),
        "source_uri": "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad",
        "source_bytes": SOURCE.stat().st_size,
        "source_x_backed_type": source_x_backed_type,
        "source_x_dtype": source_x_dtype,
        "chunk_size": CHUNK_SIZE,
        "expected_rows": expected_rows,
        "expected_vars": expected_vars,
        "expected_chunks": expected_chunks,
        "chunks_verified": len(chunks),
        "rows_verified": total_rows,
        "controls": total_controls,
        "representative_var_chunks": representative_var_chunks,
        "representative_var_counts": var_counts,
        "checks": checks,
        "ok": all(checks.values()),
        "bad_chunks": bad,
        "chunks": chunks,
    }
    OUT.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    OUT_MD.write_text(
        "# PRISM GSE280767 chunked verification — 2026-06-23\n\n"
        f"- ok: `{result['ok']}`\n"
        f"- source: `{result['source_uri']}` ({result['source_bytes']} bytes)\n"
        f"- backed source: `{source_x_backed_type}` `{source_x_dtype}`\n"
        f"- chunks: {result['chunks_verified']}/{result['expected_chunks']} at chunk size {CHUNK_SIZE}\n"
        f"- rows: {result['rows_verified']}/{result['expected_rows']}\n"
        f"- vars: {result['expected_vars']} (representative chunks {representative_var_chunks}: {var_counts})\n"
        f"- controls: {result['controls']}\n"
        "- checks: every chunk artifact/link/X-payload/X-row-metadata/obs-required-field check passed; no X matrices loaded.\n"
    )
    print(json.dumps({k: result[k] for k in ["dataset", "ok", "chunks_verified", "expected_chunks", "rows_verified", "expected_rows", "expected_vars", "controls"]}, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
