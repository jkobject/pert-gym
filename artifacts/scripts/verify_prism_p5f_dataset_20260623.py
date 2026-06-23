#!/usr/bin/env python3
"""Verify one PRISM P5F chunked dataset without loading full X matrices."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import anndata as ad

from tools.lamin_context import connect_pertdata

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--source", required=True, help="Local source h5ad path for expected shape only")
    parser.add_argument("--chunk-size", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    source = ad.read_h5ad(args.source, backed="r")
    try:
        expected_rows = int(source.n_obs)
        expected_vars = int(source.n_vars)
    finally:
        source.file.close()

    expected_chunks = math.ceil(expected_rows / args.chunk_size)
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    chunk_results: list[dict[str, Any]] = []
    total_rows = 0
    total_controls = 0
    var_counts: set[int] = set()
    bad: list[dict[str, Any]] = []

    for chunk_i in range(expected_chunks):
        start = chunk_i * args.chunk_size
        end = min(expected_rows, start + args.chunk_size)
        prefix = f"prism_collection/{args.dataset}/chunk_{chunk_i:04d}"
        row: dict[str, Any] = {"chunk": chunk_i, "prefix": prefix, "expected_rows": end - start}
        try:
            obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
            x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
            var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
            obs_df = obs_art.load()
            var_df = var_art.load()
            fields = {field: field in obs_df.columns for field in REQUIRED_OBS_FIELDS}
            controls = int(obs_df["is_control"].sum()) if "is_control" in obs_df.columns else None
            row.update(
                {
                    "obs_key": obs_art.key,
                    "x_key": x_art.key,
                    "var_key": var_art.key,
                    "obs_artifact_exists": bool(obs_art.uid),
                    "x_artifact_exists": bool(x_art.uid),
                    "var_artifact_exists": bool(var_art.uid),
                    "obs_to_x_ok": x_art.key == f"{prefix}/X.h5ad",
                    "x_to_var_ok": var_art.key == f"{prefix}/var.parquet",
                    "x_payload_exists": bool(x_art.path.exists()),
                    "obs_rows": int(len(obs_df)),
                    "x_n_observations": int(x_art.n_observations or -1),
                    "var_rows": int(len(var_df)),
                    "required_obs_fields_present": fields,
                    "all_required_obs_fields_present": all(fields.values()),
                    "controls": controls,
                }
            )
            total_rows += int(len(obs_df))
            total_controls += int(controls or 0)
            var_counts.add(int(len(var_df)))
            checks = [
                row["obs_to_x_ok"],
                row["x_to_var_ok"],
                row["x_payload_exists"],
                row["obs_rows"] == row["expected_rows"],
                row["x_n_observations"] == row["expected_rows"],
                row["var_rows"] == expected_vars,
                row["all_required_obs_fields_present"],
            ]
            row["ok"] = all(checks)
        except Exception as exc:  # noqa: BLE001 - verification artifact should capture failure shape
            row.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        if not row.get("ok"):
            bad.append(row)
        chunk_results.append(row)

    checks = {
        "all_chunks_ok": not bad,
        "chunks_verified": len(chunk_results) == expected_chunks,
        "rows_verified": total_rows == expected_rows,
        "var_counts_match": var_counts == {expected_vars},
        "no_bad_chunks": not bad,
    }
    result = {
        "dataset": args.dataset,
        "source": args.source,
        "chunk_size": args.chunk_size,
        "expected_rows": expected_rows,
        "expected_vars": expected_vars,
        "expected_chunks": expected_chunks,
        "chunks_verified": len(chunk_results),
        "rows_verified": total_rows,
        "var_counts": sorted(var_counts),
        "controls": total_controls,
        "checks": checks,
        "ok": all(checks.values()),
        "bad_chunks": bad,
        "chunks": chunk_results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    print(json.dumps({k: result[k] for k in ["dataset", "ok", "chunks_verified", "expected_chunks", "rows_verified", "expected_rows", "expected_vars", "controls"]}, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
