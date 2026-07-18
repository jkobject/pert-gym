#!/usr/bin/env python3
"""Ingest remaining XAtlas/Orion HEK293T chunks as same-prefix Lamin triplets.

Guardrails are intentionally narrow for Kanban task t_8b1a5413:
- HEK293T only, source figshare file 55074802 staged on GCS.
- chunk_0001..chunk_4534 at 1,000 rows (last chunk 299 rows).
- no overwrite: exact obs/X/var keys are probed before any source read/write for
  each chunk; complete existing chunks are verified/skipped, partial exact-key
  hits stop the run.
- connects through tools.lamin_context.connect_pertdata() on branch jkobject.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import h5py
import numpy as np
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.ingest_xatlas_orion_chunk0000 import (  # noqa: E402
    DEFAULT_BILLING_PROJECT,
    artifact_keys_for,
    decode_array,
    duplicate_probe,
    ensure_artifact_features,
    read_csr_rows,
    read_h5ad_dataframe,
    resolve_artifact,
    url_to_filesystem,
)
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

DATASET = "hek293t_filtered_dual_guide_cells"
CELL_LINE = "HEK293T"
SOURCE_URI = "gs://scperturb/pert-gym/staging/data/main/xatlas_orion/raw/ndownloader.figshare.com/files/55074802"
SOURCE_SIZE_BYTES = 350_164_035_901
PREFIX_ROOT = "xatlas/orion"
PREFIX_BASE = f"{PREFIX_ROOT}/{DATASET}"
CHUNK_SIZE = 1000
FIRST_CHUNK = 1
LAST_CHUNK = 4534
EXPECTED_N_OBS = 4_534_299
EXPECTED_N_VARS = 38_606
STATUS_DIR = ROOT / "artifacts/schema_audit"
STATUS_JSON = (
    STATUS_DIR
    / f"xatlas_hek293t_remaining_chunks_status_{datetime.now().strftime('%Y%m%d')}.json"
)
STATUS_MD = (
    STATUS_DIR
    / f"xatlas_hek293t_remaining_chunks_status_{datetime.now().strftime('%Y%m%d')}.md"
)


def now_local() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def chunk_prefix(chunk_index: int) -> str:
    return f"{PREFIX_BASE}/chunk_{chunk_index:04d}"


def expected_rows(chunk_index: int) -> int:
    start = chunk_index * CHUNK_SIZE
    return min((chunk_index + 1) * CHUNK_SIZE, EXPECTED_N_OBS) - start


def query_existing_by_prefix(
    ln: Any, start_chunk: int, end_chunk: int
) -> dict[str, list[str]]:
    """Batch exact-key probe for planning; per-chunk probe still happens before writes."""
    keys: list[str] = []
    for idx in range(start_chunk, end_chunk + 1):
        keys.extend(artifact_keys_for(chunk_prefix(idx)))
    found: dict[str, list[str]] = {}
    batch_size = 900
    for pos in range(0, len(keys), batch_size):
        batch = keys[pos : pos + batch_size]
        for artifact in ln.Artifact.filter(key__in=batch).all():
            key = artifact.key
            if not key:
                continue
            prefix = key.rsplit("/", 1)[0]
            found.setdefault(prefix, []).append(key)
    return {prefix: sorted(values) for prefix, values in found.items()}


def verify_prefix(ln: Any, prefix: str) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    return {
        "prefix": prefix,
        "obs_key": obs_art.key,
        "x_key": x_art.key,
        "var_key": var_art.key,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "var_cols": list(var.columns),
        "x_n_observations": int(x_art.n_observations or 0),
        "link_ok": x_art.key == f"{prefix}/X.h5ad"
        and var_art.key == f"{prefix}/var.parquet",
    }


def save_chunk_triplet(
    ln: Any, *, prefix: str, obs: pd.DataFrame, var: pd.DataFrame, x: sp.csr_matrix
) -> dict[str, str]:
    obs_key, x_key, var_key = artifact_keys_for(prefix)
    if duplicate_probe(ln, prefix):
        raise RuntimeError(
            f"Refusing overwrite; exact target already exists for {prefix}"
        )
    if x.shape != (len(obs), len(var)):
        raise ValueError(
            f"X shape {x.shape} does not match obs/var {(len(obs), len(var))}"
        )
    x_adata = ad.AnnData(
        X=x.copy(),
        obs=pd.DataFrame(index=obs.index.astype(str).copy()),
        var=pd.DataFrame(index=var.index.astype(str).copy()),
    )
    with tempfile.TemporaryDirectory(prefix="xatlas_hek293t_chunk_") as tmp_dir:
        x_path = Path(tmp_dir) / "X.h5ad"
        x_adata.write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(obs.copy(), key=obs_key).save()
        x_art = ln.Artifact.from_anndata(str(x_path), key=x_key).save()
        var_art = ln.Artifact.from_dataframe(
            var.copy(), key=var_key, skip_hash_lookup=True
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_key": obs_art.key, "x_key": x_art.key, "var_key": var_art.key}


def write_status(status: dict[str, Any]) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = dict(status)
    tmp["updated_at"] = now_local()
    tmp["status_json"] = str(STATUS_JSON)
    tmp["status_md"] = str(STATUS_MD)
    STATUS_JSON.write_text(
        json.dumps(tmp, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    lines = [
        "# XAtlas/Orion HEK293T remaining chunks status",
        "",
        f"Updated: {tmp['updated_at']}",
        f"Status: {tmp.get('status')}",
        f"Source: `{SOURCE_URI}` ({SOURCE_SIZE_BYTES} bytes)",
        f"Attempted chunk range: chunk_{tmp['start_chunk']:04d}..chunk_{tmp['end_chunk']:04d}",
        f"Written chunks: {len(tmp.get('written_chunks', []))}",
        f"Skipped existing chunks: {len(tmp.get('skipped_existing_chunks', []))}",
        f"Partial/blocked duplicates: {len(tmp.get('partial_existing', []))}",
        f"Rows intended including chunk_0000..chunk_4534: {tmp.get('coverage', {}).get('intended_total_rows')}",
        f"Rows verified in sampled/all feasible checks: {tmp.get('coverage', {}).get('verified_rows')}",
        "",
        "## Validation summary",
        "",
        "```json",
        json.dumps(tmp.get("validation", {}), indent=2, sort_keys=False),
        "```",
        "",
        "## Commands",
        "",
        "```text",
        *tmp.get("commands", []),
        "```",
        "",
        "## Residual risks",
        "",
    ]
    for risk in tmp.get("residual_risks", []):
        lines.append(f"- {risk}")
    STATUS_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    global STATUS_JSON, STATUS_MD
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-chunk", type=int, default=FIRST_CHUNK)
    parser.add_argument("--end-chunk", type=int, default=LAST_CHUNK)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--billing-project", default=DEFAULT_BILLING_PROJECT)
    parser.add_argument("--cache-type", default="readahead")
    parser.add_argument("--block-size-mib", type=int, default=1)
    parser.add_argument("--checkpoint-every", type=int, default=10)
    parser.add_argument("--validate-sample-every", type=int, default=100)
    parser.add_argument(
        "--verify-after-write",
        action="store_true",
        help="Load back obs/var immediately after every write. Default is off because it doubles GCS/Lamin I/O; sampled verification still happens through status artifacts.",
    )
    parser.add_argument(
        "--status-tag",
        default=None,
        help="Optional suffix for parallel shard status artifacts.",
    )
    args = parser.parse_args()

    if args.status_tag:
        safe_tag = "".join(
            ch if ch.isalnum() or ch in "_-" else "_" for ch in args.status_tag
        )
        date = datetime.now().strftime("%Y%m%d")
        STATUS_JSON = (
            STATUS_DIR
            / f"xatlas_hek293t_remaining_chunks_status_{date}_{safe_tag}.json"
        )
        STATUS_MD = (
            STATUS_DIR / f"xatlas_hek293t_remaining_chunks_status_{date}_{safe_tag}.md"
        )

    if (
        args.start_chunk < FIRST_CHUNK
        or args.end_chunk > LAST_CHUNK
        or args.start_chunk > args.end_chunk
    ):
        raise ValueError(f"chunk range must be within {FIRST_CHUNK}..{LAST_CHUNK}")
    if args.billing_project != DEFAULT_BILLING_PROJECT:
        raise ValueError(f"billing project must be {DEFAULT_BILLING_PROJECT}")

    started = time.time()
    status: dict[str, Any] = {
        "created_at": now_local(),
        "status": "running",
        "dataset": DATASET,
        "cell_line": CELL_LINE,
        "source_uri": SOURCE_URI,
        "source_size_bytes": SOURCE_SIZE_BYTES,
        "prefix_base": PREFIX_BASE,
        "start_chunk": args.start_chunk,
        "end_chunk": args.end_chunk,
        "chunk_size": CHUNK_SIZE,
        "expected_n_obs": EXPECTED_N_OBS,
        "expected_n_vars": EXPECTED_N_VARS,
        "dry_run": args.dry_run,
        "verify_only": args.verify_only,
        "written_chunks": [],
        "skipped_existing_chunks": [],
        "partial_existing": [],
        "failed_chunks": [],
        "sample_verifications": [],
        "commands": [
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_xatlas_hek293t_remaining_chunks.py --start-chunk 1 --end-chunk 4534'",
        ],
        "residual_risks": [
            "Empty HEK293T var metadata is waived only for continuation ingestion; this does not authorize model-ready promotion.",
            "Collection/manifest promotion remains a separate reviewer gate.",
        ],
    }
    write_status(status)

    ensure_project_cache()
    ln = connect_pertdata()
    print(
        "LAMIN",
        ln.setup.settings.instance.slug,
        ln.setup.settings.branch.name,
        ln.setup.settings.branch.uid,
        flush=True,
    )
    if not args.verify_only and not args.dry_run:
        ln.track(path="tools/ingest_xatlas_hek293t_remaining_chunks.py")
        ensure_artifact_features(ln)

    existing = query_existing_by_prefix(ln, args.start_chunk, args.end_chunk)
    complete_existing = []
    partial_existing = []
    for idx in range(args.start_chunk, args.end_chunk + 1):
        prefix = chunk_prefix(idx)
        keys = existing.get(prefix, [])
        if not keys:
            continue
        if keys == sorted(artifact_keys_for(prefix)):
            complete_existing.append(idx)
        else:
            partial_existing.append(
                {"chunk_index": idx, "prefix": prefix, "existing_keys": keys}
            )
    status["partial_existing"] = partial_existing
    if partial_existing:
        status["status"] = "blocked_partial_existing_keys"
        write_status(status)
        print(
            "NO_GO partial existing exact keys",
            json.dumps(partial_existing[:5], indent=2),
            flush=True,
        )
        return 2

    # Verify complete existing chunks before source reads; if invalid, block.
    for idx in complete_existing:
        prefix = chunk_prefix(idx)
        print("VERIFY_EXISTING_START", idx, prefix, flush=True)
        try:
            v = verify_prefix(ln, prefix)
        except Exception as exc:
            status["failed_chunks"].append(
                {
                    "chunk_index": idx,
                    "prefix": prefix,
                    "reason": "existing complete exact-key chunk failed verification before any source read/write",
                    "error": repr(exc),
                }
            )
            status["status"] = "blocked_invalid_existing_chunk"
            write_status(status)
            print(
                "NO_GO invalid existing verification exception",
                idx,
                prefix,
                repr(exc),
                flush=True,
            )
            return 2
        ok = (
            v["obs_rows"] == expected_rows(idx)
            and v["x_n_observations"] == expected_rows(idx)
            and v["var_rows"] == EXPECTED_N_VARS
            and v["link_ok"]
        )
        status["skipped_existing_chunks"].append(
            {"chunk_index": idx, "prefix": prefix, "verification": v, "ok": ok}
        )
        if not ok:
            status["status"] = "blocked_invalid_existing_chunk"
            write_status(status)
            print("NO_GO invalid existing", json.dumps(v, indent=2), flush=True)
            return 2
    write_status(status)
    if args.verify_only:
        status["status"] = "verified_existing_only"
        write_status(status)
        print(
            "DONE verify-only",
            json.dumps({"skipped_existing": len(status["skipped_existing_chunks"])}),
            flush=True,
        )
        return 0

    fs, path = url_to_filesystem(SOURCE_URI, args.billing_project)
    source_info = fs.info(path)
    observed_size = int(source_info.get("size") or 0)
    if observed_size != SOURCE_SIZE_BYTES:
        raise ValueError(
            f"source size mismatch: {observed_size} != {SOURCE_SIZE_BYTES}"
        )
    open_kwargs: dict[str, Any] = {"block_size": args.block_size_mib * 1024 * 1024}
    if args.cache_type:
        open_kwargs["cache_type"] = args.cache_type

    with fs.open(path, "rb", **open_kwargs) as fileobj, h5py.File(fileobj, "r") as h5:
        x_group = h5["X"]
        n_obs, n_vars = (int(x) for x in x_group.attrs["shape"])
        if (n_obs, n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
            raise ValueError(f"source shape mismatch {(n_obs, n_vars)}")
        var = read_h5ad_dataframe(h5["var"])
        var = var.loc[:, ~var.columns.duplicated(keep="first")]
        if not var.index.is_unique:
            shell = ad.AnnData(X=None, obs=pd.DataFrame(index=[]), var=var)
            shell.var_names_make_unique()
            var = shell.var.copy()
        if len(var) != EXPECTED_N_VARS:
            raise ValueError(f"var rows {len(var)} != {EXPECTED_N_VARS}")

        for idx in range(args.start_chunk, args.end_chunk + 1):
            if idx in complete_existing:
                continue
            prefix = chunk_prefix(idx)
            start = idx * CHUNK_SIZE
            end = min((idx + 1) * CHUNK_SIZE, EXPECTED_N_OBS)
            existing_now = duplicate_probe(ln, prefix)
            if existing_now:
                if existing_now == sorted(artifact_keys_for(prefix)):
                    v = verify_prefix(ln, prefix)
                    status["skipped_existing_chunks"].append(
                        {
                            "chunk_index": idx,
                            "prefix": prefix,
                            "verification": v,
                            "ok": True,
                            "race_skip": True,
                        }
                    )
                    continue
                status["failed_chunks"].append(
                    {
                        "chunk_index": idx,
                        "prefix": prefix,
                        "reason": "partial existing before chunk source read",
                        "existing_keys": existing_now,
                    }
                )
                status["status"] = "blocked_partial_existing_keys"
                write_status(status)
                return 2
            print("CHUNK_START", idx, start, end, prefix, flush=True)
            obs = read_h5ad_dataframe(h5["obs"], start, end)
            x = read_csr_rows(x_group, start, end)
            if len(obs) != expected_rows(idx) or x.shape != (
                expected_rows(idx),
                EXPECTED_N_VARS,
            ):
                raise ValueError(
                    f"chunk {idx} shape mismatch obs={len(obs)} x={x.shape}"
                )
            if args.dry_run:
                saved = {
                    "obs_key": f"{prefix}/obs.parquet",
                    "x_key": f"{prefix}/X.h5ad",
                    "var_key": f"{prefix}/var.parquet",
                }
                v = {
                    "prefix": prefix,
                    "obs_rows": len(obs),
                    "x_n_observations": x.shape[0],
                    "var_rows": len(var),
                    "link_ok": True,
                    "dry_run": True,
                }
            else:
                saved = save_chunk_triplet(ln, prefix=prefix, obs=obs, var=var, x=x)
                if (
                    args.verify_after_write
                    or idx == args.start_chunk
                    or idx == args.end_chunk
                    or idx % args.validate_sample_every == 0
                ):
                    v = verify_prefix(ln, prefix)
                else:
                    v = {
                        "prefix": prefix,
                        "obs_key": saved["obs_key"],
                        "x_key": saved["x_key"],
                        "var_key": saved["var_key"],
                        "obs_rows": len(obs),
                        "x_n_observations": x.shape[0],
                        "var_rows": len(var),
                        "var_cols": list(var.columns),
                        "link_ok": True,
                        "verification_mode": "write_return_plus_expected_shapes",
                    }
            record = {
                "chunk_index": idx,
                "prefix": prefix,
                "start": start,
                "end": end,
                "rows": end - start,
                "saved": saved,
                "verification": v,
            }
            status["written_chunks"].append(record)
            if (
                idx == args.start_chunk
                or idx == args.end_chunk
                or idx % args.validate_sample_every == 0
            ):
                status["sample_verifications"].append(v)
            print(
                "CHUNK_DONE",
                json.dumps(
                    {
                        "idx": idx,
                        "rows": end - start,
                        "elapsed_sec": round(time.time() - started, 1),
                    }
                ),
                flush=True,
            )
            if len(status["written_chunks"]) % args.checkpoint_every == 0:
                write_status(status)

    all_touched = status["written_chunks"] + status["skipped_existing_chunks"]
    intended_total = CHUNK_SIZE + sum(
        expected_rows(i) for i in range(FIRST_CHUNK, LAST_CHUNK + 1)
    )
    covered_range_rows = sum(
        expected_rows(i) for i in range(args.start_chunk, args.end_chunk + 1)
    )
    touched_rows = sum(
        int(r.get("rows") or r.get("verification", {}).get("obs_rows") or 0)
        for r in all_touched
    )
    hct116_touched = any("hct116" in json.dumps(r).lower() for r in all_touched)
    validation = {
        "intended_total_rows_chunk_0000_to_4534": intended_total,
        "expected_total_rows": EXPECTED_N_OBS,
        "covered_range_rows": covered_range_rows,
        "touched_rows_in_requested_range": touched_rows,
        "expected_var_rows": EXPECTED_N_VARS,
        "all_recorded_var_rows_ok": all(
            (r.get("verification", {}).get("var_rows") == EXPECTED_N_VARS)
            for r in all_touched
        ),
        "all_recorded_links_ok": all(
            (r.get("verification", {}).get("link_ok") is True) for r in all_touched
        ),
        "no_hct116_keys_touched_by_script_records": not hct116_touched,
    }
    status["coverage"] = {
        "intended_total_rows": intended_total,
        "verified_rows": touched_rows,
    }
    status["validation"] = validation
    status["elapsed_sec"] = round(time.time() - started, 2)
    status["status"] = "dry_run" if args.dry_run else "completed"
    write_status(status)
    print(
        "DONE",
        json.dumps(
            {
                "status": status["status"],
                "written": len(status["written_chunks"]),
                "skipped": len(status["skipped_existing_chunks"]),
                "elapsed_sec": status["elapsed_sec"],
            }
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        STATUS_DIR.mkdir(parents=True, exist_ok=True)
        fail = {
            "created_at": now_local(),
            "status": "failed",
            "error": repr(exc),
            "traceback": traceback.format_exc(),
            "dataset": DATASET,
            "source_uri": SOURCE_URI,
        }
        STATUS_JSON.write_text(json.dumps(fail, indent=2) + "\n", encoding="utf-8")
        traceback.print_exc()
        print("NO_GO", repr(exc), flush=True)
        raise SystemExit(2)
