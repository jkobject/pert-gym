#!/usr/bin/env python3
"""Ingest a bounded XAtlas/Orion HCT116 chunk tranche as same-prefix Lamin triplets.

Guarded for Kanban task t_9b114e63:
- HCT116 only, chunks 0257..0270 inclusive by default.
- exact-key duplicate probes before every chunk read/write;
- complete existing chunks are verified and skipped, partial/conflicting exact keys stop;
- writes through tools.lamin_context.connect_pertdata() on laminlabs/pertdata branch jkobject;
- intended to run on pert-gym-worker-eu near gs://scperturb.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import h5py

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.ingest_xatlas_orion_chunk0000 import (  # noqa: E402
    DEFAULT_BILLING_PROJECT,
    artifact_keys_for,
    duplicate_probe,
    ensure_artifact_features,
    read_csr_rows,
    read_h5ad_dataframe,
    resolve_artifact,
    save_chunk_triplet,
)
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_9b114e63"
DATASET = "hct116_filtered_dual_guide_cells"
CELL_LINE = "HCT116"
PREFIX_BASE = f"xatlas/orion/{DATASET}"
SOURCE_PATH = Path("/home/jkobject/scperturb_gcs/pert-gym/staging/xatlas_orion/raw/hct116_filtered_dual_guide_cells.h5ad")
EXPECTED_N_OBS = 3_409_169
EXPECTED_N_VARS = 38_606
CHUNK_SIZE = 1000
AUTHORIZED_START = 257
AUTHORIZED_END = 270
STATUS_DIR = ROOT / "artifacts/schema_audit"
DATE = datetime.now().strftime("%Y%m%d")
STATUS_JSON = STATUS_DIR / f"xatlas_hct116_bounded_tranche_0257_0270_{DATE}_{TASK_ID}.json"
STATUS_MD = STATUS_DIR / f"xatlas_hct116_bounded_tranche_0257_0270_{DATE}_{TASK_ID}.md"
LOCK_PATH = STATUS_DIR / f"xatlas_hct116_bounded_tranche_0257_0270_{TASK_ID}.lock.json"


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def chunk_prefix(idx: int) -> str:
    return f"{PREFIX_BASE}/chunk_{idx:04d}"


def expected_rows(idx: int) -> int:
    start = idx * CHUNK_SIZE
    return min((idx + 1) * CHUNK_SIZE, EXPECTED_N_OBS) - start


def query_existing(ln: Any, start_chunk: int, end_chunk: int) -> dict[str, list[str]]:
    keys: list[str] = []
    for idx in range(start_chunk, end_chunk + 1):
        keys.extend(artifact_keys_for(chunk_prefix(idx)))
    found: dict[str, list[str]] = {}
    for art in ln.Artifact.filter(key__in=keys).all():
        if art.key:
            found.setdefault(art.key.rsplit("/", 1)[0], []).append(art.key)
    return {prefix: sorted(vals) for prefix, vals in found.items()}


def verify_prefix(ln: Any, prefix: str) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    return {
        "prefix": prefix,
        "keys": {"obs": obs_art.key, "X": x_art.key, "var": var_art.key},
        "uids": {"obs": obs_art.uid, "X": x_art.uid, "var": var_art.uid},
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "link_ok": x_art.key == f"{prefix}/X.h5ad" and var_art.key == f"{prefix}/var.parquet",
    }


def write_status(status: dict[str, Any]) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    status = dict(status)
    status["updated_at"] = now()
    status["status_json"] = str(STATUS_JSON)
    status["status_md"] = str(STATUS_MD)
    STATUS_JSON.write_text(json.dumps(status, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    lines = [
        "# XAtlas/Orion HCT116 bounded tranche 0257-0270 status",
        "",
        f"Updated: {status['updated_at']}",
        f"Status: {status.get('status')}",
        f"Task: `{TASK_ID}`",
        f"Source path: `{SOURCE_PATH}`",
        f"Chunk range: chunk_{status['start_chunk']:04d}..chunk_{status['end_chunk']:04d}",
        f"Written chunks: {len(status.get('written_chunks', []))}",
        f"Skipped existing chunks: {len(status.get('skipped_existing_chunks', []))}",
        f"Failed chunks: {len(status.get('failed_chunks', []))}",
        "",
        "## Chunk results",
        "",
        "```json",
        json.dumps({
            "written_chunks": status.get("written_chunks", []),
            "skipped_existing_chunks": status.get("skipped_existing_chunks", []),
            "failed_chunks": status.get("failed_chunks", []),
            "partial_existing": status.get("partial_existing", []),
        }, indent=2, sort_keys=False),
        "```",
        "",
        "## Commands",
        "",
        "```text",
        *status.get("commands", []),
        "```",
        "",
        "## Residual risks",
        "",
    ]
    for risk in status.get("residual_risks", []):
        lines.append(f"- {risk}")
    STATUS_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def acquire_lock(status: dict[str, Any]) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        old = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
        if old.get("task_id") != TASK_ID or old.get("status") == "running":
            raise RuntimeError(f"single-writer lock already exists: {old}")
    LOCK_PATH.write_text(json.dumps({
        "task_id": TASK_ID,
        "status": "running",
        "created_at": now(),
        "chunk_range": [AUTHORIZED_START, AUTHORIZED_END],
        "status_json": str(STATUS_JSON),
    }, indent=2) + "\n", encoding="utf-8")
    status["single_writer_lock"] = str(LOCK_PATH)


def finish_lock(final_status: str) -> None:
    if LOCK_PATH.exists():
        old = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    else:
        old = {}
    old.update({"status": final_status, "updated_at": now(), "status_json": str(STATUS_JSON)})
    LOCK_PATH.write_text(json.dumps(old, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-chunk", type=int, default=AUTHORIZED_START)
    parser.add_argument("--end-chunk", type=int, default=AUTHORIZED_END)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--checkpoint-every", type=int, default=10)
    args = parser.parse_args()

    if args.start_chunk < AUTHORIZED_START or args.end_chunk > AUTHORIZED_END or args.start_chunk > args.end_chunk:
        raise ValueError(f"chunk range must be within {AUTHORIZED_START}..{AUTHORIZED_END}")
    status: dict[str, Any] = {
        "created_at": now(),
        "status": "running",
        "task_id": TASK_ID,
        "dataset": DATASET,
        "cell_line": CELL_LINE,
        "prefix_base": PREFIX_BASE,
        "source_path": str(SOURCE_PATH),
        "expected_n_obs": EXPECTED_N_OBS,
        "expected_n_vars": EXPECTED_N_VARS,
        "chunk_size": CHUNK_SIZE,
        "start_chunk": args.start_chunk,
        "end_chunk": args.end_chunk,
        "authorized_range": [AUTHORIZED_START, AUTHORIZED_END],
        "dry_run": args.dry_run,
        "verify_only": args.verify_only,
        "written_chunks": [],
        "skipped_existing_chunks": [],
        "partial_existing": [],
        "failed_chunks": [],
        "commands": [
            "gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command 'cd ~/work/pert-gym && uv run python tools/ingest_xatlas_hct116_bounded_tranche.py --start-chunk 257 --end-chunk 270 --checkpoint-every 10'",
        ],
        "residual_risks": [
            "Collection/model-ready promotion remains separate; this script only writes/validates same-prefix triplets.",
            "Continuation starts after accepted bounded tranche t_b186bd87/reviewer t_cfe68673; chunk_0257 is the first chunk in this bounded task.",
            "HCT116 denominator caveat: full target is 3,409,169 obs = 3,410 chunks; after this tranche, accepted coverage is only 271/3410 = 7.95%.",
        ],
    }
    write_status(status)
    acquire_lock(status)
    write_status(status)
    final_status = "failed"
    try:
        ensure_project_cache()
        ln = connect_pertdata()
        print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)
        if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
            raise RuntimeError("wrong Lamin target")
        if not args.dry_run and not args.verify_only:
            ln.track(path="tools/ingest_xatlas_hct116_bounded_tranche.py")
            ensure_artifact_features(ln)

        existing = query_existing(ln, args.start_chunk, args.end_chunk)
        partial_existing = []
        complete_existing = []
        for idx in range(args.start_chunk, args.end_chunk + 1):
            prefix = chunk_prefix(idx)
            keys = existing.get(prefix, [])
            if not keys:
                continue
            if keys == sorted(artifact_keys_for(prefix)):
                complete_existing.append(idx)
            else:
                partial_existing.append({"chunk_index": idx, "prefix": prefix, "existing_keys": keys})
        status["partial_existing"] = partial_existing
        if partial_existing:
            status["status"] = "blocked_partial_existing_keys"
            write_status(status)
            final_status = status["status"]
            print("NO_GO partial existing", json.dumps(partial_existing, indent=2), flush=True)
            return 2

        for idx in complete_existing:
            prefix = chunk_prefix(idx)
            v = verify_prefix(ln, prefix)
            ok = v["obs_rows"] == expected_rows(idx) and v["x_n_observations"] == expected_rows(idx) and v["var_rows"] == EXPECTED_N_VARS and v["link_ok"]
            status["skipped_existing_chunks"].append({"chunk_index": idx, "prefix": prefix, "verification": v, "ok": ok})
            if not ok:
                status["status"] = "blocked_invalid_existing_chunk"
                write_status(status)
                final_status = status["status"]
                print("NO_GO invalid existing", json.dumps(v, indent=2), flush=True)
                return 2
        write_status(status)
        if args.verify_only:
            status["status"] = "verified_existing_only"
            write_status(status)
            final_status = status["status"]
            return 0
        if args.dry_run:
            status["status"] = "dry_run_ok"
            write_status(status)
            final_status = status["status"]
            return 0
        if not SOURCE_PATH.exists():
            raise FileNotFoundError(SOURCE_PATH)

        with h5py.File(SOURCE_PATH, "r") as h5:
            x_group = h5["X"]
            n_obs, n_vars = (int(x) for x in x_group.attrs["shape"])
            if (n_obs, n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
                raise ValueError(f"source shape mismatch {(n_obs, n_vars)}")
            var = read_h5ad_dataframe(h5["var"])
            var = var.loc[:, ~var.columns.duplicated(keep="first")]
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
                        status["skipped_existing_chunks"].append({"chunk_index": idx, "prefix": prefix, "verification": v, "ok": True, "race_skip": True})
                        write_status(status)
                        continue
                    status["failed_chunks"].append({"chunk_index": idx, "prefix": prefix, "reason": "partial existing before chunk source read", "existing_keys": existing_now})
                    status["status"] = "blocked_partial_existing_keys"
                    write_status(status)
                    final_status = status["status"]
                    return 2
                print("CHUNK_START", idx, start, end, prefix, flush=True)
                obs = read_h5ad_dataframe(h5["obs"], start, end)
                x = read_csr_rows(x_group, start, end)
                if len(obs) != expected_rows(idx) or x.shape != (expected_rows(idx), EXPECTED_N_VARS):
                    raise ValueError({"chunk_index": idx, "obs_rows": len(obs), "x_shape": x.shape})
                saved = save_chunk_triplet(ln, prefix=prefix, obs=obs, var=var, x=x)
                v = verify_prefix(ln, prefix)
                ok = v["obs_rows"] == expected_rows(idx) and v["x_n_observations"] == expected_rows(idx) and v["var_rows"] == EXPECTED_N_VARS and v["link_ok"]
                status["written_chunks"].append({"chunk_index": idx, "prefix": prefix, "bounds": [start, end], "saved": saved, "verification": v, "ok": ok})
                if not ok:
                    status["status"] = "blocked_postwrite_validation_failed"
                    write_status(status)
                    final_status = status["status"]
                    return 2
                if len(status["written_chunks"]) % args.checkpoint_every == 0 or idx == args.end_chunk:
                    status["status"] = "running_checkpoint"
                    write_status(status)
                    print("CHECKPOINT", json.dumps({"written": len(status["written_chunks"]), "skipped": len(status["skipped_existing_chunks"])}), flush=True)
        status["status"] = "ok"
        status["duration_seconds"] = round(time.time() - time.mktime(datetime.fromisoformat(status["created_at"]).timetuple()), 1) if False else None
        write_status(status)
        final_status = "ok"
        print(json.dumps(status, indent=2, sort_keys=False), flush=True)
        return 0
    except Exception as exc:
        status["status"] = "failed_exception"
        status["exception"] = repr(exc)
        status["traceback"] = traceback.format_exc()
        write_status(status)
        print(status["traceback"], flush=True)
        final_status = status["status"]
        return 1
    finally:
        finish_lock(final_status)


if __name__ == "__main__":
    raise SystemExit(main())
