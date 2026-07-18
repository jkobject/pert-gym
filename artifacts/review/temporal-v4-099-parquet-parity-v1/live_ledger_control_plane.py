#!/usr/bin/env python3
"""Loopback-only, read-only accepted-components control-plane helper."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

BOARD = "pert-gym"
METRIC = "accepted_components"
PROTOCOL = "pert-gym-accepted-components-loopback/v1"
DATABASE = Path("/Users/jkobject/.hermes/kanban/boards/pert-gym/kanban.db")
REQUEST_PATH = "/v1/accepted-components"
READY_PATH = "/ready"
TOKEN_ENV = "PERT_GYM_LEDGER_BEARER_TOKEN"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_NONCE = re.compile(r"^[0-9a-f]{64}$")
_IMMUTABLE_MANIFEST = re.compile(
    r"^gs://scperturb/pert-gym/staging/.+/revisions/[^/]+/manifest\.json$"
)
_DELTA_KEYS = {
    "before",
    "after",
    "denominator",
    "unit",
    "mismatch",
    "live_readback",
}


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise RuntimeError(f"accepted-components {label} is not an integer")
    return value


def _manifest_identity(metadata: dict[str, Any], live_readback: str) -> dict[str, str]:
    manifest = metadata.get("manifest")
    if not isinstance(manifest, dict):
        raise RuntimeError("latest accepted-components owner manifest identity is absent")
    if set(manifest) < {"uri", "generation", "sha256"}:
        raise RuntimeError("latest accepted-components owner manifest identity is incomplete")
    uri = manifest["uri"]
    generation = manifest["generation"]
    sha256 = manifest["sha256"]
    if (
        not isinstance(uri, str)
        or uri != live_readback
        or _IMMUTABLE_MANIFEST.fullmatch(uri) is None
    ):
        raise RuntimeError("latest accepted-components immutable manifest URI is incoherent")
    if not isinstance(generation, str) or not generation.isdigit():
        raise RuntimeError("latest accepted-components manifest generation is malformed")
    if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
        raise RuntimeError("latest accepted-components manifest SHA-256 is malformed")
    return {"uri": uri, "generation": generation, "sha256": sha256}


def _validated_delta(
    value: object,
    *,
    task_id: object,
    run_id: object,
    ended_at: object,
) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != _DELTA_KEYS:
        raise RuntimeError("accepted-components ledger record is malformed")
    delta = cast(dict[str, object], value)
    before = _integer(delta["before"], "before")
    after = _integer(delta["after"], "after")
    denominator = _integer(delta["denominator"], "denominator")
    run = _integer(run_id, "run identity")
    ended = _integer(ended_at, "completion timestamp")
    if (
        not isinstance(task_id, str)
        or not task_id.startswith("t_")
        or denominator != 153
        or not 0 <= before <= denominator
        or not 0 <= after <= denominator
        or (not (before == after == 0) and after != before + 1)
        or delta["unit"] != "components"
        or delta["mismatch"] != 0
        or not isinstance(delta["live_readback"], str)
        or not delta["live_readback"].strip()
        or run <= 0
        or ended <= 0
    ):
        raise RuntimeError("accepted-components ledger record is incoherent")
    return {
        "task_id": task_id,
        "run_id": run,
        "ended_at": ended,
        "before": before,
        "after": after,
    }


def _read_completed_metadata() -> list[tuple[int, str, int, dict[str, Any]]]:
    sidecars = [Path(f"{DATABASE}-wal"), Path(f"{DATABASE}-shm")]
    if any(path.exists() for path in sidecars):
        raise RuntimeError("authoritative board has an active SQLite sidecar")
    before = DATABASE.stat()
    uri = f"file:{quote(str(DATABASE), safe='/')}?mode=ro&immutable=1"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA query_only").fetchone() != (1,):
            raise RuntimeError("SQLite query-only enforcement failed")
        rows = connection.execute(
            """
            SELECT r.id, r.task_id, r.ended_at, r.metadata
            FROM task_runs AS r
            JOIN tasks AS t ON t.id = r.task_id
            WHERE t.status = 'done'
              AND r.status = 'done'
              AND r.outcome = 'completed'
              AND r.metadata IS NOT NULL
            ORDER BY r.ended_at, r.id
            """
        ).fetchall()
    except (OSError, sqlite3.Error) as error:
        raise RuntimeError(
            f"authoritative accepted-components board unavailable: {type(error).__name__}"
        ) from error
    finally:
        if connection is not None:
            connection.close()
    after = DATABASE.stat()
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or any(path.exists() for path in sidecars)
    ):
        raise RuntimeError("authoritative board changed during query")

    result: list[tuple[int, str, int, dict[str, Any]]] = []
    for run_id, task_id, ended_at, raw_metadata in rows:
        try:
            metadata = json.loads(raw_metadata)
            delta = metadata["product_delta"]["metrics"][METRIC]
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        if not isinstance(metadata, dict):
            raise RuntimeError("accepted-components owner metadata is malformed")
        result.append((run_id, task_id, ended_at, metadata | {"_delta": delta}))
    return result


def _validate_administrative_credit_replay(
    metadata: dict[str, Any],
    *,
    owner: dict[str, object],
    owner_metadata: dict[str, Any],
) -> None:
    gates = metadata.get("independent_gates")
    if not isinstance(gates, dict):
        raise RuntimeError("accepted-components administrative replay binding is malformed")
    owner_task_id = owner["task_id"]
    if gates.get("administrative_credit_task") != owner_task_id:
        raise RuntimeError(
            "accepted-components administrative replay is not bound to the latest owner"
        )
    delta = metadata["_delta"]
    owner_delta = owner_metadata["_delta"]
    if any(delta[key] != owner_delta[key] for key in _DELTA_KEYS):
        raise RuntimeError("accepted-components administrative replay delta conflicts")
    manifest = _manifest_identity(metadata, delta["live_readback"])
    owner_manifest = _manifest_identity(
        owner_metadata, owner_delta["live_readback"]
    )
    if manifest != owner_manifest:
        raise RuntimeError("accepted-components administrative replay manifest conflicts")


def build_response(nonce: str, *, issued_at: float | None = None) -> dict[str, object]:
    """Return only the validated metric chain and latest immutable owner identity."""
    if _NONCE.fullmatch(nonce) is None:
        raise RuntimeError("accepted-components request nonce is malformed")
    rows = _read_completed_metadata()
    chain: list[dict[str, object]] = []
    metadata_by_run: dict[int, dict[str, Any]] = {}
    seen_task_ids: set[str] = set()
    seen_run_ids: set[int] = set()
    last_ended_at: int | None = None
    for run_id, task_id, ended_at, metadata in rows:
        record = _validated_delta(
            metadata["_delta"],
            task_id=task_id,
            run_id=run_id,
            ended_at=ended_at,
        )
        record_task_id = record["task_id"]
        if not isinstance(record_task_id, str):
            raise RuntimeError("accepted-components ledger task identity is malformed")
        record_run_id = _integer(record["run_id"], "run identity")
        if record_task_id in seen_task_ids or record_run_id in seen_run_ids:
            raise RuntimeError("accepted-components ledger owner identity is duplicated")
        seen_task_ids.add(record_task_id)
        seen_run_ids.add(record_run_id)
        current_ended = _integer(record["ended_at"], "completion timestamp")
        if last_ended_at is not None and current_ended <= last_ended_at:
            raise RuntimeError("accepted-components ledger completion order is regressive")
        last_ended_at = current_ended
        if chain:
            latest = chain[-1]
            if record["before"] != latest["after"]:
                latest_run_id = _integer(latest["run_id"], "run identity")
                _validate_administrative_credit_replay(
                    metadata,
                    owner=latest,
                    owner_metadata=metadata_by_run[latest_run_id],
                )
                continue
        chain.append(record)
        metadata_by_run[record_run_id] = metadata
    if not chain:
        raise RuntimeError("accepted-components ledger has no completed record")
    latest = chain[-1]
    latest_run_id = _integer(latest["run_id"], "run identity")
    latest_metadata = metadata_by_run[latest_run_id]
    delta = latest_metadata["_delta"]
    manifest = _manifest_identity(latest_metadata, delta["live_readback"])
    timestamp = time.time() if issued_at is None else issued_at
    return {
        "protocol": PROTOCOL,
        "board": BOARD,
        "metric": METRIC,
        "request_nonce": nonce,
        "issued_at": timestamp,
        "chain": chain,
        "latest_owner": {
            **latest,
            "current": latest["after"],
            "denominator": 153,
            "unit": "components",
            "mismatch": 0,
            "live_readback": manifest["uri"],
            "manifest": manifest,
        },
    }


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, address: tuple[str, int], token: str):
        if address[0] != "127.0.0.1":
            raise RuntimeError("control-plane helper must bind exact Mac loopback")
        self.token = token
        super().__init__(address, _Handler)


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def _json(self, status: int, value: object) -> None:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        if self.path != READY_PATH:
            self._json(404, {"error": "not-found"})
            return
        self._json(200, {"protocol": PROTOCOL, "status": "ready"})

    def do_POST(self) -> None:
        if self.path != REQUEST_PATH:
            self._json(404, {"error": "not-found"})
            return
        authorization = self.headers.get("Authorization", "")
        server = self.server
        if not isinstance(server, _Server):
            self._json(503, {"error": "server-binding"})
            return
        if not hmac.compare_digest(authorization, f"Bearer {server.token}"):
            self._json(401, {"error": "unauthorized"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 1 <= length <= 256:
                raise RuntimeError("request size is invalid")
            value = json.loads(self.rfile.read(length))
            if not isinstance(value, dict) or set(value) != {"request_nonce"}:
                raise RuntimeError("request body is malformed")
            response = build_response(value["request_nonce"])
        except (json.JSONDecodeError, RuntimeError, OSError) as error:
            self._json(503, {"error": type(error).__name__})
            return
        self._json(200, response)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    token = os.environ.get(TOKEN_ENV, "")
    if len(token) < 32:
        raise RuntimeError(f"ephemeral bearer token missing from {TOKEN_ENV}")
    server = _Server(("127.0.0.1", args.port), token)
    print(f"READY 127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
