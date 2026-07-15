#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import multiprocessing as mp
import os
import resource
import signal
import socket
import sys
import threading
import time
import urllib.request
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import fsspec
import h5py
import numpy as np
import pandas as pd
import parquet_frame_parity as frame_parity
import writer_contract
import zarr
from anndata._io.specs import read_elem
from scipy import sparse

ROOT = Path.home() / "work" / "pert-gym"
sys.path.insert(0, str(ROOT))
from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import (  # noqa: E402
    lamin_writer_lock,
    legacy_lamin_writer_lock_paths,
    vm_global_lamin_writer_lock_path,
)

TASK_ID: str
URL: str
API_URL: str
LOGICAL: str
GCS_ROOT: str
BILLING_PROJECT: str
SOURCE: dict[str, str]
EXPECTED_HEAD: dict[str, object]
EXPECTED_API: dict[str, object]
N_OBS: int
N_VARS: int
MAX_RSS: int
MIN_AVAILABLE: int
OUT: Path
REVISION_PREFIX: str
EXECUTION_TIMEOUT_SECONDS: int
ACTIVE_CONFIG: dict[str, Any]


def apply_contract(contract: Any) -> None:
    """Populate runtime values only from a fully validated bound contract."""
    global TASK_ID, URL, API_URL, LOGICAL, GCS_ROOT, BILLING_PROJECT
    global SOURCE, EXPECTED_HEAD, EXPECTED_API, N_OBS, N_VARS
    global MAX_RSS, MIN_AVAILABLE, OUT, REVISION_PREFIX
    global EXECUTION_TIMEOUT_SECONDS, ACTIVE_CONFIG

    config = contract.config
    ACTIVE_CONFIG = config
    TASK_ID = config["task_id"]
    URL = config["source"]["url"]
    API_URL = config["source"]["api_url"]
    SOURCE = {
        key: config["source"][key]
        for key in (
            "collection_id",
            "collection_version_id",
            "dataset_id",
            "dataset_version_id",
            "asset_id",
        )
    }
    EXPECTED_HEAD = config["source_head"]
    EXPECTED_API = config["api_identity"]
    N_OBS, N_VARS = config["shape"]
    LOGICAL = config["logical_key"]
    GCS_ROOT = config["storage"]["gcs_root"]
    BILLING_PROJECT = config["execution"]["billing_project"]
    MAX_RSS = config["execution"]["max_rss_bytes"]
    MIN_AVAILABLE = config["execution"]["min_available_bytes"]
    OUT = Path(config["execution"]["output_directory"])
    REVISION_PREFIX = config["revision"]["prefix"]
    EXECUTION_TIMEOUT_SECONDS = config["execution"]["timeout_seconds"]


def now() -> float:
    return time.time()


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_array(value: np.ndarray) -> str:
    return sha256_bytes(np.ascontiguousarray(value).tobytes(order="C"))


def frame_sha256(frame: pd.DataFrame) -> str:
    return frame_parity.semantic_frame_sha256(frame)


def index_sha256(frame: pd.DataFrame) -> str:
    return sha256_bytes(("\n".join(map(str, frame.index)) + "\n").encode())


def ordered_var_identity(ids: list[str]) -> str:
    value = {
        "organism_ontology_id": "NCBITaxon:9606",
        "canonical_feature_namespace": "Ensembl Gene ID",
        "normalization_version": "source-string/v1",
        "n_vars": len(ids),
        "ordered_canonical_feature_identifiers": ids,
    }
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return sha256_bytes(payload)


def mem_available() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable unavailable")


def rss_bytes(pid: int) -> int:
    path = Path(f"/proc/{pid}/status")
    if not path.exists():
        return 0
    for line in path.read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return 0


def heartbeat_progress_rows(n_obs: int) -> range:
    """Return intermediate row checkpoints strictly below the configured shape."""
    return range(2048, n_obs, 1024)


def source_head(contract: Any) -> dict[str, object]:
    request = urllib.request.Request(URL, method="HEAD")
    with urllib.request.urlopen(request, timeout=30) as response:
        result = {
            "status": response.status,
            "final_url": response.url,
            "content_length": int(response.headers["Content-Length"]),
            "etag": response.headers["ETag"],
            "last_modified": response.headers["Last-Modified"],
            "version_id": response.headers.get("x-amz-version-id"),
        }
    observed = {key: result[key] for key in EXPECTED_HEAD}
    if observed != EXPECTED_HEAD:
        raise RuntimeError(f"source HEAD drift: {result}")
    return result


def source_api(contract: Any) -> dict[str, object]:
    with urllib.request.urlopen(API_URL, timeout=30) as response:
        collection = json.load(response)
    datasets = [row for row in collection["datasets"] if row["dataset_id"] == SOURCE["dataset_id"]]
    if len(datasets) != 1:
        raise RuntimeError("exact dataset absent or duplicated in collection API")
    dataset = datasets[0]
    asset_urls = [asset["url"] for asset in dataset["assets"] if asset["filetype"] == "H5AD"]
    observed = {
        "collection_id": collection["collection_id"],
        "collection_version_id": collection["collection_version_id"],
        "dataset_id": dataset["dataset_id"],
        "dataset_version_id": dataset["dataset_version_id"],
        "asset_url": asset_urls[0] if len(asset_urls) == 1 else None,
        "n_obs": dataset["cell_count"],
        "n_vars": dataset["feature_count"],
        "organism": dataset["organism"],
        "assay": dataset["assay"],
        "tombstone": dataset["tombstone"],
        "is_primary_data": dataset.get("is_primary_data"),
        "public": collection.get("visibility") == "PUBLIC",
    }
    expected = {
        "collection_id": SOURCE["collection_id"],
        "collection_version_id": SOURCE["collection_version_id"],
        "dataset_id": SOURCE["dataset_id"],
        "dataset_version_id": SOURCE["dataset_version_id"],
        "asset_url": URL,
        "n_obs": N_OBS,
        "n_vars": N_VARS,
        "organism": [EXPECTED_API["organism"]],
        "assay": EXPECTED_API["assays"],
        "tombstone": EXPECTED_API["tombstone"],
        "is_primary_data": EXPECTED_API["is_primary_data"],
        "public": EXPECTED_API["public"],
    }
    if observed != expected:
        raise RuntimeError(f"source API drift: {observed}")
    return observed


def hash_source() -> tuple[str, float, int]:
    started = time.monotonic()
    digest = hashlib.sha256()
    count = 0
    with urllib.request.urlopen(URL, timeout=120) as response:
        while True:
            block = response.read(8 * 1024**2)
            if not block:
                break
            digest.update(block)
            count += len(block)
    elapsed = time.monotonic() - started
    if count != EXPECTED_HEAD["content_length"]:
        raise RuntimeError(f"source stream length mismatch: {count}")
    return digest.hexdigest(), elapsed, count


def duplicate_probe() -> dict[str, object]:
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == ACTIVE_CONFIG["execution"]["lamin_instance"]
    assert ln.setup.settings.branch.name == ACTIVE_CONFIG["execution"]["lamin_branch"]
    candidates: dict[str, dict[str, object]] = {}
    queries: list[dict[str, object]] = []

    def add(label: str, rows: Any) -> None:
        values = list(rows)
        queries.append({"label": label, "rows": len(values)})
        for row in values:
            candidates[str(row.uid)] = {
                "uid": str(row.uid),
                "key": str(row.key or ""),
                "description": str(row.description or ""),
                "extra_data": row.extra_data,
            }

    add("key__startswith exact logical key", ln.Artifact.filter(key__startswith=LOGICAL).all())
    seen: set[str] = set()
    for name, identifier in SOURCE.items():
        if identifier in seen:
            continue
        seen.add(identifier)
        add(f"key__icontains {name}", ln.Artifact.filter(key__icontains=identifier).all())
        add(f"description__icontains {name}", ln.Artifact.filter(description__icontains=identifier).all())
        try:
            add(f"extra_data__icontains {name}", ln.Artifact.filter(extra_data__icontains=identifier).all())
        except Exception as error:
            queries.append({"label": f"extra_data__icontains {name}", "error": f"{type(error).__name__}: {error}"})
    logical_hits = []
    tuple_hits = []
    source_candidates = []
    for row in candidates.values():
        haystack = "\n".join((str(row["key"]), str(row["description"]), json.dumps(row["extra_data"], sort_keys=True, default=str)))
        if str(row["key"]).startswith(LOGICAL):
            logical_hits.append(row)
        presence = {name: identifier in haystack for name, identifier in SOURCE.items()}
        if any(presence.values()):
            source_candidates.append({**row, "source_tuple_presence": presence})
        if all(presence.values()):
            tuple_hits.append({**row, "source_tuple_presence": presence})
    result = {
        "observed_at": now(),
        "instance": ACTIVE_CONFIG["execution"]["lamin_instance"],
        "branch": ACTIVE_CONFIG["execution"]["lamin_branch"],
        "queries": queries,
        "candidate_rows_returned": len(candidates),
        "exact_logical_key_prefix_hits": logical_hits,
        "exact_full_source_tuple_hits": tuple_hits,
        "exact_source_identifier_candidates": source_candidates,
        "complete_publication_detected": bool(logical_hits or tuple_hits),
    }
    if result["complete_publication_detected"]:
        raise RuntimeError(f"complete duplicate detected: {result}")
    return result


def map_obs(source: pd.DataFrame, organism: str) -> pd.DataFrame:
    if len(source) != N_OBS or not source.index.is_unique or source.index.isna().any():
        raise RuntimeError("source obs identity failed")
    mapper = ACTIVE_CONFIG["obs"]
    required = mapper["required_non_null"]
    for column in required:
        if column not in source or source[column].isna().any():
            raise RuntimeError(f"required obs field failed: {column}")
    for predicate in mapper["predicates"]:
        values = source[predicate["column"]].astype(str)
        if predicate["op"] == "domain_equals":
            passed = set(values.unique()) == set(predicate["values"])
        elif predicate["op"] == "all_contains":
            passed = values.str.contains(predicate["value"], regex=False).all()
        elif predicate["op"] == "all_equals":
            passed = values.eq(str(predicate["value"])).all()
        else:
            raise RuntimeError(f"unknown OBS predicate: {predicate['op']}")
        if not passed:
            raise RuntimeError(f"required OBS predicate failed: {predicate}")
    obs = source.copy()
    for assignment in mapper["assignments"]:
        target = assignment["target"]
        if assignment["op"] == "literal":
            obs[target] = assignment["value"]
        elif assignment["op"] == "index":
            obs[target] = source.index.astype(str)
        elif assignment["op"] == "copy":
            obs[target] = source[assignment["source"]]
        elif assignment["op"] == "concat":
            parts = [source[column].astype(str) for column in assignment["sources"]]
            value = parts[0]
            for part in parts[1:]:
                value = value + assignment["separator"] + part
            obs[target] = value
        elif assignment["op"] == "nullable_float":
            obs[target] = pd.Series(pd.NA, index=obs.index, dtype="Float64")
        else:
            raise RuntimeError(f"unknown OBS assignment: {assignment['op']}")
    return obs


def map_var(source: pd.DataFrame, organism: str) -> tuple[pd.DataFrame, str]:
    ids = [str(value) for value in source.index]
    if len(ids) != N_VARS or source.index.isna().any() or not source.index.is_unique:
        raise RuntimeError("source var identity failed")
    if not all(value.startswith("ENSG") and value[4:].isdigit() for value in ids):
        raise RuntimeError("source var namespace is not stable Homo sapiens Ensembl Gene ID")
    reference_column = ACTIVE_CONFIG["ordered_var"]["feature_reference_column"]
    expected_organism = ACTIVE_CONFIG["ordered_var"]["organism_ontology_id"]
    if not (source[reference_column].astype(str) == expected_organism).all():
        raise RuntimeError("source var organism namespace drift")
    var = source.copy()
    var["ensembl_id"] = ids
    var["gene_symbol"] = var["feature_name"].astype(str)
    var["gene_id"] = ids
    var["organism"] = organism
    var["author_gene_id"] = ids
    var["author_gene_symbol"] = var["feature_name"].astype(str)
    identity = ordered_var_identity(ids)
    if identity != ACTIVE_CONFIG["ordered_var"]["identity_sha256"]:
        raise RuntimeError(f"ordered var identity drift: {identity}")
    return var, identity


def inventory_frame(frame: pd.DataFrame, columns: list[str]) -> dict[str, object]:
    result: dict[str, object] = {}
    for column in columns:
        series = frame[column]
        result[column] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "value_counts": {str(key): int(value) for key, value in series.value_counts(dropna=False).items()},
        }
    return result


def exclusive_bytes(fs: Any, key: str, payload: bytes) -> dict[str, object]:
    if fs.exists(key):
        raise RuntimeError(f"refusing overwrite: gs://{key}")
    with fs.open(key, "xb") as handle:
        handle.write(payload)
    info = fs.info(key)
    if int(info["size"]) != len(payload):
        raise RuntimeError(f"write size mismatch: {key}")
    return {"key": key, "generation": str(info["generation"]), "size": len(payload), "sha256": sha256_bytes(payload)}


def parquet_bytes(frame: pd.DataFrame) -> bytes:
    return frame_parity.parquet_bytes(frame)


def read_parquet(fs: Any, key: str) -> pd.DataFrame:
    with fs.open(key, "rb") as handle:
        return pd.read_parquet(io.BytesIO(handle.read()))


def zarr_inventory(fs: Any, prefix: str) -> list[dict[str, object]]:
    details = fs.find(prefix, detail=True)
    values = details.items() if isinstance(details, dict) else ((key, fs.info(key)) for key in details)
    return [
        {"key": key, "generation": str(info["generation"]), "size": int(info["size"])}
        for key, info in sorted(values)
    ]


def watcher(pid: int, stop: mp.synchronize.Event, output: str) -> None:  # type: ignore[name-defined]
    path = Path(output)
    status = "running"
    exit_code = 0
    started = time.monotonic()
    while not stop.is_set():
        sample = {"observed_at": now(), "pid": pid, "rss_bytes": rss_bytes(pid), "mem_available_bytes": mem_available()}
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(sample, sort_keys=True) + "\n")
        if sample["rss_bytes"] > MAX_RSS or sample["mem_available_bytes"] < MIN_AVAILABLE:
            status = "resource_breach"
            exit_code = 2
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            break
        if time.monotonic() - started > EXECUTION_TIMEOUT_SECONDS:
            status = "execution_timeout"
            exit_code = 4
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            break
        if not Path(f"/proc/{pid}").exists():
            status = "writer_absent"
            exit_code = 3
            break
        stop.wait(10)
    terminal = {"observed_at": now(), "status": "stopped" if stop.is_set() else status, "exit_code": exit_code}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(terminal, sort_keys=True) + "\n")
    raise SystemExit(exit_code)


class Heartbeats:
    def __init__(self, fs: Any, prefix: str, revision: str, lease_id: str, created: list[dict[str, object]], rollback: Path) -> None:
        self.fs = fs
        self.prefix = prefix
        self.revision = revision
        self.lease_id = lease_id
        self.created = created
        self.rollback = rollback
        self.phase = "preflight"
        self.rows = 0
        self.checkpoint = "lease-acquired"
        self.sequence = 0
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def update(self, phase: str, rows: int, checkpoint: str) -> None:
        with self.lock:
            self.phase, self.rows, self.checkpoint = phase, rows, checkpoint
        self.emit()

    def emit(self) -> None:
        with self.lock:
            sequence = self.sequence
            self.sequence += 1
            ledger = ACTIVE_CONFIG["accepted_components"]
            execution = ACTIVE_CONFIG["execution"]
            record = {
                "intent_key": f"pert-gym|publication-wave1|{REVISION_PREFIX}|single-component-writer|v1",
                "revision_uuid": self.revision,
                "lease_id": self.lease_id,
                "product_execution": {
                    "host": socket.gethostname().split(".")[0],
                    "pid": os.getpid(),
                    "phase": self.phase,
                    "payload_heartbeat_at": now(),
                    "metric": execution["heartbeat_metric"],
                    "current": ledger["current"],
                    "denominator": ledger["denominator"],
                },
                "rows_completed": self.rows,
                "checkpoint": self.checkpoint,
                "rss_bytes": rss_bytes(os.getpid()),
                "mem_available_bytes": mem_available(),
            }
            key = f"{self.prefix}/product_execution/{sequence:06d}.json"
            obj = exclusive_bytes(self.fs, key, json_bytes(record))
            self.created.append(obj)
            with (OUT / "product_execution.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
            with self.rollback.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"action": "created", **obj}, sort_keys=True) + "\n")

    def _loop(self) -> None:
        self.emit()
        while not self.stop_event.wait(ACTIVE_CONFIG["execution"]["heartbeat_interval_seconds"]):
            self.emit()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise RuntimeError("heartbeat thread did not stop")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--authorization", type=Path, required=True)
    args = parser.parse_args()
    contract = writer_contract.load_bound_contract(
        args.config,
        args.authorization,
        writer_path=Path(__file__),
        helper_path=Path(__file__).with_name("parquet_frame_parity.py"),
        require_execution=False,
    )
    writer_contract.require_execution_authorized(contract)
    apply_contract(contract)
    started = time.monotonic()
    if socket.gethostname().split(".")[0] != ACTIVE_CONFIG["execution"]["host"]:
        raise RuntimeError("writer is not on the exact authorized host")
    if mem_available() < MIN_AVAILABLE:
        raise RuntimeError("preflight MemAvailable below 4 GiB")
    OUT.mkdir(parents=True, exist_ok=True)

    pre_api = source_api(contract)
    pre_head = source_head(contract)
    source_sha256, raw_hash_seconds, source_bytes = hash_source()
    if source_head(contract) != pre_head:
        raise RuntimeError("source changed during complete hash")

    http = fsspec.filesystem("http")
    with http.open(URL, "rb", block_size=8 * 1024**2, cache_type="readahead") as handle, h5py.File(handle, "r") as h5:
        x = h5["X"]
        if not isinstance(x, h5py.Group) or x.attrs.get("encoding-type") != "csr_matrix":
            raise RuntimeError("source X is not backed CSR")
        shape = tuple(int(value) for value in x.attrs["shape"])
        if shape != (N_OBS, N_VARS):
            raise RuntimeError(f"source shape drift: {shape}")
        source_obs = read_elem(h5["obs"])
        source_var = read_elem(h5["var"])
        organism = str(read_elem(h5["uns"]["organism"]))
        obs = map_obs(source_obs, organism)
        var, ordered_var_sha = map_var(source_var, organism)
        data = np.asarray(x["data"][:])
        indices = np.asarray(x["indices"][:], dtype=np.int64)
        indptr = np.asarray(x["indptr"][:], dtype=np.int64)
        stored_sparse_bytes = sum(int(x[name].id.get_storage_size()) for name in ("data", "indices", "indptr"))
    matrix = sparse.csr_matrix((data, indices, indptr), shape=shape)
    logical_sparse_bytes = int(data.nbytes + indices.nbytes + indptr.nbytes)
    estimated_rss_bytes = logical_sparse_bytes * 4
    if estimated_rss_bytes > MAX_RSS:
        raise RuntimeError(f"estimated RSS exceeds 24 GiB: {estimated_rss_bytes}")
    if matrix.nnz != len(data) or matrix.shape != shape:
        raise RuntimeError("source sparse structure mismatch")

    selected_columns = sorted(
        set(ACTIVE_CONFIG["obs"]["required_non_null"])
        | {assignment["target"] for assignment in ACTIVE_CONFIG["obs"]["assignments"]}
    )
    semantics = {
        **ACTIVE_CONFIG["obs"]["semantic_evidence"],
        "predicates": ACTIVE_CONFIG["obs"]["predicates"],
        "assignments": ACTIVE_CONFIG["obs"]["assignments"],
        "selected_fields": inventory_frame(obs, selected_columns),
    }
    preflight = {
        "observed_at": now(),
        "source_api": pre_api,
        "source_head": pre_head,
        "source_sha256_file_bytes": source_sha256,
        "source_hash_seconds": raw_hash_seconds,
        "source_bytes": source_bytes,
        "source_path_choice": {
            "selected": "complete-hash-bound HTTP source plus backed HDF5 CSR range reuse",
            "authenticated_sparse_reuse_stored_bytes_estimate": stored_sparse_bytes,
            "authenticated_sparse_reuse_logical_bytes_estimate": logical_sparse_bytes,
            "authenticated_sparse_reuse_time_estimate_seconds": raw_hash_seconds * stored_sparse_bytes / source_bytes,
            "rejected": "raw full local file materialization",
            "rejected_bytes": source_bytes,
            "rejected_time_estimate_seconds": raw_hash_seconds,
            "rejection_reason": "backed CSR reuse transfers fewer X bytes and avoids consuming the VM root disk's narrow 50 GiB safety margin",
        },
        "semantics": semantics,
        "var": {"n_vars": len(var), "ordered_var_identity_sha256": ordered_var_sha, "namespace": "Ensembl Gene ID", "unique": var.index.is_unique, "nulls": int(var.index.isna().sum())},
        "resources": {"mem_available_bytes": mem_available(), "estimated_rss_bytes": estimated_rss_bytes, "max_rss_bytes": MAX_RSS},
    }
    (OUT / "prewrite-preflight.json").write_bytes(json_bytes(preflight))

    lock_metadata = {
        "pid": os.getpid(),
        "run_id": f"{TASK_ID}-{REVISION_PREFIX}",
        "host": ACTIVE_CONFIG["execution"]["host"],
        "project": BILLING_PROJECT,
        "zone": ACTIVE_CONFIG["execution"]["zone"],
        "branch": ACTIVE_CONFIG["execution"]["lamin_branch"],
        "started_at": now(),
    }
    with ExitStack() as locks:
        locks.enter_context(lamin_writer_lock(vm_global_lamin_writer_lock_path(), lock_metadata))
        for path in legacy_lamin_writer_lock_paths():
            locks.enter_context(lamin_writer_lock(path, lock_metadata, check_live_metadata=False))
        lease_acquired = now()
        duplicate = duplicate_probe()
        (OUT / "duplicate-probe-under-lease.json").write_bytes(json_bytes(duplicate))

        revision = f"{REVISION_PREFIX}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.urandom(4).hex()}"
        if revision in ACTIVE_CONFIG["revision"]["failed_candidate_denylist"]:
            raise RuntimeError("generated revision is denylisted")
        prefix = f"{GCS_ROOT}/{LOGICAL}/revisions/{revision}"
        var_key = f"{GCS_ROOT}/{LOGICAL}/var/{ordered_var_sha}/var.parquet"
        fs = fsspec.filesystem("gcs", version_aware=True, project=BILLING_PROJECT, requester_pays=BILLING_PROJECT)
        if fs.exists(prefix):
            raise RuntimeError(f"fresh revision is not absent: gs://{prefix}")
        created: list[dict[str, object]] = []
        rollback = OUT / "rollback-map.jsonl"
        rollback.write_text("", encoding="utf-8")
        lease_id = sha256_bytes(f"{revision}|{lease_acquired}|{os.getpid()}".encode())[:24]
        hb = Heartbeats(fs, prefix, revision, lease_id, created, rollback)
        hb.start()
        stop_watch = mp.Event()
        watcher_path = str(OUT / "resource-samples.jsonl")
        watch = mp.Process(
            target=watcher,
            args=(os.getpid(), stop_watch, watcher_path),
            name=f"{REVISION_PREFIX}-watcher",
        )
        watch.start()
        if not watch.is_alive():
            raise RuntimeError("resource watcher failed to start")
        try:
            hb.update("writing", 0, "source-and-metadata-accepted")
            existing_shared_var_parity = None
            if not fs.exists(var_key):
                var_obj = exclusive_bytes(fs, var_key, parquet_bytes(var))
                created.append(var_obj)
                with rollback.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps({"action": "created", **var_obj}, sort_keys=True) + "\n")
                var_created = True
            else:
                remote_var = read_parquet(fs, var_key)
                if ordered_var_identity(list(map(str, remote_var.index))) != ordered_var_sha:
                    raise RuntimeError("existing shared var content mismatch")
                existing_shared_var_parity = frame_parity.assert_parquet_frame_parity(var, remote_var)
                info = fs.info(var_key)
                var_obj = {"key": var_key, "generation": str(info["generation"]), "size": int(info["size"]), "reused": True}
                var_created = False

            obs_key = f"{prefix}/obs.parquet"
            obs_payload = parquet_bytes(obs)
            obs_obj = exclusive_bytes(fs, obs_key, obs_payload)
            created.append(obs_obj)
            with rollback.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"action": "created", **obs_obj}, sort_keys=True) + "\n")
            hb.update("writing", 1024, "rows-0-1024")

            x_key = f"{prefix}/X.zarr"
            if fs.exists(x_key):
                raise RuntimeError("X.zarr unexpectedly exists")
            store = zarr.storage.FSStore(x_key, fs=fs, mode="w", check=False)
            group = zarr.open_group(store=store, mode="w")
            group.attrs.update({"format": "pert-gym.logical-sparse-zarr", "version": 1, "sparse_format": "csr", "shape": list(shape), "nnz": int(matrix.nnz), "row_interval": [0, N_OBS], "ordered_var_identity_sha256": ordered_var_sha})
            for name, values in (("data", data), ("indices", indices), ("indptr", indptr)):
                group.create_dataset(name, data=values, chunks=(max(1, min(len(values), 65536)),))
            store.close()
            x_objects = zarr_inventory(fs, x_key)
            created.extend(x_objects)
            with rollback.open("a", encoding="utf-8") as handle:
                for obj in x_objects:
                    handle.write(json.dumps({"action": "created", **obj}, sort_keys=True) + "\n")
            for rows in heartbeat_progress_rows(N_OBS):
                hb.update("writing", rows, f"rows-0-{rows}")
            hb.update("checkpointing", N_OBS, "full-source-readback-parity")

            remote_obs = read_parquet(fs, obs_key)
            obs_parity = frame_parity.assert_parquet_frame_parity(obs, remote_obs)
            remote_var = read_parquet(fs, var_key)
            if ordered_var_identity(list(map(str, remote_var.index))) != ordered_var_sha:
                raise RuntimeError("shared var ordered feature identity mismatch")
            var_parity = frame_parity.assert_parquet_frame_parity(var, remote_var)
            remote_store = zarr.storage.FSStore(x_key, fs=fs, mode="r", check=False)
            remote_group = zarr.open_group(store=remote_store, mode="r")
            remote_data = np.asarray(remote_group["data"])
            remote_indices = np.asarray(remote_group["indices"])
            remote_indptr = np.asarray(remote_group["indptr"])
            remote_store.close()
            sparse_hashes = {
                "data": {"source": sha256_array(data), "destination": sha256_array(remote_data)},
                "indices": {"source": sha256_array(indices), "destination": sha256_array(remote_indices)},
                "indptr": {"source": sha256_array(indptr), "destination": sha256_array(remote_indptr)},
            }
            if any(item["source"] != item["destination"] for item in sparse_hashes.values()):
                raise RuntimeError("full sparse payload parity mismatch")
            remote_matrix = sparse.csr_matrix((remote_data, remote_indices, remote_indptr), shape=shape)
            sample_rows = sorted(set([0, N_OBS // 2, N_OBS - 1, *range(0, N_OBS, 1024)]))
            sample_parity = []
            for row in sample_rows:
                source_row = matrix.getrow(row)
                dest_row = remote_matrix.getrow(row)
                equal = source_row.nnz == dest_row.nnz and np.array_equal(source_row.indices, dest_row.indices) and np.array_equal(source_row.data, dest_row.data)
                sample_parity.append({"row": row, "equal": bool(equal), "nnz": int(source_row.nnz)})
            if not all(item["equal"] for item in sample_parity):
                raise RuntimeError("sample/boundary sparse parity mismatch")

            post_api = source_api(contract)
            post_head = source_head(contract)
            if post_api != pre_api or post_head != pre_head:
                raise RuntimeError("source inventory changed during writer")
            hb.update("checkpointing", N_OBS, "source-inventory-unchanged")
            hb.stop()
            stop_watch.set()
            watch.join(timeout=30)
            if watch.is_alive():
                watch.terminate()
                watch.join()
                raise RuntimeError("watcher did not exit")
            if watch.exitcode != 0:
                raise RuntimeError(f"watcher exit {watch.exitcode}")

            samples = [json.loads(line) for line in Path(watcher_path).read_text().splitlines() if line.strip()]
            numeric_samples = [row for row in samples if "rss_bytes" in row]
            safety = {
                "watcher_exit": watch.exitcode,
                "samples": len(numeric_samples),
                "hwm_bytes": max([row["rss_bytes"] for row in numeric_samples] + [resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024]),
                "min_available_bytes": min(row["mem_available_bytes"] for row in numeric_samples),
                "max_rss_bytes": MAX_RSS,
                "min_available_floor_bytes": MIN_AVAILABLE,
                "violations": 0,
            }
            if safety["hwm_bytes"] > MAX_RSS or safety["min_available_bytes"] < MIN_AVAILABLE:
                raise RuntimeError(f"terminal resource gate failed: {safety}")

            product_execution_obj = exclusive_bytes(fs, f"{prefix}/product_execution.jsonl", (OUT / "product_execution.jsonl").read_bytes())
            created.append(product_execution_obj)
            with rollback.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"action": "created", **product_execution_obj}, sort_keys=True) + "\n")
            resource_obj = exclusive_bytes(fs, f"{prefix}/resource-samples.jsonl", Path(watcher_path).read_bytes())
            created.append(resource_obj)
            with rollback.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"action": "created", **resource_obj}, sort_keys=True) + "\n")
            rollback_obj = exclusive_bytes(fs, f"{prefix}/rollback-map.jsonl", rollback.read_bytes())
            created.append(rollback_obj)

            x_objects = zarr_inventory(fs, x_key)
            x_stored_bytes = sum(int(obj["size"]) for obj in x_objects)
            verification = {
                "shape": list(shape),
                "nnz": int(matrix.nnz),
                "row_coverage": [0, N_OBS],
                "gaps": 0,
                "overlaps": 0,
                "duplicate_indices": 0,
                "obs_mismatch": 0,
                "ordered_var_mismatch": 0,
                "sparse_structure_mismatch": 0,
                "sparse_value_mismatch": 0,
                "sparse_hashes": sparse_hashes,
                "sample_and_boundary_parity": sample_parity,
                "obs_index_sha256": index_sha256(obs),
                "obs_frame_sha256": frame_sha256(obs),
                "var_index_sha256": index_sha256(var),
                "var_frame_sha256": frame_sha256(var),
                "parquet_frame_parity": {
                    "obs": obs_parity,
                    "shared_var": var_parity,
                    "existing_shared_var_prewrite": existing_shared_var_parity,
                },
                "ordered_var_identity_sha256": ordered_var_sha,
                "shared_var_count": 1,
                "per_block_var_count": 0,
                "x_logical_object_count": 1,
                "x_stored_bytes": x_stored_bytes,
                "small_dataset_exception": {"applies": x_stored_bytes < 2 * 1024**3, "reason": "whole dataset is genuinely below the standard 2 GiB block minimum"},
            }
            terminal = {
                "runtime_seconds_before_manifest": time.monotonic() - started,
                "lease": {"id": lease_id, "acquired_at": lease_acquired},
                "safety": safety,
                "source_inventory_unchanged": True,
                "writer_exit_expected": 0,
                "watcher_exit": watch.exitcode,
            }
            manifest = {
                "format": "pert-gym.logical-sparse-zarr-component-manifest/v1",
                "task_id": TASK_ID,
                "logical_key": LOGICAL,
                "revision": revision,
                "candidate_prefix": f"gs://{prefix}",
                "source": {**SOURCE, "url": URL, "size_bytes": source_bytes, "etag": EXPECTED_HEAD["etag"], "last_modified": EXPECTED_HEAD["last_modified"], "version_id": EXPECTED_HEAD["version_id"], "sha256_file_bytes": source_sha256},
                "schema": {"obs": "pert-gym canonical obs", "X": "pert-gym.logical-sparse-zarr/v1 CSR", "var_policy": "shared_exact_hash"},
                "objects": {
                    "obs": {**obs_obj, "uri": f"gs://{obs_key}", "X_key": f"gs://{x_key}"},
                    "X": {"uri": f"gs://{x_key}", "objects": x_objects, "var_key": f"gs://{var_key}"},
                    "var": {**var_obj, "uri": f"gs://{var_key}", "created_by_this_revision": var_created},
                    "rollback_map": {**rollback_obj, "uri": f"gs://{prefix}/rollback-map.jsonl"},
                    "product_execution": {**product_execution_obj, "uri": f"gs://{prefix}/product_execution.jsonl"},
                    "resource_samples": {**resource_obj, "uri": f"gs://{prefix}/resource-samples.jsonl"},
                },
                "links": {"obs_to_X": {"from": f"gs://{obs_key}", "to": f"gs://{x_key}"}, "X_to_var": {"from": f"gs://{x_key}", "to": f"gs://{var_key}"}},
                "preflight": preflight,
                "duplicate_probe": duplicate,
                "verification": verification,
                "terminal": terminal,
                "forbidden_actions_performed": {"collection_mutation": False, "lamin_main_write": False, "lamin_registration": False, "promotion": False, "legacy_mutation_or_deletion": False, "vm_lifecycle_change": False},
                "accepted_components_credit": ACTIVE_CONFIG["accepted_components"]["credit"],
            }
            manifest_key = f"{prefix}/manifest.json"
            manifest_payload = json_bytes(manifest)
            manifest_obj = exclusive_bytes(fs, manifest_key, manifest_payload)
            candidate_generations = [int(obj["generation"]) for obj in created if str(obj.get("generation", "")).isdigit()]
            if not candidate_generations or int(manifest_obj["generation"]) <= max(candidate_generations):
                raise RuntimeError("manifest generation is not newer than every candidate object")
            final = {
                "manifest_uri": f"gs://{manifest_key}",
                "manifest_generation": manifest_obj["generation"],
                "manifest_sha256": manifest_obj["sha256"],
                "candidate_prefix": f"gs://{prefix}",
                "revision": revision,
                "verification": verification,
                "terminal": terminal,
                "manifest_last": True,
                "manifest_newer_than_all_candidate_objects": True,
            }
            (OUT / "writer-result.json").write_bytes(json_bytes(final))
            print(json.dumps(final, indent=2, sort_keys=True))
        except BaseException as error:
            try:
                hb.stop()
            except BaseException:
                pass
            if watch.is_alive():
                stop_watch.set()
                watch.join(timeout=20)
            failure = {"status": "failed", "error": f"{type(error).__name__}: {error}", "observed_at": now(), "created_objects": created}
            (OUT / "writer-failure.json").write_bytes(json_bytes(failure))
            raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
