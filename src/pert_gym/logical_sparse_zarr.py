"""Local, append-only writer and reader for logical sparse-Zarr revisions.

The module deliberately has no cloud or Lamin dependency.  A VM-only CLI owns
source loading and any future remote publication; this module makes the logical
surface deterministic, resumable, and testable before a candidate is promoted.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import resource
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, TypeAlias

import numpy as np
import pandas as pd
import zarr
from scipy import sparse

from pert_gym.sparse_zarr_contract import (
    LogicalSparseSurface,
    adaptive_target_rows,
    balanced_row_chunks,
    validate_logical_sparse_surface,
)

WRITER_VERSION = "pert-gym.logical-sparse-zarr.writer/v1"
DEFAULT_MIN_ROWS = 5_000
DEFAULT_MAX_ROWS = 100_000
DEFAULT_BYTES_PER_NNZ = 12
DEFAULT_MATERIALIZATION_FACTOR = 3.0
SOURCE_CHECKSUM_RE = re.compile(r"^sha256-file-bytes/v1:[0-9a-fA-F]{64}$")
CompressedMatrix: TypeAlias = sparse.csr_matrix | sparse.csc_matrix


class MigrationInterrupted(RuntimeError):
    """A test or operator intentionally stopped a resumable candidate."""


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", "utf-8")
    temporary.replace(path)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_array(value: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(value)
    return _sha256_bytes(contiguous.tobytes(order="C"))


def _json_scalar(value: object) -> object:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            raise ValueError("var identity cannot serialize non-finite numbers")
        return float(value)
    if pd.isna(value):
        return None
    raise TypeError(f"unsupported var identity scalar {type(value).__name__}")


@dataclass(frozen=True)
class VarIdentity:
    """The exact, order-sensitive shared-var identity mandated by ADR 0001."""

    index_sha256: str
    frame_sha256: str
    schema_fingerprint: str

    @property
    def key(self) -> str:
        schema_digest = _sha256_bytes(self.schema_fingerprint.encode("utf-8"))
        return f"{self.index_sha256}-{self.frame_sha256}-{schema_digest}"


def shared_var_identity(var: pd.DataFrame, *, schema_fingerprint: str) -> VarIdentity:
    """Hash an exact var index plus canonical JSON-lines frame representation."""
    if not schema_fingerprint:
        raise ValueError("schema_fingerprint must be non-empty")
    index = [str(value) for value in var.index]
    index_bytes = ("\n".join(index) + "\n").encode("utf-8")
    columns = sorted(str(column) for column in var.columns)
    lines = []
    for position, index_value in enumerate(index):
        row = {"index": index_value}
        for column in columns:
            row[column] = _json_scalar(var.iloc[position][column])
        lines.append(
            json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
    frame_bytes = ("\n".join(lines) + "\n").encode("utf-8")
    return VarIdentity(
        index_sha256=_sha256_bytes(index_bytes),
        frame_sha256=_sha256_bytes(frame_bytes),
        schema_fingerprint=schema_fingerprint,
    )


def _index_sha256(frame: pd.DataFrame) -> str:
    """Hash the exact UTF-8 index order used to bind an obs resume."""
    return _sha256_bytes(
        ("\n".join(str(value) for value in frame.index) + "\n").encode("utf-8")
    )


def _frame_sha256(frame: pd.DataFrame) -> str:
    """Bind a resume to exact obs values as well as its index/order."""
    values = pd.util.hash_pandas_object(frame, index=True, categorize=True).values
    schema = "\n".join(f"{column}:{dtype}" for column, dtype in frame.dtypes.items())
    return _sha256_bytes(
        np.ascontiguousarray(values).tobytes() + schema.encode("utf-8")
    )


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    """Create a candidate-local lock without replacing an existing writer lock."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise RuntimeError(f"single-writer lock already exists: {path}") from error
    try:
        os.write(descriptor, f"pid={os.getpid()}\n".encode())
        yield
    finally:
        os.close(descriptor)
        path.unlink(missing_ok=True)


def _peak_rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if os.uname().sysname == "Darwin" else peak * 1024


def _matrix_components(
    matrix: CompressedMatrix,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return matrix.data, matrix.indices, matrix.indptr


def _source_sparse_format(matrix: object) -> str:
    if sparse.isspmatrix_csr(matrix):
        return "csr"
    if sparse.isspmatrix_csc(matrix):
        return "csc"
    sparse_format = getattr(matrix, "format", None)
    if sparse_format in {"csr", "csc"} and hasattr(matrix, "__getitem__"):
        return sparse_format
    raise TypeError("matrix must be CSR or CSC; format conversion must be explicit")


def _source_nnz(matrix: object) -> int:
    nnz = getattr(matrix, "nnz", None)
    if nnz is not None:
        return int(nnz)
    indptr = getattr(matrix, "_indptr", None)
    if indptr is not None and len(indptr):
        return int(indptr[-1])
    raise TypeError("backed sparse matrix must expose nnz or _indptr")


def _materialize_rows(
    matrix: object, start: int, end: int, sparse_format: str
) -> CompressedMatrix:
    """Materialize only one logical obs interval from an in-memory or backed X."""
    chunk = matrix[start:end]  # type: ignore[index]
    if sparse_format == "csr" and sparse.isspmatrix_csr(chunk):
        return chunk.copy()
    if sparse_format == "csc" and sparse.isspmatrix_csc(chunk):
        return chunk.copy()
    raise TypeError("backed sparse row access did not preserve declared orientation")


def _assert_source_readback_parity(
    source: CompressedMatrix,
    loaded: CompressedMatrix,
    source_obs: pd.DataFrame,
    loaded_obs: pd.DataFrame,
) -> None:
    if source.shape != loaded.shape or source.nnz != loaded.nnz:
        raise RuntimeError("source/readback matrix shape or nnz mismatch")
    for source_values, loaded_values in zip(
        _matrix_components(source), _matrix_components(loaded)
    ):
        if _sha256_array(source_values) != _sha256_array(loaded_values):
            raise RuntimeError("source/readback sparse payload mismatch")
    if not source_obs.equals(loaded_obs):
        raise RuntimeError("source/readback obs identity or order mismatch")


def _write_matrix(
    path: Path, matrix: CompressedMatrix, sparse_format: str
) -> dict[str, str]:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing chunk: {path}")
    group = zarr.open_group(str(path), mode="w")
    group.attrs.update(
        {
            "sparse_format": sparse_format,
            "shape": list(matrix.shape),
            "nnz": int(matrix.nnz),
        }
    )
    data, indices, indptr = _matrix_components(matrix)
    for name, values in (("data", data), ("indices", indices), ("indptr", indptr)):
        group.create_dataset(
            name, data=values, chunks=(max(1, min(len(values), 65_536)),)
        )
    return {
        "data_sha256": _sha256_array(data),
        "indices_sha256": _sha256_array(indices),
        "indptr_sha256": _sha256_array(indptr),
    }


def _read_matrix(path: Path, sparse_format: str) -> CompressedMatrix:
    group = zarr.open_group(str(path), mode="r")
    constructor = sparse.csr_matrix if sparse_format == "csr" else sparse.csc_matrix
    return constructor(
        (
            np.asarray(group["data"]),
            np.asarray(group["indices"]),
            np.asarray(group["indptr"]),
        ),
        shape=tuple(group.attrs["shape"]),
    )


def _safe_relative_key(value: str) -> Path:
    result = Path(value)
    if not value or result.is_absolute() or ".." in result.parts:
        raise ValueError("logical_key must be a non-empty relative path without '..'")
    return result


def _candidate_root(root: Path, logical_key: str, revision: str) -> Path:
    if not revision or "/" in revision or ".." in revision:
        raise ValueError("revision must be a simple non-empty name")
    return root / _safe_relative_key(logical_key) / "revisions" / revision


def _checkpoint_path(root: Path, logical_key: str, revision: str) -> Path:
    return root / _safe_relative_key(logical_key) / "checkpoints" / f"{revision}.json"


def _checkpoint_base(
    *,
    logical_key: str,
    revision: str,
    shape: tuple[int, int],
    nnz: int,
    sparse_format: str,
    source_checksum: str,
    source_uri: str,
    source_row_start: int,
    ingestion_run_id: str,
    var_identity: VarIdentity,
    obs_index_sha256: str,
    obs_frame_sha256: str,
    chunks: tuple[tuple[int, int], ...],
) -> dict[str, object]:
    return {
        "format": "pert-gym.logical-sparse-zarr.checkpoint",
        "version": 1,
        "logical_key": logical_key,
        "revision": revision,
        "shape": list(shape),
        "nnz": nnz,
        "sparse_format": sparse_format,
        "source_checksum": source_checksum,
        "source_uri": source_uri,
        "source_row_start": source_row_start,
        "ingestion_run_id": ingestion_run_id,
        "var_index_sha256": var_identity.index_sha256,
        "var_frame_sha256": var_identity.frame_sha256,
        "schema_fingerprint": var_identity.schema_fingerprint,
        "obs_index_sha256": obs_index_sha256,
        "obs_frame_sha256": obs_frame_sha256,
        "planned_chunks": [list(chunk) for chunk in chunks],
        "completed_chunks": [],
        "status": "in_progress",
        "writer_version": WRITER_VERSION,
    }


def _load_or_create_checkpoint(
    path: Path, expected: Mapping[str, object]
) -> dict[str, object]:
    if not path.exists():
        result = dict(expected)
        _atomic_json(path, result)
        return result
    checkpoint = json.loads(path.read_text("utf-8"))
    for field in (
        "logical_key",
        "revision",
        "shape",
        "nnz",
        "sparse_format",
        "source_checksum",
        "source_uri",
        "source_row_start",
        "ingestion_run_id",
        "var_index_sha256",
        "var_frame_sha256",
        "schema_fingerprint",
        "obs_index_sha256",
        "obs_frame_sha256",
        "planned_chunks",
    ):
        if checkpoint.get(field) != expected[field]:
            raise RuntimeError(
                f"checkpoint mismatch for {field}; refusing incompatible resume"
            )
    if checkpoint.get("status") == "completed":
        raise RuntimeError(
            "candidate is already completed; promote or choose a new revision"
        )
    return checkpoint


def _ensure_shared_var(root: Path, identity: VarIdentity, var: pd.DataFrame) -> str:
    relative = Path("vars") / identity.key / "var.parquet"
    path = root / relative
    if path.exists():
        existing = pd.read_parquet(path)
        if (
            shared_var_identity(
                existing, schema_fingerprint=identity.schema_fingerprint
            )
            != identity
        ):
            raise RuntimeError(f"existing shared var identity does not match: {path}")
        return relative.as_posix()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.parquet")
    var.to_parquet(temporary)
    temporary.replace(path)
    return relative.as_posix()


def _observed_target_rows(
    matrix: object,
    *,
    sparse_format: str,
    target_rows: int,
    max_rss_bytes: int,
    min_rows: int,
    max_rows: int,
) -> int:
    """Sample materialization before writes so the final plan remains balanced."""
    sample_end = min(matrix.shape[0], target_rows)  # type: ignore[attr-defined]
    sample = _materialize_rows(matrix, 0, sample_end, sparse_format)
    observed = _peak_rss_bytes()
    del sample
    if observed <= max_rss_bytes:
        return target_rows
    scaled = max(min_rows, int(target_rows * max_rss_bytes / observed))
    return max(min_rows, min(max_rows, scaled, matrix.shape[0]))  # type: ignore[attr-defined]


def write_logical_sparse_revision(
    *,
    root: Path,
    logical_key: str,
    revision: str,
    matrix: object,
    obs: pd.DataFrame,
    var: pd.DataFrame,
    schema_fingerprint: str,
    source_uri: str,
    source_checksum: str,
    source_row_start: int = 0,
    ingestion_run_id: str,
    max_rss_bytes: int = 4 * 1024**3,
    min_rows: int = DEFAULT_MIN_ROWS,
    max_rows: int = DEFAULT_MAX_ROWS,
    stop_after_chunks: int | None = None,
) -> dict[str, object]:
    """Append a resumable candidate revision without promoting or overwriting it.

    ``stop_after_chunks`` exists only for deterministic interruption tests and is
    deliberately not exposed by the production CLI.
    """
    if not source_uri or not ingestion_run_id:
        raise ValueError("source_uri and ingestion_run_id must be non-empty")
    if not SOURCE_CHECKSUM_RE.fullmatch(source_checksum):
        raise ValueError(
            "source_checksum must use sha256-file-bytes/v1:<64-hex-digest>"
        )
    shape = getattr(matrix, "shape", None)
    if not isinstance(shape, tuple) or len(shape) != 2:
        raise TypeError("matrix must expose a two-dimensional shape")
    if shape != (len(obs), len(var)):
        raise ValueError("matrix shape must match obs and var row counts")
    if source_row_start < 0:
        raise ValueError("source_row_start must be non-negative")
    sparse_format = _source_sparse_format(matrix)
    matrix_nnz = _source_nnz(matrix)
    candidate = _candidate_root(root, logical_key, revision)
    checkpoint_path = _checkpoint_path(root, logical_key, revision)
    lock_path = candidate / ".writer.lock"
    identity = shared_var_identity(var, schema_fingerprint=schema_fingerprint)
    if shape[0] == 0:
        chunks = ()
    else:
        initial_target = adaptive_target_rows(
            n_obs=shape[0],
            n_vars=shape[1],
            nnz=matrix_nnz,
            max_rss_bytes=max_rss_bytes,
            min_rows=min_rows,
            max_rows=max_rows,
            bytes_per_nnz=DEFAULT_BYTES_PER_NNZ,
            materialization_factor=DEFAULT_MATERIALIZATION_FACTOR,
        )
        target_rows = _observed_target_rows(
            matrix,
            sparse_format=sparse_format,
            target_rows=initial_target,
            max_rss_bytes=max_rss_bytes,
            min_rows=min_rows,
            max_rows=max_rows,
        )
        chunks = balanced_row_chunks(shape[0], target_rows)
    expected = _checkpoint_base(
        logical_key=logical_key,
        revision=revision,
        shape=shape,
        nnz=matrix_nnz,
        sparse_format=sparse_format,
        source_checksum=source_checksum,
        source_uri=source_uri,
        source_row_start=source_row_start,
        ingestion_run_id=ingestion_run_id,
        var_identity=identity,
        obs_index_sha256=_index_sha256(obs),
        obs_frame_sha256=_frame_sha256(obs),
        chunks=chunks,
    )
    with _exclusive_lock(lock_path):
        checkpoint = _load_or_create_checkpoint(checkpoint_path, expected)
        shared_key = _ensure_shared_var(root, identity, var)
        completed_raw = checkpoint.get("completed_chunks")
        if not isinstance(completed_raw, list):
            raise RuntimeError("checkpoint completed_chunks must be a list")
        if any(
            not isinstance(index, int) or isinstance(index, bool)
            for index in completed_raw
        ):
            raise RuntimeError(
                "checkpoint completed_chunks must contain integer indexes"
            )
        completed = set(completed_raw)
        records: list[dict[str, object]] = []
        for index, (start, end) in enumerate(chunks):
            chunk_key = f"chunks/chunk_{index:06d}.zarr"
            obs_key = f"obs/chunk_{index:06d}.parquet"
            chunk_path = candidate / chunk_key
            obs_path = candidate / obs_key
            source_chunk = _materialize_rows(matrix, start, end, sparse_format)
            source_obs = obs.iloc[start:end]
            payload_exists = chunk_path.exists()
            obs_exists = obs_path.exists()
            if index in completed:
                if not payload_exists or not obs_exists:
                    raise RuntimeError(
                        f"checkpoint says chunk {index} completed but payload is missing"
                    )
            elif payload_exists or obs_exists:
                if not payload_exists or not obs_exists:
                    raise RuntimeError(
                        f"incomplete pre-checkpoint payload for chunk {index}; refusing overwrite"
                    )
                completed.add(index)
                checkpoint["completed_chunks"] = sorted(completed)
                checkpoint["last_completed_end"] = end
                checkpoint["updated_at"] = time.time()
                _atomic_json(checkpoint_path, checkpoint)
            else:
                _write_matrix(chunk_path, source_chunk, sparse_format)
                obs_path.parent.mkdir(parents=True, exist_ok=True)
                source_obs.to_parquet(obs_path)
                completed.add(index)
                checkpoint["completed_chunks"] = sorted(completed)
                checkpoint["last_completed_end"] = end
                checkpoint["updated_at"] = time.time()
                _atomic_json(checkpoint_path, checkpoint)
                if (
                    stop_after_chunks is not None
                    and len(completed) >= stop_after_chunks
                ):
                    raise MigrationInterrupted(
                        f"interrupted after chunk {index} for resume test"
                    )
            loaded = _read_matrix(chunk_path, sparse_format)
            loaded_obs = pd.read_parquet(obs_path)
            _assert_source_readback_parity(source_chunk, loaded, source_obs, loaded_obs)
            data, indices, indptr = _matrix_components(loaded)
            records.append(
                {
                    "key": chunk_key,
                    "start": start,
                    "end": end,
                    "nnz": int(loaded.nnz),
                    "shape": [end - start, shape[1]],
                    "dtype": str(loaded.dtype),
                    "checksums": {
                        "data_sha256": _sha256_array(data),
                        "indices_sha256": _sha256_array(indices),
                        "indptr_sha256": _sha256_array(indptr),
                    },
                    "obs": {
                        "key": obs_key,
                        "provenance": {
                            "source_uri": source_uri,
                            "source_checksum": source_checksum,
                            "source_row_start": source_row_start + start,
                            "source_row_end": source_row_start + end,
                            "ingestion_run_id": ingestion_run_id,
                            "writer_version": WRITER_VERSION,
                        },
                    },
                }
            )
        manifest: dict[str, object] = {
            "format": "pert-gym.logical-sparse-zarr",
            "version": 1,
            "revision": revision,
            "shape": list(shape),
            "nnz": matrix_nnz,
            "sparse_format": sparse_format,
            "chunks": records,
            "shared_var": {
                "key": shared_key,
                "index_sha256": identity.index_sha256,
                "frame_sha256": identity.frame_sha256,
                "schema_fingerprint": identity.schema_fingerprint,
            },
        }
        validate_logical_sparse_surface(manifest)
        manifest_path = candidate / "manifest.json"
        if manifest_path.exists():
            raise FileExistsError(
                f"refusing to overwrite immutable manifest: {manifest_path}"
            )
        _atomic_json(manifest_path, manifest)
        checkpoint.update(
            {
                "status": "completed",
                "manifest": str(manifest_path),
                "updated_at": time.time(),
            }
        )
        _atomic_json(checkpoint_path, checkpoint)
        return manifest


def read_logical_sparse_revision(
    root: Path, logical_key: str, revision: str
) -> tuple[LogicalSparseSurface, sparse.spmatrix, pd.DataFrame, pd.DataFrame]:
    """Load a candidate and verify per-chunk checksums and obs/var parity."""
    candidate = _candidate_root(root, logical_key, revision)
    manifest = json.loads((candidate / "manifest.json").read_text("utf-8"))
    surface = validate_logical_sparse_surface(manifest)
    matrices = []
    obs_frames = []
    for chunk in surface.chunks:
        matrix = _read_matrix(candidate / chunk.key, surface.sparse_format)
        if matrix.shape != chunk.shape or matrix.nnz != chunk.nnz:
            raise RuntimeError(f"readback shape/nnz mismatch for {chunk.key}")
        arrays = _matrix_components(matrix)
        for name, values in zip(
            ("data_sha256", "indices_sha256", "indptr_sha256"), arrays
        ):
            if _sha256_array(values).lower() != chunk.checksums[name].lower():
                raise RuntimeError(f"readback checksum mismatch for {chunk.key}:{name}")
        obs = pd.read_parquet(candidate / str(chunk.obs["key"]))
        if len(obs) != chunk.shape[0]:
            raise RuntimeError(f"obs row count mismatch for {chunk.obs['key']}")
        matrices.append(matrix)
        obs_frames.append(obs)
    if matrices:
        combined = sparse.vstack(matrices, format=surface.sparse_format)
    elif surface.sparse_format == "csr":
        combined = sparse.csr_matrix(surface.shape)
    else:
        combined = sparse.csc_matrix(surface.shape)
    obs = pd.concat(obs_frames, axis=0) if obs_frames else pd.DataFrame()
    var = pd.read_parquet(root / surface.shared_var["key"])
    if (
        combined.shape != surface.shape
        or combined.nnz != surface.nnz
        or len(obs) != surface.shape[0]
    ):
        raise RuntimeError("candidate denominator/readback parity failed")
    identity = shared_var_identity(
        var, schema_fingerprint=surface.shared_var["schema_fingerprint"]
    )
    if (
        identity.index_sha256.lower() != surface.shared_var["index_sha256"].lower()
        or identity.frame_sha256.lower() != surface.shared_var["frame_sha256"].lower()
    ):
        raise RuntimeError("shared var identity mismatch during readback")
    return surface, combined, obs, var


def promote_revision(root: Path, logical_key: str, revision: str) -> Path:
    """Atomically promote an already-verified immutable candidate manifest."""
    read_logical_sparse_revision(root, logical_key, revision)
    alias = root / _safe_relative_key(logical_key) / "aliases" / "current.json"
    _atomic_json(
        alias, {"revision": revision, "manifest": f"revisions/{revision}/manifest.json"}
    )
    return alias


def rollback_to_revision(
    root: Path, logical_key: str, revision: str, *, reason: str
) -> Path:
    """Retain audit metadata while atomically repointing the logical alias."""
    if not reason:
        raise ValueError("rollback reason must be non-empty")
    alias = promote_revision(root, logical_key, revision)
    record = (
        root
        / _safe_relative_key(logical_key)
        / "rollbacks"
        / f"{int(time.time() * 1_000_000)}.json"
    )
    _atomic_json(record, {"revision": revision, "reason": reason, "alias": str(alias)})
    return record
