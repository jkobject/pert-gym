"""Deterministic, bounded adapter for immutable same-prefix legacy triplets."""

from __future__ import annotations

import hashlib
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import anndata as ad
import pandas as pd
from scipy import sparse

from pert_gym.logical_sparse_zarr import (
    DEFAULT_MAX_ROWS,
    DEFAULT_MIN_ROWS,
    shared_var_identity,
    write_logical_sparse_revision,
)


@dataclass(frozen=True)
class LegacyTriplet:
    """Local materializations plus immutable Lamin artifact identity for one shard."""

    chunk_id: int
    obs_path: Path
    x_path: Path
    var_path: Path
    obs_artifact_id: str
    x_artifact_id: str
    var_artifact_id: str
    obs_key: str
    x_key: str
    var_key: str


_TRIPLET_KEY = re.compile(
    r"^(?P<prefix>.+)/chunk_(?P<chunk>\d+)/(?P<role>obs\.parquet|X\.h5ad|var\.parquet)$"
)


def resolve_legacy_triplets(
    *, ln: Any, prefix: str, cache_dir: Path
) -> list[LegacyTriplet]:
    """Resolve exactly one complete numeric Lamin triplet per shard.

    The initial query is metadata-only. Each immutable payload is cached once,
    then opened backed by ``build_legacy_revision`` rather than family-loaded.
    """
    prefix = prefix.rstrip("/")
    if not prefix:
        raise ValueError("legacy prefix must be non-empty")
    records = list(ln.Artifact.filter(key__startswith=f"{prefix}/chunk_").all())
    grouped: dict[int, dict[str, Any]] = {}
    for artifact in records:
        key = str(getattr(artifact, "key", ""))
        match = _TRIPLET_KEY.fullmatch(key)
        if match is None or match["prefix"] != prefix:
            continue
        chunk = int(match["chunk"])
        role = match["role"]
        if role in grouped.setdefault(chunk, {}):
            raise ValueError(f"duplicate legacy artifact role {role} for chunk {chunk}")
        grouped[chunk][role] = artifact
    if not grouped:
        raise ValueError(f"no numeric legacy triplets found below {prefix!r}")
    cache_dir.mkdir(parents=True, exist_ok=True)
    result: list[LegacyTriplet] = []
    for chunk in sorted(grouped):
        roles = grouped[chunk]
        expected = {"obs.parquet", "X.h5ad", "var.parquet"}
        if set(roles) != expected:
            raise ValueError(f"incomplete legacy triplet for chunk {chunk}")
        local = cache_dir / f"chunk_{chunk:06d}"
        local.mkdir(exist_ok=True)
        paths: dict[str, Path] = {}
        for role, artifact in roles.items():
            cached = Path(artifact.cache())
            target = local / role
            if cached.resolve() != target.resolve():
                shutil.copy2(cached, target)
            paths[role] = target
        result.append(
            LegacyTriplet(
                chunk_id=chunk,
                obs_path=paths["obs.parquet"],
                x_path=paths["X.h5ad"],
                var_path=paths["var.parquet"],
                obs_artifact_id=str(getattr(roles["obs.parquet"], "uid", "")),
                x_artifact_id=str(getattr(roles["X.h5ad"], "uid", "")),
                var_artifact_id=str(getattr(roles["var.parquet"], "uid", "")),
                obs_key=str(getattr(roles["obs.parquet"], "key", "")),
                x_key=str(getattr(roles["X.h5ad"], "key", "")),
                var_key=str(getattr(roles["var.parquet"], "key", "")),
            )
        )
    return _ordered_complete(result)


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _ordered_complete(triplets: Sequence[LegacyTriplet]) -> list[LegacyTriplet]:
    ordered = sorted(triplets, key=lambda item: item.chunk_id)
    if not ordered:
        raise ValueError("legacy triplet list must be non-empty")
    ids = [item.chunk_id for item in ordered]
    if any(
        not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in ids
    ):
        raise ValueError("legacy chunk IDs must be non-negative integers")
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate legacy chunk IDs")
    if ids != list(range(ids[-1] + 1)):
        raise ValueError("legacy chunk IDs must be contiguous from zero")
    for item in ordered:
        if not all(
            path.is_file() for path in (item.obs_path, item.x_path, item.var_path)
        ):
            raise ValueError(f"incomplete legacy triplet for chunk {item.chunk_id}")
        if not all(
            (
                item.obs_artifact_id,
                item.x_artifact_id,
                item.var_artifact_id,
                item.obs_key,
                item.x_key,
                item.var_key,
            )
        ):
            raise ValueError(
                f"legacy triplet identity is incomplete for chunk {item.chunk_id}"
            )
    return ordered


class _LegacyMatrix:
    format = "csr"

    def __init__(
        self,
        triplets: Sequence[LegacyTriplet],
        row_ends: Sequence[int],
        n_vars: int,
        nnz: int,
    ):
        self.triplets = triplets
        self.row_ends = row_ends
        self.shape = (row_ends[-1], n_vars)
        self.nnz = nnz

    def __getitem__(self, selection: slice) -> sparse.csr_matrix:
        if not isinstance(selection, slice) or selection.step not in (None, 1):
            raise TypeError("legacy adapter supports contiguous row slices only")
        start, end, _ = selection.indices(self.shape[0])
        parts: list[sparse.csr_matrix] = []
        cursor = 0
        for triplet, stop in zip(self.triplets, self.row_ends):
            overlap_start, overlap_end = max(start, cursor), min(end, stop)
            if overlap_start < overlap_end:
                source = ad.read_h5ad(triplet.x_path, backed="r")
                try:
                    matrix: Any = source.X
                    part = matrix[  # type: ignore[not-subscriptable]
                        overlap_start - cursor : overlap_end - cursor
                    ]
                    if not sparse.isspmatrix_csr(part):
                        raise TypeError("legacy X must be CSR for row-streaming")
                    parts.append(part.copy())  # type: ignore[invalid-argument-type,unresolved-attribute]
                finally:
                    source.file.close()
            cursor = stop
        return (
            sparse.vstack(parts, format="csr")
            if parts
            else sparse.csr_matrix((0, self.shape[1]))
        )


def build_legacy_revision(
    *,
    root: Path,
    logical_key: str,
    revision: str,
    triplets: Sequence[LegacyTriplet],
    schema_fingerprint: str,
    ingestion_run_id: str,
    max_rss_bytes: int = 4 * 1024**3,
    min_rows: int = DEFAULT_MIN_ROWS,
    max_rows: int = DEFAULT_MAX_ROWS,
) -> dict[str, object]:
    """Stream ordered legacy X payloads without materializing the family matrix."""
    ordered = _ordered_complete(triplets)
    first_var = pd.read_parquet(ordered[0].var_path)
    var_identity = shared_var_identity(first_var, schema_fingerprint=schema_fingerprint)
    obs_frames: list[pd.DataFrame] = []
    row_ends: list[int] = []
    source_rows: list[dict[str, object]] = []
    nnz = 0
    total = 0
    for item in ordered:
        var = pd.read_parquet(item.var_path)
        if (
            shared_var_identity(var, schema_fingerprint=schema_fingerprint)
            != var_identity
        ):
            raise ValueError(f"legacy var identity mismatch for chunk {item.chunk_id}")
        obs = pd.read_parquet(item.obs_path)
        source = ad.read_h5ad(item.x_path, backed="r")
        try:
            if source.shape[0] != len(obs) or source.shape[1] != len(first_var):
                raise ValueError(
                    f"legacy obs/X shape mismatch for chunk {item.chunk_id}"
                )
            if getattr(source.X, "format", None) != "csr":
                raise TypeError("legacy X must be CSR for row-streaming")
            indptr = getattr(source.X, "_indptr", None)
            if indptr is None:
                raise TypeError("legacy backed CSR X does not expose indptr")
            nnz += int(indptr[-1])
        finally:
            source.file.close()
        start, total = total, total + len(obs)
        row_ends.append(total)
        obs_frames.append(obs)
        source_rows.append(
            {
                "chunk_id": item.chunk_id,
                "obs_artifact_id": item.obs_artifact_id,
                "x_artifact_id": item.x_artifact_id,
                "var_artifact_id": item.var_artifact_id,
                "obs_key": item.obs_key,
                "x_key": item.x_key,
                "var_key": item.var_key,
                "obs_checksum": _file_checksum(item.obs_path),
                "x_checksum": _file_checksum(item.x_path),
                "var_checksum": _file_checksum(item.var_path),
                "row_start": start,
                "row_end": total,
            }
        )
    identity = {"kind": "legacy-triplets/v1", "sources": source_rows}
    family_checksum = hashlib.sha256(repr(identity).encode()).hexdigest()
    return write_logical_sparse_revision(
        root=root,
        logical_key=logical_key,
        revision=revision,
        matrix=_LegacyMatrix(ordered, row_ends, len(first_var), nnz),
        obs=pd.concat(obs_frames),
        var=first_var,
        schema_fingerprint=schema_fingerprint,
        source_uri=f"lamin://{logical_key}",
        source_checksum=f"sha256-file-bytes/v1:{family_checksum}",
        source_identity=identity,
        ingestion_run_id=ingestion_run_id,
        max_rss_bytes=max_rss_bytes,
        min_rows=min_rows,
        max_rows=max_rows,
    )
