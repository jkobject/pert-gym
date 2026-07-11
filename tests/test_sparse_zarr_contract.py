import json
from pathlib import Path

import pytest

from pert_gym.sparse_zarr_contract import (
    LEGACY_FORMAT,
    SURFACE_FORMAT,
    adaptive_target_rows,
    balanced_row_chunks,
    load_compatible_surface,
    validate_logical_sparse_surface,
)

ROOT = Path(__file__).resolve().parents[1]


def manifest() -> dict:
    return {
        "format": SURFACE_FORMAT,
        "version": 1,
        "shape": [10, 4],
        "nnz": 12,
        "sparse_format": "csr",
        "chunks": [
            {"key": "chunks/0000.zarr", "start": 0, "end": 5, "nnz": 6},
            {"key": "chunks/0001.zarr", "start": 5, "end": 10, "nnz": 6},
        ],
        "shared_var": {
            "key": "vars/abc/var.parquet",
            "index_sha256": "index-hash",
            "frame_sha256": "frame-hash",
        },
        "obs": {"key": "obs.parquet"},
        "provenance": {"source_uri": "gs://example/source.h5ad"},
    }


def test_v1_manifest_requires_exact_chunk_denominator_and_nnz_parity() -> None:
    surface = validate_logical_sparse_surface(manifest())

    assert surface.shape == (10, 4)
    assert [(chunk.start, chunk.end) for chunk in surface.chunks] == [(0, 5), (5, 10)]

    invalid = manifest()
    invalid["chunks"][1]["start"] = 6
    with pytest.raises(ValueError, match="contiguous"):
        validate_logical_sparse_surface(invalid)

    invalid = manifest()
    invalid["chunks"][1]["nnz"] = 5
    with pytest.raises(ValueError, match="nnz sum"):
        validate_logical_sparse_surface(invalid)


def test_balanced_chunks_cover_exact_denominator_without_small_tail() -> None:
    chunks = balanced_row_chunks(n_obs=25_001, target_rows=10_000)

    assert chunks[0][0] == 0
    assert chunks[-1][1] == 25_001
    assert (
        max(end - start for start, end in chunks)
        - min(end - start for start, end in chunks)
        <= 1
    )


def test_adaptive_target_uses_nnz_width_and_rss_bounds() -> None:
    target = adaptive_target_rows(
        n_obs=1_000_000,
        n_vars=20_000,
        nnz=100_000_000,
        max_rss_bytes=20_000_000,
        min_rows=5_000,
        max_rows=100_000,
    )

    assert 5_000 <= target <= 100_000
    assert target < 100_000


def test_legacy_triplet_loader_is_normalized_to_one_chunk() -> None:
    surface = load_compatible_surface(
        {
            "format": LEGACY_FORMAT,
            "n_obs": 3,
            "n_vars": 2,
            "nnz": 4,
            "x_key": "old/X.h5ad",
            "obs_key": "old/obs.parquet",
            "var_key": "old/var.parquet",
        }
    )

    assert surface.format == LEGACY_FORMAT
    assert [(chunk.start, chunk.end, chunk.nnz) for chunk in surface.chunks] == [
        (0, 3, 4)
    ]


def test_machine_readable_policy_covers_required_families_and_vm_benchmark() -> None:
    policy = json.loads(
        (ROOT / "config/logical_sparse_zarr_policy.v1.json").read_text()
    )

    assert policy["surface"]["version"] == 1
    assert policy["benchmark"]["runner"] == "pert-gym-worker-eu only"
    assert set(policy["benchmark"]["shapes"]) == {5_000, 10_000, 25_000}
    assert set(policy["dataset_family_policies"]) == {
        "xatlas_orion_hct116",
        "xatlas_orion_hek293t",
        "prism_perturbseq",
        "tcell_gwps",
        "temporal_and_spatial",
        "perturbai",
    }
