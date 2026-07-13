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
SHA256_A = "a" * 64
SHA256_B = "B" * 64


def manifest() -> dict:
    return {
        "format": SURFACE_FORMAT,
        "version": 1,
        "shape": [10, 4],
        "nnz": 12,
        "sparse_format": "csr",
        "chunks": [
            {
                "key": "chunks/0000.zarr",
                "start": 0,
                "end": 5,
                "nnz": 6,
                "shape": [5, 4],
                "dtype": "float32",
                "checksums": {
                    "data_sha256": SHA256_A,
                    "indices_sha256": SHA256_A,
                    "indptr_sha256": SHA256_A,
                },
                "obs": {
                    "key": "obs/0000.parquet",
                    "provenance": {
                        "source_uri": "gs://example/source.h5ad",
                        "source_checksum": f"sha256-file-bytes/v1:{SHA256_A}",
                        "source_row_start": 0,
                        "source_row_end": 5,
                        "ingestion_run_id": "run-0000",
                        "writer_version": "1.0.0",
                    },
                },
            },
            {
                "key": "chunks/0001.zarr",
                "start": 5,
                "end": 10,
                "nnz": 6,
                "shape": [5, 4],
                "dtype": "float32",
                "checksums": {
                    "data_sha256": SHA256_B,
                    "indices_sha256": SHA256_B,
                    "indptr_sha256": SHA256_B,
                },
                "obs": {
                    "key": "obs/0001.parquet",
                    "provenance": {
                        "source_uri": "gs://example/source.h5ad",
                        "source_checksum": f"sha256-file-bytes/v1:{SHA256_B}",
                        "source_row_start": 5,
                        "source_row_end": 10,
                        "ingestion_run_id": "run-0001",
                        "writer_version": "1.0.0",
                    },
                },
            },
        ],
        "shared_var": {
            "key": "vars/abc/var.parquet",
            "index_sha256": SHA256_A,
            "frame_sha256": SHA256_B,
            "schema_fingerprint": "schema-v1",
        },
    }


def test_v1_manifest_requires_exact_chunk_denominator_and_nnz_parity() -> None:
    surface = validate_logical_sparse_surface(manifest())
    assert surface.shape == (10, 4)
    assert [(chunk.start, chunk.end) for chunk in surface.chunks] == [(0, 5), (5, 10)]

    invalid = manifest()
    invalid["chunks"][1]["start"] = 6
    invalid["chunks"][1]["shape"] = [4, 4]
    invalid["chunks"][1]["obs"]["provenance"]["source_row_start"] = 6
    with pytest.raises(ValueError, match="contiguous"):
        validate_logical_sparse_surface(invalid)

    invalid = manifest()
    invalid["chunks"][1]["nnz"] = 5
    with pytest.raises(ValueError, match="nnz sum"):
        validate_logical_sparse_surface(invalid)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["shared_var"].pop("schema_fingerprint"),
            "schema_fingerprint",
        ),
        (lambda value: value["chunks"][0].pop("shape"), "shape"),
        (lambda value: value["chunks"][0].pop("dtype"), "dtype"),
        (lambda value: value["chunks"][0].pop("checksums"), "checksums"),
        (lambda value: value["chunks"][0]["obs"].update({"key": ""}), "obs.key"),
        (
            lambda value: value["chunks"][0]["obs"]["provenance"].pop("source_uri"),
            "source_uri",
        ),
    ],
)
def test_v1_manifest_rejects_missing_chunk_integrity_and_obs_provenance(
    mutate, message: str
) -> None:
    invalid = manifest()
    mutate(invalid)
    with pytest.raises(ValueError, match=message):
        validate_logical_sparse_surface(invalid)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["chunks"][1].update({"key": "chunks/0000.zarr"}),
            "key must be unique",
        ),
        (
            lambda value: value["chunks"][1]["obs"].update({"key": "obs/0000.parquet"}),
            "obs.key must be unique",
        ),
        (
            lambda value: value["chunks"][0]["checksums"].update(
                {"data_sha256": "not-a-sha256"}
            ),
            "64-hex",
        ),
        (
            lambda value: value["chunks"][0]["checksums"].update(
                {"payload_sha256": "not-a-sha256"}
            ),
            "64-hex",
        ),
        (lambda value: value["shared_var"].update({"index_sha256": "short"}), "64-hex"),
        (
            lambda value: value["shared_var"].update(
                {"metadata_sha256": "not-a-sha256"}
            ),
            "64-hex",
        ),
        (
            lambda value: value["chunks"][0]["obs"]["provenance"].update(
                {"source_checksum": SHA256_A}
            ),
            "sha256-file-bytes/v1",
        ),
        (
            lambda value: value["chunks"][0]["obs"]["provenance"].update(
                {"source_checksum": "sha256-file-bytes/v1:not-a-sha256"}
            ),
            "64-hex",
        ),
    ],
)
def test_v1_manifest_rejects_duplicate_object_identities_and_invalid_checksums(
    mutate, message: str
) -> None:
    invalid = manifest()
    mutate(invalid)
    with pytest.raises(ValueError, match=message):
        validate_logical_sparse_surface(invalid)


def test_v1_manifest_accepts_uppercase_sha256_digests() -> None:
    surface = validate_logical_sparse_surface(manifest())
    assert surface.shared_var["frame_sha256"] == SHA256_B


def test_v1_manifest_rejects_chunk_shape_and_source_row_mismatches() -> None:
    invalid = manifest()
    invalid["chunks"][0]["shape"] = [4, 4]
    with pytest.raises(ValueError, match="shape"):
        validate_logical_sparse_surface(invalid)

    invalid = manifest()
    invalid["chunks"][0]["obs"]["provenance"]["source_row_end"] = 4
    with pytest.raises(ValueError, match="source row"):
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
            "version": 1,
            "n_obs": 3,
            "n_vars": 2,
            "nnz": 4,
            "sparse_format": "csr",
            "x_key": "old/X.h5ad",
            "obs_key": "old/obs.parquet",
            "var_key": "old/var.parquet",
        }
    )
    assert surface.format == LEGACY_FORMAT
    assert [(chunk.start, chunk.end, chunk.nnz) for chunk in surface.chunks] == [
        (0, 3, 4)
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [("version", 2, "version"), ("sparse_format", "coo", "sparse_format")],
)
def test_legacy_loader_fails_closed_for_unknown_versions_and_formats(
    field: str, value: object, message: str
) -> None:
    legacy = {
        "format": LEGACY_FORMAT,
        "version": 1,
        "n_obs": 3,
        "n_vars": 2,
        "nnz": 4,
        "sparse_format": "csr",
        "x_key": "old/X.h5ad",
        "obs_key": "old/obs.parquet",
        "var_key": "old/var.parquet",
    }
    legacy[field] = value
    with pytest.raises(ValueError, match=message):
        load_compatible_surface(legacy)


def test_machine_readable_policy_covers_required_families_and_vm_benchmark() -> None:
    policy = json.loads(
        (ROOT / "config/logical_sparse_zarr_policy.v1.json").read_text()
    )
    assert policy["surface"]["version"] == 1
    assert policy["benchmark"]["runner"] == "pert-gym-worker-eu only"
    assert set(policy["benchmark"]["shapes"]) == {5_000, 10_000, 25_000}
    assert set(policy["benchmark"]["required_metrics"]) == {
        "case_rss_baseline_bytes",
        "case_rss_peak_bytes",
        "case_rss_peak_delta_bytes",
        "case_rss_peak_measurement",
        "wall_seconds",
        "bytes",
        "matrix_parity",
        "obs_parity",
        "source_row_parity",
    }
    assert set(policy["dataset_family_policies"]) == {
        "xatlas_orion_hct116",
        "xatlas_orion_hek293t",
        "prism_perturbseq",
        "tcell_gwps",
        "temporal_and_spatial",
        "perturbai",
    }
    assert policy["production_block_policy"] == {
        "minimum_bytes": 2 * 1024**3,
        "target_bytes": 5 * 1024**3 // 2,
        "maximum_bytes": 3 * 1024**3,
        "exceptions": ["one_final_tail", "genuinely_smaller_dataset"],
        "measurement": "post-write compressed matrix object bytes summed over contiguous physical chunks",
        "manifest_enforcement": "immutable plan pins the policy; every chunk records compressed_bytes; manifest records and readback recomputes logical block groupings",
        "var_reference": "every block resolves the manifest's one full-hash shared_var",
    }
