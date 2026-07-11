import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("scipy")
pytest.importorskip("zarr")

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "benchmark_sparse_zarr", ROOT / "tools/benchmark_sparse_zarr.py"
)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_case_emits_local_resources_and_matrix_obs_source_parity(
    tmp_path: Path,
) -> None:
    matrix = benchmark.matrix_for(8, "csr")

    result = benchmark.write_and_readback(
        tmp_path / "case", matrix, "csr", source_row_start=100
    )

    assert result["wall_seconds"] >= 0
    assert result["local_rss_bytes"] > 0
    assert result["bytes"] > 0
    assert result["matrix_parity"] is True
    assert result["obs_parity"] is True
    assert result["source_row_parity"] is True
