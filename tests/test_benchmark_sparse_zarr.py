import importlib.util
import json
import sys
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


def test_case_measures_generation_write_readback_and_case_local_rss(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    samples = iter((100, 140, 180))
    monkeypatch.setattr(benchmark, "local_rss_bytes", lambda: next(samples))
    monkeypatch.setattr(
        benchmark,
        "write_and_readback",
        lambda *_args, **_kwargs: {
            "write_readback_seconds": 0.1,
            "bytes": 1,
            "matrix_parity": True,
            "obs_parity": True,
            "source_row_parity": True,
        },
    )

    result = benchmark.run_case(tmp_path / "case", 8, "csr")

    assert result["n_obs"] == 8
    assert result["format"] == "csr"
    assert result["case_rss_baseline_bytes"] == 100
    assert result["case_rss_peak_bytes"] == 180
    assert result["case_rss_peak_delta_bytes"] == 80
    assert result["wall_seconds"] >= 0


def test_main_emits_exactly_six_required_cases_and_top_level_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "benchmark.json"
    observed: list[tuple[int, str]] = []

    monkeypatch.setattr(benchmark, "host_name", lambda: "pert-gym-worker-eu-test")

    def fake_run_case(_root: Path, n_obs: int, sparse_format: str) -> dict[str, object]:
        observed.append((n_obs, sparse_format))
        return {
            "n_obs": n_obs,
            "n_vars": 2_000,
            "nnz": n_obs,
            "format": sparse_format,
            "wall_seconds": 0.1,
            "case_rss_baseline_bytes": 10,
            "case_rss_peak_bytes": 20,
            "case_rss_peak_delta_bytes": 10,
            "bytes": 30,
            "matrix_parity": True,
            "obs_parity": True,
            "source_row_parity": True,
        }

    monkeypatch.setattr(benchmark, "run_case", fake_run_case)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_sparse_zarr.py",
            "--output",
            str(output),
            "--workdir",
            str(tmp_path / "work"),
        ],
    )

    assert benchmark.main() == 0
    payload = json.loads(output.read_text())
    assert payload["host"] == "pert-gym-worker-eu-test"
    assert payload["total_wall_seconds"] >= 0
    assert observed == [
        (5_000, "csr"),
        (5_000, "csc"),
        (10_000, "csr"),
        (10_000, "csc"),
        (25_000, "csr"),
        (25_000, "csc"),
    ]
    assert len(payload["cases"]) == 6
    for case in payload["cases"]:
        assert set(case) >= {
            "n_obs",
            "n_vars",
            "nnz",
            "format",
            "wall_seconds",
            "case_rss_baseline_bytes",
            "case_rss_peak_bytes",
            "case_rss_peak_delta_bytes",
            "bytes",
            "matrix_parity",
            "obs_parity",
            "source_row_parity",
        }
        assert case["matrix_parity"] is True
        assert case["obs_parity"] is True
        assert case["source_row_parity"] is True
