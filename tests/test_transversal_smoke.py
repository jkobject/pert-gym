from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "tools/run_transversal_smoke.py"


def _runner_module():
    spec = importlib.util.spec_from_file_location("transversal_smoke", RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sgd_step_has_finite_losses_and_nonzero_parameter_witness() -> None:
    runner = _runner_module()

    result = runner._mse_step(((1.0, 2.0), (3.0, 4.0)), (1.0, -1.0))

    assert result["updated"] is True
    assert result["loss_before"] >= 0.0
    assert result["loss_after"] >= 0.0
    assert result["parameter_delta_l2"] > 0.0


def test_immutable_report_write_has_readback_sha_and_refuses_overwrite(
    tmp_path,
) -> None:
    runner = _runner_module()
    out = tmp_path / "report.json"
    report = {"schema_version": "test.v1", "status": "passed"}

    sidecar = runner._write_immutable_json_report(out, report)

    expected_sha = hashlib.sha256(out.read_bytes()).hexdigest()
    assert sidecar == out.with_suffix(".json.sha256")
    assert sidecar.read_text() == f"{expected_sha}  {out.name}\n"
    assert json.loads(out.read_text()) == report
    with pytest.raises(FileExistsError, match="overwrite"):
        runner._write_immutable_json_report(out, report)
