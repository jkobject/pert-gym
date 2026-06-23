#!/usr/bin/env python3
"""Smoke the real DRUG-seq chemCPA-ready tiny loader.

This is intentionally read-only and tiny: it imports the chemCPA environment,
loads the local DRUG-seq export, verifies expression/fingerprint dimensions, and
runs the existing mean-control evaluation scaffold. It does not train chemCPA.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pert_gym.benchmarks import load_chemcpa_drugseq_tiny  # noqa: E402
from pert_gym.evaluate import evaluate_model  # noqa: E402
from pert_gym.models import MeanControlBaseline  # noqa: E402


def main() -> int:
    if importlib.util.find_spec("chemCPA") is None and importlib.util.find_spec("chemcpa") is None:
        raise RuntimeError("Neither chemCPA nor chemcpa import spec is available")

    artifact = Path("artifacts/model_benchmarks/chemcpa_drugseq_tiny_20260622.json")
    dataset = load_chemcpa_drugseq_tiny(artifact_path=artifact)
    if dataset.train.compound_features is None:
        raise RuntimeError("missing train compound fingerprints")
    if len(dataset.train.compound_features[0]) != 256:
        raise RuntimeError("unexpected fingerprint width")
    if len(dataset.train.X[0]) != 128:
        raise RuntimeError("unexpected expression feature width")

    result = evaluate_model(MeanControlBaseline(), train=dataset.train, test=dataset.test)
    print("chemCPA DRUG-seq tiny smoke passed")
    print(f"source={dataset.source}")
    print(f"train_n_obs={len(dataset.train.X)} val_n_obs={len(dataset.val.X)} test_n_obs={len(dataset.test.X)}")
    print(f"expression_features={len(dataset.train.X[0])}")
    print(f"fingerprint_bits={len(dataset.train.compound_features[0])}")
    print(f"test_mae={result.metrics['mae']:.6f}")
    print(f"test_rmse={result.metrics['rmse']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
