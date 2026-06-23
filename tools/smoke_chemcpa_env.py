#!/usr/bin/env python3
"""Read-only chemCPA environment smoke.

This intentionally avoids Lamin writes and heavy training. It verifies that the
isolated chemCPA env can import the local package, torch/scanpy/anndata, RDKit,
and the upstream chemCPA package, then builds a tiny compound fingerprint.
"""

from __future__ import annotations

import importlib.util

import numpy as np
import torch
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator

from pert_gym.evaluate import EvaluationBatch, evaluate_model
from pert_gym.models import MeanControlBaseline


def main() -> int:
    import anndata as ad
    import scanpy as sc

    if importlib.util.find_spec("chemCPA") is None and importlib.util.find_spec("chemcpa") is None:
        raise RuntimeError("Neither chemCPA nor chemcpa import spec is available")

    mol = Chem.MolFromSmiles("CCO")
    if mol is None:
        raise RuntimeError("RDKit failed to parse ethanol SMILES")
    generator = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=128)
    fp = np.asarray(generator.GetFingerprint(mol), dtype=np.int8)
    if fp.shape != (128,) or int(fp.sum()) <= 0:
        raise RuntimeError(f"Unexpected fingerprint shape/sum: {fp.shape}, {fp.sum()}")

    batch = EvaluationBatch(
        X=torch.tensor([[1.0, 2.0], [2.0, 4.0], [5.0, 5.0]], dtype=torch.float32).numpy().tolist(),
        perturbations=["control", "control", "BRD-ETHANOL"],
        controls=[True, True, False],
    )
    result = evaluate_model(MeanControlBaseline(), train=batch, test=batch)
    assert result.n_obs == 3

    tiny = ad.AnnData(X=np.asarray(batch.X, dtype=np.float32))
    tiny.obs["perturbation"] = batch.perturbations
    tiny.obs["is_control"] = batch.controls

    print("chemCPA env smoke passed")
    print(f"torch={torch.__version__}")
    print(f"anndata={ad.__version__} scanpy={sc.__version__}")
    print(f"rdkit_fingerprint_bits={int(fp.sum())}/128")
    print("chemCPA_import=available")
    print(f"baseline_mae={result.metrics['mae']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
