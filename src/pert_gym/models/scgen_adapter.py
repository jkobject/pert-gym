"""scGEN/scArches-style perturbation-transfer adapter for smoke benchmarks.

The upstream scGen package is maintained on PyPI, but it brings a broad Scanpy
runtime and its production API is AnnData-first.  This module provides the small
pert-gym adapter boundary used by the benchmark smoke: map an AnnData-like
control/condition contract onto the existing tiny conditional VAE implementation
without importing scgen/scanpy at package import time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .base import Matrix
from .conditional_vae import ConditionalPerturbationVAE


@dataclass
class ScgenPerturbationAdapter:
    """Small scGEN-style conditional VAE adapter for dependency/API smokes.

    The adapter mirrors the scGEN input semantics that matter for pert-gym:
    expression matrix ``X``, an obs condition key where one value is the control
    state and non-control values are perturbation conditions, optional covariate
    fields such as batch/cell type, and held-out perturbation identities at
    evaluation time.  It intentionally delegates the tiny train/predict smoke to
    :class:`ConditionalPerturbationVAE`; it is not a biological-performance claim
    for upstream scGen.
    """

    condition_key: str = "condition"
    control_value: str = "control"
    batch_key: str | None = None
    cell_type_key: str | None = None
    latent_dim: int = 2
    hidden_dim: int = 8
    condition_dim: int = 2
    epochs: int = 20
    lr: float = 3e-2
    beta_kl: float = 1e-3
    seed: int = 41
    name: str = "scgen_perturbation_adapter"

    _model: ConditionalPerturbationVAE | None = None
    data_contract_: dict[str, Any] | None = None

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "ScgenPerturbationAdapter":
        """Fit from the generic pert-gym benchmark arrays."""

        controls = list(_default_controls(perturbations, controls, self.control_value))
        self._fit_core(X=X, perturbations=perturbations, controls=controls)
        self.data_contract_ = {
            "input": "pert_gym BenchmarkBatch",
            "X": "small in-memory expression matrix",
            "condition_key": self.condition_key,
            "control_value": self.control_value,
            "batch_key": self.batch_key,
            "cell_type_key": self.cell_type_key,
            "held_out_semantics": "non-control perturbation identities are split across train/val/test; controls are available in each split",
        }
        return self

    def fit_anndata(self, adata: Any) -> "ScgenPerturbationAdapter":
        """Fit from an AnnData-like object using the scGEN-style obs contract.

        This method deliberately accepts ``Any`` and imports no AnnData symbols so
        the base package remains lightweight.  It is exercised in the isolated
        scgen env where ``anndata`` is installed.
        """

        if not hasattr(adata, "X") or not hasattr(adata, "obs"):
            raise TypeError("fit_anndata expects an AnnData-like object with X and obs.")
        if self.condition_key not in adata.obs:
            raise ValueError(f"AnnData.obs is missing condition_key={self.condition_key!r}.")
        conditions = [str(value) for value in adata.obs[self.condition_key].tolist()]
        controls = [value == self.control_value for value in conditions]
        X = _matrix_to_lists(adata.X)
        self._fit_core(X=X, perturbations=conditions, controls=controls)
        self.data_contract_ = {
            "input": "AnnData",
            "X": "adata.X expression matrix; smoke keeps it tiny/in-memory",
            "condition_key": self.condition_key,
            "control_value": self.control_value,
            "batch_key": self.batch_key,
            "cell_type_key": self.cell_type_key,
            "observed_conditions": sorted(set(conditions)),
            "n_obs": len(X),
            "n_vars": len(X[0]) if X else 0,
        }
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        """Predict expression rows for requested perturbation conditions."""

        if self._model is None:
            raise RuntimeError("Model must be fit before calling predict().")
        controls = list(_default_controls(perturbations, controls, self.control_value))
        return self._model.predict(perturbations=perturbations, controls=controls)

    @property
    def loss_(self) -> float | None:
        return None if self._model is None else self._model.loss_

    @property
    def reconstruction_loss_(self) -> float | None:
        return None if self._model is None else self._model.reconstruction_loss_

    @property
    def kl_loss_(self) -> float | None:
        return None if self._model is None else self._model.kl_loss_

    def _fit_core(
        self,
        *,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool],
    ) -> None:
        model = ConditionalPerturbationVAE(
            latent_dim=self.latent_dim,
            hidden_dim=self.hidden_dim,
            condition_dim=self.condition_dim,
            epochs=self.epochs,
            lr=self.lr,
            beta_kl=self.beta_kl,
            seed=self.seed,
            name=self.name,
        )
        model.fit(X=X, perturbations=perturbations, controls=controls)
        self._model = model


def _default_controls(
    perturbations: Sequence[str],
    controls: Sequence[bool] | None,
    control_value: str,
) -> Sequence[bool]:
    if controls is not None:
        return controls
    return [str(perturbation) == control_value for perturbation in perturbations]


def _matrix_to_lists(matrix: Any) -> list[list[float]]:
    if hasattr(matrix, "toarray"):
        matrix = matrix.toarray()
    if hasattr(matrix, "tolist"):
        values = matrix.tolist()
    else:
        values = matrix
    return [[float(value) for value in row] for row in values]
