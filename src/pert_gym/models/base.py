"""Base interfaces for perturbation-response models."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

Vector = Sequence[float]
Matrix = Sequence[Vector]


@runtime_checkable
class PerturbationModel(Protocol):
    """Minimal estimator protocol used by the lightweight evaluation scaffold.

    Implementations intentionally operate on in-memory sequences here. Production
    loaders can adapt Lamin/AnnData batches to this contract without coupling the
    model package to LaminDB or deep-learning frameworks.
    """

    name: str

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "PerturbationModel":
        """Fit model state from expression/response vectors and metadata."""
        ...

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        """Predict one response vector per requested perturbation."""
        ...
