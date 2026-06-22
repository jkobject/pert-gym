"""Minimal torch-backed Latent Perturbation Model baseline.

The class in this module intentionally keeps torch out of the base import path:
``torch`` is imported lazily only when fitting/predicting the LPM baseline. Use the
``lpm`` optional dependency group in an isolated environment for real execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, cast

from .base import Matrix
from .baselines import _default_controls, _n_features, _validate_training_inputs


@dataclass
class LatentPerturbationModel:
    """Tiny latent-delta perturbation baseline compatible with ``PerturbationModel``.

    The model trains a small autoencoder on expression vectors, computes a latent
    centroid for control rows, then stores per-perturbation latent shifts relative
    to that control centroid. Prediction decodes ``control_latent + delta`` for
    each requested perturbation. This is a smoke-testable LPM baseline, not a
    production recipe for biological training.
    """

    latent_dim: int = 4
    hidden_dim: int = 16
    epochs: int = 100
    lr: float = 1e-2
    seed: int = 0
    name: str = "latent_perturbation"

    n_features_: int | None = None
    loss_: float | None = None
    perturbation_deltas_: dict[str, list[float]] | None = None
    control_latent_: list[float] | None = None
    _model: Any | None = None

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "LatentPerturbationModel":
        """Fit the autoencoder and latent perturbation deltas on a small batch."""
        if self.epochs < 1:
            raise ValueError("epochs must be >= 1.")
        _validate_training_inputs(X, perturbations, controls)
        controls = _default_controls(perturbations, controls)
        if not any(controls):
            raise ValueError("LatentPerturbationModel requires at least one control row.")
        if not any(not is_control for is_control in controls):
            raise ValueError(
                "LatentPerturbationModel requires at least one perturbed row."
            )

        torch = _torch()
        self.n_features_ = _n_features(X)
        torch.manual_seed(self.seed)
        tensor = torch.tensor(X, dtype=torch.float32)
        model = cast(
            Any,
            _TinyAutoencoder(
                n_features=self.n_features_,
                hidden_dim=self.hidden_dim,
                latent_dim=self.latent_dim,
            ),
        )
        optimizer = torch.optim.Adam(model.parameters(), lr=self.lr)
        loss_fn = torch.nn.MSELoss()

        model.train()
        for _ in range(self.epochs):
            optimizer.zero_grad(set_to_none=True)
            reconstructed = model(tensor)
            loss = loss_fn(reconstructed, tensor)
            loss.backward()
            optimizer.step()
        self.loss_ = float(loss.detach().cpu().item())

        model.eval()
        with torch.no_grad():
            latents = model.encoder(tensor)
            control_mask = torch.tensor(list(controls), dtype=torch.bool)
            control_latent = latents[control_mask].mean(dim=0)
            self.control_latent_ = _tensor_to_list(control_latent)
            deltas: dict[str, list[float]] = {}
            for perturbation in sorted(set(perturbations)):
                rows = [
                    idx
                    for idx, value in enumerate(perturbations)
                    if value == perturbation and not controls[idx]
                ]
                if not rows:
                    continue
                row_index = torch.tensor(rows, dtype=torch.long)
                delta = latents[row_index].mean(dim=0) - control_latent
                deltas[perturbation] = _tensor_to_list(delta)
        self.perturbation_deltas_ = deltas
        self._model = model
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        """Decode one predicted expression vector per perturbation label."""
        self._require_fitted()
        torch = _torch()
        assert self._model is not None
        assert self.control_latent_ is not None
        assert self.perturbation_deltas_ is not None

        controls = _default_controls(perturbations, controls)
        latent_rows: list[list[float]] = []
        for perturbation, is_control in zip(perturbations, controls):
            if is_control:
                latent_rows.append(list(self.control_latent_))
                continue
            delta = self.perturbation_deltas_.get(
                perturbation, [0.0] * len(self.control_latent_)
            )
            latent_rows.append(
                [base + shift for base, shift in zip(self.control_latent_, delta)]
            )

        self._model.eval()
        with torch.no_grad():
            latent_tensor = torch.tensor(latent_rows, dtype=torch.float32)
            decoded = self._model.decoder(latent_tensor)
        return decoded.detach().cpu().tolist()

    def _require_fitted(self) -> None:
        if self.n_features_ is None or self._model is None:
            raise RuntimeError("Model must be fit before calling predict().")


class _TinyAutoencoder:
    """Runtime torch.nn.Module subclass created without importing torch at module import."""

    def __new__(cls, n_features: int, hidden_dim: int, latent_dim: int) -> Any:
        torch = _torch()

        class TinyAutoencoder(torch.nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.encoder = torch.nn.Sequential(
                    torch.nn.Linear(n_features, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, latent_dim),
                )
                self.decoder = torch.nn.Sequential(
                    torch.nn.Linear(latent_dim, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, n_features),
                )

            def forward(self, x: Any) -> Any:
                return self.decoder(self.encoder(x))

        return TinyAutoencoder()


def _tensor_to_list(tensor: Any) -> list[float]:
    return [float(value) for value in tensor.detach().cpu().tolist()]


def _torch() -> Any:
    try:
        import torch
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised without lpm env
        raise RuntimeError(
            "LatentPerturbationModel requires the optional lpm dependencies. "
            "Create the isolated env with: uv venv .venv-lpm --python 3.11 && "
            "uv pip install -p .venv-lpm/bin/python -e '.[lpm,dev]'"
        ) from exc
    return torch
