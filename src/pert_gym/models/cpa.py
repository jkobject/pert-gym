"""Minimal standalone CPA-style perturbation autoencoder.

This module deliberately implements a tiny, torch-backed adapter instead of
binding to a heavyweight upstream CPA package. It gives pert-gym a stable smoke
boundary for compositional perturbation modeling while keeping deep-learning
imports lazy and isolated to the dedicated ``cpa`` model environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, cast

from .base import Matrix
from .baselines import _default_controls, _n_features, _validate_training_inputs


@dataclass
class CompositionalPerturbationAutoencoder:
    """Tiny CPA-style autoencoder compatible with ``PerturbationModel``.

    The model encodes expression vectors, learns one embedding per perturbation,
    and decodes ``latent + perturbation_embedding``. For control rows the
    perturbation embedding is zeroed during training/prediction. This is a
    CPU-smokeable API adapter, not a replacement for production CPA training.
    """

    latent_dim: int = 4
    hidden_dim: int = 16
    perturbation_dim: int = 4
    epochs: int = 80
    lr: float = 1e-2
    seed: int = 0
    name: str = "cpa_standalone"

    n_features_: int | None = None
    loss_: float | None = None
    perturbation_to_index_: dict[str, int] | None = None
    unknown_perturbation_index_: int | None = None
    _model: Any | None = None

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "CompositionalPerturbationAutoencoder":
        """Fit a tiny compositional perturbation autoencoder on a small batch."""
        if self.epochs < 1:
            raise ValueError("epochs must be >= 1.")
        if self.latent_dim < 1 or self.hidden_dim < 1 or self.perturbation_dim < 1:
            raise ValueError("latent_dim, hidden_dim, and perturbation_dim must be >= 1.")
        _validate_training_inputs(X, perturbations, controls)
        controls = _default_controls(perturbations, controls)
        if not any(controls):
            raise ValueError(
                "CompositionalPerturbationAutoencoder requires at least one control row."
            )
        if not any(not is_control for is_control in controls):
            raise ValueError(
                "CompositionalPerturbationAutoencoder requires at least one perturbed row."
            )

        torch = _torch()
        self.n_features_ = _n_features(X)
        torch.manual_seed(self.seed)

        perturbation_names = sorted(
            {perturbation for perturbation, is_control in zip(perturbations, controls) if not is_control}
        )
        self.perturbation_to_index_ = {
            perturbation: idx for idx, perturbation in enumerate(perturbation_names)
        }
        self.unknown_perturbation_index_ = len(self.perturbation_to_index_)
        model = cast(
            Any,
            _TinyCPA(
                n_features=self.n_features_,
                n_perturbations=len(self.perturbation_to_index_) + 1,
                unknown_perturbation_index=self.unknown_perturbation_index_,
                hidden_dim=self.hidden_dim,
                latent_dim=self.latent_dim,
                perturbation_dim=self.perturbation_dim,
            ),
        )
        optimizer = torch.optim.Adam(model.parameters(), lr=self.lr)
        loss_fn = torch.nn.MSELoss()

        expression = torch.tensor(X, dtype=torch.float32)
        perturbation_index = torch.tensor(
            [
                self.perturbation_to_index_.get(
                    perturbation,
                    self.unknown_perturbation_index_,
                )
                for perturbation in perturbations
            ],
            dtype=torch.long,
        )
        control_mask = torch.tensor(list(controls), dtype=torch.bool)

        final_loss: Any | None = None
        model.train()
        for _ in range(self.epochs):
            optimizer.zero_grad(set_to_none=True)
            reconstructed = model(expression, perturbation_index, control_mask)
            current_loss = loss_fn(reconstructed, expression)
            current_loss.backward()
            optimizer.step()
            final_loss = current_loss

        assert final_loss is not None
        self.loss_ = float(final_loss.detach().cpu().item())
        self._model = model
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        """Decode one expression vector per requested perturbation."""
        self._require_fitted()
        torch = _torch()
        assert self._model is not None
        assert self.perturbation_to_index_ is not None
        assert self.unknown_perturbation_index_ is not None

        controls = _default_controls(perturbations, controls)
        perturbation_index = torch.tensor(
            [
                self.perturbation_to_index_.get(
                    perturbation,
                    self.unknown_perturbation_index_,
                )
                for perturbation in perturbations
            ],
            dtype=torch.long,
        )
        control_mask = torch.tensor(list(controls), dtype=torch.bool)

        self._model.eval()
        with torch.no_grad():
            decoded = self._model.decode_from_control(
                perturbation_index=perturbation_index,
                control_mask=control_mask,
            )
        return decoded.detach().cpu().tolist()

    def _require_fitted(self) -> None:
        if self.n_features_ is None or self._model is None:
            raise RuntimeError("Model must be fit before calling predict().")


class _TinyCPA:
    """Runtime torch.nn.Module subclass created without importing torch globally."""

    def __new__(
        cls,
        *,
        n_features: int,
        n_perturbations: int,
        unknown_perturbation_index: int,
        hidden_dim: int,
        latent_dim: int,
        perturbation_dim: int,
    ) -> Any:
        torch = _torch()

        class TinyCPA(torch.nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.encoder = torch.nn.Sequential(
                    torch.nn.Linear(n_features, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, latent_dim),
                )
                self.perturbation_embedding = torch.nn.Embedding(
                    n_perturbations,
                    perturbation_dim,
                )
                with torch.no_grad():
                    self.perturbation_embedding.weight[unknown_perturbation_index].zero_()
                self.decoder = torch.nn.Sequential(
                    torch.nn.Linear(latent_dim + perturbation_dim, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, n_features),
                )
                self.register_buffer("control_latent", torch.zeros(latent_dim))

            def forward(
                self,
                expression: Any,
                perturbation_index: Any,
                control_mask: Any,
            ) -> Any:
                latent = self.encoder(expression)
                if bool(control_mask.any()):
                    self.control_latent.copy_(latent[control_mask].mean(dim=0).detach())
                perturbation_effect = self.perturbation_embedding(perturbation_index)
                perturbation_effect = perturbation_effect.masked_fill(
                    control_mask.unsqueeze(1),
                    0.0,
                )
                return self.decoder(torch.cat([latent, perturbation_effect], dim=1))

            def decode_from_control(
                self,
                *,
                perturbation_index: Any,
                control_mask: Any,
            ) -> Any:
                latent = self.control_latent.repeat(len(perturbation_index), 1)
                perturbation_effect = self.perturbation_embedding(perturbation_index)
                perturbation_effect = perturbation_effect.masked_fill(
                    control_mask.unsqueeze(1),
                    0.0,
                )
                return self.decoder(torch.cat([latent, perturbation_effect], dim=1))

        return TinyCPA()


def _torch() -> Any:
    try:
        import torch
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised without cpa env
        raise RuntimeError(
            "CompositionalPerturbationAutoencoder requires the isolated CPA "
            "dependencies. Create the env with: uv run python tools/model_env.py "
            "create cpa"
        ) from exc
    return torch
