"""Tiny conditional VAE replacement for legacy trVAE-style smokes.

The original trVAE package is blocked by TensorFlow 1.x dependency pins on the
current Mac/Python policy. This module provides a maintained-dependency analogue:
a small torch-backed conditional variational autoencoder that shares pert-gym's
``PerturbationModel`` fit/predict boundary and can be exercised in an isolated
model env without importing torch in the base runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, cast

from .base import Matrix
from .baselines import _default_controls, _n_features, _validate_training_inputs


@dataclass
class ConditionalPerturbationVAE:
    """Small trVAE-like conditional VAE compatible with ``PerturbationModel``.

    The smoke model encodes expression together with a perturbation-condition
    embedding, trains a VAE reconstruction objective, stores the average control
    latent mean, and decodes requested perturbation conditions from that control
    latent. It is intentionally tiny and CPU-friendly; it is a dependency and API
    replacement for the blocked trVAE smoke path, not a validated production
    implementation of the original trVAE paper.
    """

    latent_dim: int = 4
    hidden_dim: int = 16
    condition_dim: int = 4
    epochs: int = 80
    lr: float = 1e-2
    beta_kl: float = 1e-3
    seed: int = 0
    name: str = "conditional_perturbation_vae"

    n_features_: int | None = None
    loss_: float | None = None
    reconstruction_loss_: float | None = None
    kl_loss_: float | None = None
    perturbation_to_index_: dict[str, int] | None = None
    control_latent_: list[float] | None = None
    _model: Any | None = None

    def fit(
        self,
        X: Matrix,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> "ConditionalPerturbationVAE":
        """Fit the tiny conditional VAE on a small in-memory batch."""
        if self.epochs < 1:
            raise ValueError("epochs must be >= 1.")
        if self.latent_dim < 1 or self.hidden_dim < 1 or self.condition_dim < 1:
            raise ValueError("latent_dim, hidden_dim, and condition_dim must be >= 1.")
        if self.beta_kl < 0:
            raise ValueError("beta_kl must be non-negative.")
        _validate_training_inputs(X, perturbations, controls)
        controls = _default_controls(perturbations, controls)
        if not any(controls):
            raise ValueError("ConditionalPerturbationVAE requires at least one control row.")
        if not any(not is_control for is_control in controls):
            raise ValueError(
                "ConditionalPerturbationVAE requires at least one perturbed row."
            )

        torch = _torch()
        self.n_features_ = _n_features(X)
        torch.manual_seed(self.seed)

        perturbation_names = sorted(
            {perturbation for perturbation, is_control in zip(perturbations, controls) if not is_control}
        )
        # index 0 is reserved for the control condition. Non-control perturbations
        # start at 1 so unseen requested perturbations can safely fall back to the
        # decoded control condition rather than pretending a learned effect exists.
        self.perturbation_to_index_ = {
            perturbation: idx + 1 for idx, perturbation in enumerate(perturbation_names)
        }
        model = cast(
            Any,
            _TinyConditionalVAE(
                n_features=self.n_features_,
                n_conditions=len(self.perturbation_to_index_) + 1,
                hidden_dim=self.hidden_dim,
                latent_dim=self.latent_dim,
                condition_dim=self.condition_dim,
            ),
        )
        optimizer = torch.optim.Adam(model.parameters(), lr=self.lr)
        expression = torch.tensor(X, dtype=torch.float32)
        condition_index = torch.tensor(
            [0 if is_control else self.perturbation_to_index_[perturbation] for perturbation, is_control in zip(perturbations, controls)],
            dtype=torch.long,
        )
        control_mask = torch.tensor(list(controls), dtype=torch.bool)

        final_loss: Any | None = None
        final_reconstruction: Any | None = None
        final_kl: Any | None = None
        model.train()
        for _ in range(self.epochs):
            optimizer.zero_grad(set_to_none=True)
            reconstructed, mu, logvar = model(expression, condition_index)
            reconstruction_loss = torch.nn.functional.mse_loss(reconstructed, expression)
            kl_loss = -0.5 * torch.mean(1 + logvar - mu.pow(2) - logvar.exp())
            loss = reconstruction_loss + self.beta_kl * kl_loss
            loss.backward()
            optimizer.step()
            final_loss = loss
            final_reconstruction = reconstruction_loss
            final_kl = kl_loss

        assert final_loss is not None
        assert final_reconstruction is not None
        assert final_kl is not None
        model.eval()
        with torch.no_grad():
            mu, _ = model.encode(expression, condition_index)
            control_latent = mu[control_mask].mean(dim=0)
        self.control_latent_ = _tensor_to_list(control_latent)
        self.loss_ = float(final_loss.detach().cpu().item())
        self.reconstruction_loss_ = float(final_reconstruction.detach().cpu().item())
        self.kl_loss_ = float(final_kl.detach().cpu().item())
        self._model = model
        return self

    def predict(
        self,
        perturbations: Sequence[str],
        controls: Sequence[bool] | None = None,
    ) -> list[list[float]]:
        """Decode one expression vector per requested perturbation condition."""
        self._require_fitted()
        torch = _torch()
        assert self._model is not None
        assert self.control_latent_ is not None
        assert self.perturbation_to_index_ is not None

        controls = _default_controls(perturbations, controls)
        condition_index = torch.tensor(
            [0 if is_control else self.perturbation_to_index_.get(perturbation, 0) for perturbation, is_control in zip(perturbations, controls)],
            dtype=torch.long,
        )
        latent = torch.tensor([self.control_latent_] * len(condition_index), dtype=torch.float32)
        self._model.eval()
        with torch.no_grad():
            decoded = self._model.decode(latent, condition_index)
        return decoded.detach().cpu().tolist()

    def _require_fitted(self) -> None:
        if self.n_features_ is None or self._model is None:
            raise RuntimeError("Model must be fit before calling predict().")


class _TinyConditionalVAE:
    """Runtime torch.nn.Module subclass created without importing torch globally."""

    def __new__(
        cls,
        *,
        n_features: int,
        n_conditions: int,
        hidden_dim: int,
        latent_dim: int,
        condition_dim: int,
    ) -> Any:
        torch = _torch()

        class TinyConditionalVAE(torch.nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.condition_embedding = torch.nn.Embedding(n_conditions, condition_dim)
                encoder_input_dim = n_features + condition_dim
                decoder_input_dim = latent_dim + condition_dim
                self.encoder = torch.nn.Sequential(
                    torch.nn.Linear(encoder_input_dim, hidden_dim),
                    torch.nn.ReLU(),
                )
                self.mu = torch.nn.Linear(hidden_dim, latent_dim)
                self.logvar = torch.nn.Linear(hidden_dim, latent_dim)
                self.decoder = torch.nn.Sequential(
                    torch.nn.Linear(decoder_input_dim, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, n_features),
                )

            def encode(self, expression: Any, condition_index: Any) -> tuple[Any, Any]:
                condition = self.condition_embedding(condition_index)
                hidden = self.encoder(torch.cat([expression, condition], dim=1))
                return self.mu(hidden), self.logvar(hidden)

            def reparameterize(self, mu: Any, logvar: Any) -> Any:
                std = torch.exp(0.5 * logvar)
                return mu + torch.randn_like(std) * std

            def decode(self, latent: Any, condition_index: Any) -> Any:
                condition = self.condition_embedding(condition_index)
                return self.decoder(torch.cat([latent, condition], dim=1))

            def forward(self, expression: Any, condition_index: Any) -> tuple[Any, Any, Any]:
                mu, logvar = self.encode(expression, condition_index)
                latent = self.reparameterize(mu, logvar)
                return self.decode(latent, condition_index), mu, logvar

        return TinyConditionalVAE()


def _tensor_to_list(tensor: Any) -> list[float]:
    return [float(value) for value in tensor.detach().cpu().tolist()]


def _torch() -> Any:
    try:
        import torch
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised without model env
        raise RuntimeError(
            "ConditionalPerturbationVAE requires the isolated trvae-replacement "
            "dependencies. Create the env with: uv run python tools/model_env.py "
            "create trvae-replacement"
        ) from exc
    return torch
