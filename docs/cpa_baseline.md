# Standalone CPA smoke adapter

`CompositionalPerturbationAutoencoder` is a minimal torch-backed CPA-style
adapter for pert-gym's lightweight `PerturbationModel` protocol.

Route decision for M5-Mac (2026-06-22): use the standalone adapter for the first
CPU smoke. The `scvi-tools` route is deferred because the project needs a stable,
direct CPA-like boundary for `fit(X, perturbations, controls)` / `predict(...)`,
and pulling the full scvi/scanpy stack just for an import smoke would not prove a
CPA implementation. The isolated CPA env therefore installs only:

- `torch>=2.3` (resolved version recorded by the smoke run)
- `anndata>=0.10` (for future read-only AnnData/model-ready adapters)

## API boundary

```python
from pert_gym.models import CompositionalPerturbationAutoencoder

model = CompositionalPerturbationAutoencoder(
    latent_dim=2,
    hidden_dim=8,
    perturbation_dim=2,
    epochs=20,
)
model.fit(X=train.X, perturbations=train.perturbations, controls=train.controls)
pred = model.predict(perturbations=test.perturbations, controls=test.controls)
```

The current model:

1. encodes expression vectors with a tiny MLP;
2. learns one embedding per perturbation identity;
3. decodes `latent + perturbation_embedding`;
4. masks perturbation embeddings for control rows;
5. predicts from the learned control latent centroid plus requested perturbation
   embeddings.

## Smoke command

```bash
uv run python tools/model_env.py create cpa
uv run python tools/model_env.py smoke cpa
```

The smoke reads the model-ready-v0 manifest metadata and uses the deterministic
synthetic fallback from `load_model_ready_v0_or_synthetic`; it does not load heavy
Lamin matrices or write Lamin artifacts.

## Limitations / blockers

- This is a smoke adapter, not a biologically validated CPA recipe.
- No covariate adversary, dose composition, drug combination semantics,
  held-out cell-context generalization, early stopping, batching, checkpoints, or
  calibrated uncertainty yet.
- A production CPA/scvi integration still needs a package/API selection gate and
  a bounded AnnData loader that maps obs fields to perturbation/control labels.
- Model outputs must stay local under `artifacts/model-runs/cpa/`; model env code
  must not call Lamin artifact mutation APIs.
