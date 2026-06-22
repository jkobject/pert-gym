# Maintained trVAE replacement smoke

Legacy upstream `trVAE==1.1.2` remains documented as blocked for this repo: it pins `tensorflow==1.15.2` and legacy Keras, which are incompatible with the current Mac/Python policy. The replacement benchmark path is therefore not the old package and not the lowercase `trvae` fallback.

## Decision — 2026-06-22

Use `pert_gym.models.ConditionalPerturbationVAE` as the pragmatic trVAE-like analogue for the first smoke benchmark.

Why this choice:

1. It preserves the relevant boundary: conditional latent transfer from a learned control latent to requested perturbation conditions.
2. It implements the existing `PerturbationModel` protocol directly: `fit(X, perturbations, controls)` and `predict(perturbations, controls)`.
3. It uses maintained, installable dependencies (`torch>=2.3`, `anndata>=0.10`) in `.venv-models/trvae-replacement` on Python 3.11.
4. It avoids pulling the full scanpy/scvi stack into the first smoke and avoids silently installing TensorFlow 1.x, old Keras, or the obsolete package route.

## Alternatives surveyed

- scGen / scArches-style conditional VAE: conceptually close, but it brings an AnnData-first scanpy/scvi adapter surface. The latest `scvi-tools` PyPI metadata currently reports Python `>=3.12`, while the project model policy is Python 3.11 unless explicitly changed.
- scVI-tools TOTALVI/SCANVI-style covariate models: maintained, but not a direct trVAE perturbation-transfer replacement and too heavy for the first smoke boundary.
- CPA-like VAE: already represented by the standalone CPA adapter; useful as its own benchmark family, but collapsing it into trVAE would hide a distinct modeling assumption.
- Small in-repo conditional VAE: chosen for this bounded replacement because it is smokeable, reviewable, and matches the current pert-gym API.

## API shape

```python
from pert_gym.models import ConditionalPerturbationVAE

model = ConditionalPerturbationVAE(
    latent_dim=2,
    hidden_dim=8,
    condition_dim=2,
    epochs=25,
)
model.fit(X=train.X, perturbations=train.perturbations, controls=train.controls)
pred = model.predict(perturbations=test.perturbations, controls=test.controls)
```

The smoke model:

1. reserves condition index `0` for control;
2. embeds non-control perturbation identities;
3. encodes expression plus condition into a latent mean/log-variance;
4. trains a small VAE reconstruction + beta-KL objective;
5. stores the average control latent mean;
6. decodes that control latent under requested perturbation-condition embeddings.

Unseen perturbation identities currently fall back to the decoded control condition. That is deliberately conservative for the synthetic held-out-perturbation smoke; a production implementation should choose a real unseen-perturbation strategy before making biological claims.

## Commands

```bash
uv run python tools/model_env.py create trvae-replacement
uv run python tools/model_env.py smoke trvae-replacement
.venv-models/trvae-replacement/bin/python tools/run_trvae_replacement_benchmark.py --date 20260622
```

Latest artifact:

```text
artifacts/model_benchmarks/trvae_replacement_20260622.json
artifacts/model_benchmarks/trvae_replacement_20260622.md
```

## Safety and limitations

- No Lamin writes.
- No huge matrix loads.
- No TensorFlow 1.x / legacy Keras / lowercase `trvae` install attempts.
- Current benchmark uses the model-ready-v0 metadata path with deterministic synthetic fallback; metrics are only environment/API smoke evidence, not biological model performance.
- No batching, validation early stopping, dose/time handling, covariate adversary, calibrated uncertainty, or production AnnData adapter yet.
