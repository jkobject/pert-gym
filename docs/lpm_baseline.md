# LPM baseline smoke

This project keeps deep-learning dependencies out of the default development
environment. The Latent Perturbation Model (LPM) baseline lives behind the
`lpm` optional dependency group and should be run in a separate environment.

## Isolated environment

```bash
uv venv .venv-lpm --python 3.11
uv pip install -p .venv-lpm/bin/python -e '.[lpm,dev]'
```

This installs torch only into `.venv-lpm`; do not add torch to the core project
dependencies or rely on the default `.venv` for LPM runs.

## Smoke tests

```bash
.venv-lpm/bin/python -m pytest tests/test_lpm_baseline.py -q
.venv-lpm/bin/python -m pytest tests/test_metrics.py tests/test_smoke.py tests/test_models_and_evaluate.py tests/test_lpm_baseline.py -q
```

The current smoke uses tiny synthetic in-memory batches and CPU only. It does
not connect to LaminDB and performs no artifact writes.

## Model shape

`pert_gym.models.LatentPerturbationModel` implements the existing
`PerturbationModel` scaffold:

1. train a tiny autoencoder on expression vectors;
2. encode control and perturbed rows;
3. store per-perturbation latent deltas relative to the control latent centroid;
4. decode `control_latent + delta` for requested perturbation labels.

Unseen perturbations fall back to the decoded control latent. This is deliberate
for a smoke baseline: it avoids hallucinating unseen biology while preserving the
same prediction shape as the mean baselines.

## Limitations and next training requirements

- Synthetic smoke only; not a scientifically validated perturbation model.
- No AnnData/Lamin loader yet; model-ready triplets still need a bounded loader
  adapter that maps obs perturbation/control fields to `EvaluationBatch`.
- No train/validation split, early stopping, scaling, batching, covariates,
  cell-type stratification, dose/time handling, or uncertainty estimates.
- For real model-ready-v0 training, add a read-only loader smoke first, keep CPU
  payload tiny, and record the exact collection member/prefix used. Do not write
  Lamin artifacts from LPM smoke code.
