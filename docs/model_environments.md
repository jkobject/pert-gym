# Isolated model environments

Perturbation model dependencies must not poison the Lamin ingestion/runtime
`.venv`. Model work uses lazy, per-family uv environments under:

```text
.venv-models/<model-family>/
```

`.venv-models/` is scratch and should be ignored by git. The checked-in source of
truth is `config/model_envs.toml`; create only the env you are actively working
on.

## Why this exists

The base project env carries LaminDB, ingestion tools, schema audits, and light
pure-Python model tests. Deep perturbation models pull incompatible or bulky
stacks (`torch`, `scvi-tools`, `scanpy`, `rdkit`, legacy VAE packages). Those
belong in isolated model envs so ingestion jobs and triplet audits remain stable.

## Commands

List planned envs:

```bash
uv run python tools/model_env.py list
```

Dry-run creation without installing anything:

```bash
uv run python tools/model_env.py create classical --dry-run
uv run python tools/model_env.py smoke classical --dry-run
```

Create and smoke the cheap pure-Python baseline env:

```bash
uv run python tools/model_env.py create baselines
uv run python tools/model_env.py smoke baselines
```

Create heavier envs only when implementing that family:

```bash
uv run python tools/model_env.py create cpa
uv run python tools/model_env.py smoke cpa
```

The manager installs local `pert-gym` as editable with `--no-deps` by default,
then installs only the dependencies listed for that model family. A model spec may
set `install_local=false` when upstream requires a Python version that cannot
satisfy this repo's `project.requires-python`; such smokes must import only the
upstream package or explicit exported data files.

## Environment matrix

| Env | Models/family | Python | Dependencies | Install | Smoke | Isolation policy |
| --- | --- | --- | --- | --- | --- | --- |
| `baselines` | mean-control, mean-perturbation, binary split | 3.11 | none beyond local package | `uv run python tools/model_env.py create baselines` | `uv run python tools/model_env.py smoke baselines` | No heavy deps; safe first smoke target. |
| `classical` | linear, ridge, elasticnet, random forest, gradient boosting, logistic cell-state classifier | 3.11 | numpy, scipy, scikit-learn, joblib | `uv run python tools/model_env.py create classical` | `uv run python tools/model_env.py smoke classical` | CPU sklearn stack only; separate from Lamin env. |
| `lpm` | latent perturbation model | 3.11 | numpy, scipy, torch, anndata | `uv run python tools/model_env.py create lpm` | `uv run python tools/model_env.py smoke lpm` | CPU torch by default; MPS/GPU changes are opt-in inside this env only. |
| `cpa` | standalone CPA-style autoencoder | 3.11 | torch, anndata | `uv run python tools/model_env.py create cpa` | `uv run python tools/model_env.py smoke cpa` | Torch/anndata isolated; scvi-tools route deferred until a stable direct CPA API is selected. |
| `chemcpa` | chemCPA | 3.10 | torch, anndata, scanpy, rdkit, chemCPA | `uv run python tools/model_env.py create chemcpa` | `uv run python tools/model_env.py smoke chemcpa` | Separate Python 3.10 env for dependency-pin risk. |
| `trvae` | legacy trVAE | 3.10 | `trVAE==1.1.2` | documented blocker; replaced by `trvae-replacement` | documented blocker | PyPI 1.1.2 pins TensorFlow 1.15.2 (`cp35`–`cp37` wheels only), incompatible with repo Python >=3.10 and Mac arm64; do not install legacy TensorFlow/Keras here. |
| `trvae-replacement` | conditional perturbation VAE analogue | 3.11 | torch, anndata | `uv run python tools/model_env.py create trvae-replacement` | `uv run python tools/model_env.py smoke trvae-replacement` | Maintained trVAE-like conditional latent smoke adapter; no TensorFlow 1.x, no old lowercase `trvae`, no base-env pollution. |
| `scgen` | scGEN-style conditional transfer adapter | 3.11 | torch, anndata | `uv run python tools/model_env.py create scgen` | `uv run python tools/model_env.py smoke scgen` | Upstream `scgen==2.1.0` was assessed but not enabled: it installs, then fails import under current Python 3.11 scverse resolver (`scvi._compat`/anndata/mudata private-API drift). Current route is the in-repo AnnData condition/control adapter smoke only. |
| `gears` | GEARS graph perturbation predictor | 3.11 | `cell-gears==0.1.2` (torch, torch_geometric, scanpy stack) | `uv run python tools/model_env.py create gears` | `uv run python tools/model_env.py smoke gears` | Official SNAP GEARS package isolated under `.venv-models/gears`; do not use unrelated PyPI `gears`; current smoke is dependency/API + synthetic data-contract only. |
| `scpram` | scPRAM | 3.8 | `scpram==0.0.3` via CPU torch backend | `uv run python tools/model_env.py create scpram` | `uv run python tools/model_env.py smoke scpram` | Upstream pins torch/torchaudio/torchvision 1.13/0.13/0.14 and scanpy 1.9.3; local pert-gym install skipped because this repo is Python >=3.10. |

## Data access policy

Model envs read model-ready data; they do not curate or write Lamin artifacts.
Preferred paths:

1. Use shared/base env tools to define or export a model-ready subset.
2. Pass local read-only paths/manifests to model env code.
3. Load triplets as data only: `obs.parquet`, `X.h5ad`, `var.parquet`.
4. Write model outputs/checkpoints under local run directories such as
   `artifacts/model-runs/<model>/`; do not call `ln.Artifact(...)`,
   `artifact.save()`, `features.set_values(...)`, or any Lamin branch mutation
   from a model env.

The local package import is intentionally lightweight: model envs install
`pert-gym` editable with `--no-deps`, so `pert_gym.models` and
`pert_gym.evaluate` can be imported without pulling LaminDB into every model
runtime.

## Adding a new model env

1. Add one `[models.<name>]` section to `config/model_envs.toml`.
2. Choose the oldest Python version required by that model's dependency pins.
3. Keep dependencies family-scoped; do not add them to `[project.dependencies]`
   or the base `dev` extra unless the shared package imports them directly.
4. Add or update a read-only smoke in `tools/smoke_model_env.py`.
5. Run `uv run python tools/model_env.py create <name> --dry-run`, then create
   the env only if the install is expected to be cheap/safe.
6. If upstream pins are not installable on this platform, set
   `create_supported=false`, add `upstream_url` and a precise `blocker`, and do
   not add an adapter until a maintained fork/container route exists.

## Current upstream notes

- trVAE: PyPI `trVAE==1.1.2` is the maintained package name/version but pins
  `tensorflow==1.15.2`; uv reports TensorFlow 1.15.2 wheels only for cp35/cp36/cp37,
  so this cannot be created from this Python >=3.10 repo on Mac arm64. An
  unpinned lowercase `trvae` resolution selects old `trvae==1.0.1` with modern
  TensorFlow/Keras, which is not the intended 1.1.2 implementation and is left
  blocked rather than adapted silently. As of 2026-06-22, active benchmark work
  should use `trvae-replacement`, the in-repo `ConditionalPerturbationVAE` smoke
  adapter documented in `docs/trvae_replacement.md`.
- GEARS: the official package is `cell-gears==0.1.2` from
  `https://github.com/snap-stanford/GEARS`; the unrelated PyPI package
  `gears==0.7.2` is a JavaScript/CSS asset tool and must not be used. Upstream
  GEARS expects AnnData with `obs.condition`, `obs.cell_type`, and
  `var.gene_name`; its README notes it is not designed for cross-cell-type
  training or bulk sequencing. Current artifact:
  `artifacts/model_benchmarks/gears_20260622.md` records dependency/API and
  synthetic data-contract smoke only; a real biological GEARS benchmark needs a
  separately promoted GEARS-ready perturbation subset.
- scPRAM: PyPI `scpram==0.0.3` resolves in a Python 3.8 CPU env with upstream
  pins (`scanpy==1.9.3`, `torch==1.13.1`, `torchaudio==0.13.1`,
  `torchvision==0.14.1`). The installed 0.0.3 package currently reports
  `scpram.__version__ == "0.0.2"`, so rely on `uv pip list`/the lock-free env
  install command rather than that module attribute for provenance. The first
  real adapter smoke is `tools/run_scpram_real_adapter.py`, which follows the
  upstream contract: binary `condition` (`control`/`stimulated`) for one selected
  perturbation, real `cell_type`/context as the transfer axis, and a held-out
  stimulated target context. It explicitly does not reuse the old MB6
  perturbation-identity-as-cell-type hack. Current artifact:
  `artifacts/model_benchmarks/scpram_real_adapter_20260622.md`; current
  model-ready v0 is infeasible for a real scPRAM benchmark because it has one
  unknown-context VIPerturb screen chunk rather than controls/stimulated rows for
  one perturbation across multiple real cell types/contexts.

## CPA route decision — 2026-06-22

M5-Mac uses `src/pert_gym/models/cpa.py`, a tiny standalone torch-backed
`CompositionalPerturbationAutoencoder`, instead of installing `scvi-tools` for
the first smoke. This gives pert-gym a real `fit`/`predict` CPA boundary and a
model-ready-v0 synthetic fallback benchmark without pulling the full scvi/scanpy
stack into the CPA env. See `docs/cpa_baseline.md` for API details and remaining
production blockers.


## trVAE replacement route decision — 2026-06-22

M8-Mac replaces the blocked legacy trVAE train/eval path with
`src/pert_gym/models/conditional_vae.py`, a tiny torch-backed conditional VAE
adapter. The selected env is `trvae-replacement` on Python 3.11 with only
`torch>=2.3` and `anndata>=0.10`. Candidate routes considered were scGen/scArches,
scVI-tools covariate models, CPA-like VAE, and a small in-repo conditional VAE;
the in-repo adapter won because it directly implements the current
`PerturbationModel` boundary and avoids a full scanpy/scvi dependency stack for a
smoke benchmark. See `docs/trvae_replacement.md` and
`artifacts/model_benchmarks/trvae_replacement_20260622.md`.

## scGEN route decision — 2026-06-22

M8B-Mac adds `src/pert_gym/models/scgen_adapter.py`, an AnnData-first
scGEN-style condition/control adapter backed by the tiny maintained torch
conditional VAE. Upstream PyPI `scgen==2.1.0` was inspected and trial-installed
in `.venv-models/scgen`, but it is not currently enabled: the latest resolver
path failed on missing `scvi._compat`, and pinning `scvi-tools<1` exposed
`anndata`/`mudata` private-API incompatibilities. The reproducible scGEN env now
uses only `torch>=2.3` and `anndata>=0.10`; the smoke artifact is
`artifacts/model_benchmarks/scgen_20260622.md`.

M8B-follow-up (`t_8a3acae6`) adds a bounded real VIPerturb scGEN-ready export
from the reviewed `pert-gym/model-ready/20260621` member:
`artifacts/model_benchmarks/scgen_real_viperturb_tiny_20260622.{json,h5ad}`.
The export has 22 obs × 96 genes, 12 controls, five CRISPRi perturbation
identities, `obs.condition`, `obs.control_value == "control"`,
`obs.perturbation`, `obs.is_control`, and optional `batch`/`cell_type` fields.
`tools/run_scgen_benchmark.py --real-artifact ...` now writes
`artifacts/model_benchmarks/scgen_real_20260622.{json,md}` with no synthetic
fallback and with non-control perturbation identities disjoint across
train/val/test while controls are present in every split.

Limitations: this is still a bounded adapter smoke, not a biological scGEN
performance claim. Upstream `scgen==2.1.0` remains disabled under the current
Python 3.11 scverse resolver, and model envs must read the local export only; any
future Lamin promotion/revision belongs in the base env, not `.venv-models/scgen`.
