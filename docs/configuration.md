# Configuration

Configuration in `pert-gym` is split by responsibility. Keep runtime configs, model-environment specs, and generated status artifacts separate.

## Pipeline config

Default YAML configuration files live under `config/`.

```text
config/base.yml
config/README.md
```

The current `pert-gym run` command accepts a config path:

```bash
pert-gym run --config config/base.yml
```

At present, `run` is lightweight scaffolding: it reports the selected config and warns if the file is missing. Do not assume it performs ingestion, training, or benchmark orchestration.

## Model environment config

Optional model families are configured in:

```text
config/model_envs.toml
```

Use `tools/model_env.py` to inspect or create those environments:

```bash
uv run python tools/model_env.py list
uv run python tools/model_env.py create classical --dry-run
uv run python tools/model_env.py smoke classical --dry-run
```

See [Isolated model environments](model_environments.md) for the matrix and dependency-isolation rules.

## Lamin / data configuration

The active Lamin target is documented in `wiki/pert-gym/current-status.md`: `laminlabs/pertdata`, branch `jkobject`.

Code should connect through:

```python
from tools.lamin_context import connect_pertdata

ln = connect_pertdata()
```

Do not rely on global Lamin CLI state. Large raw downloads are temporary cache; durable curated data belongs in LaminDB or project GCS staging, not Git.

## Artifact paths

Common generated outputs:

- `artifacts/schema_audit/` — schema audits, Collection manifests, validation reports.
- `artifacts/model_benchmarks/` — benchmark loader smoke outputs.
- `artifacts/model-runs/<model>/` — local model run outputs/checkpoints.

These are status/artifact outputs, not primary configuration. Prefer `wiki/pert-gym/current-status.md` for the latest human-readable status.
