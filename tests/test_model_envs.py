from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "model_envs.toml"


def test_model_env_manifest_covers_planned_families() -> None:
    with CONFIG.open("rb") as handle:
        models = tomllib.load(handle)["models"]

    assert {
        "baselines",
        "classical",
        "lpm",
        "cpa",
        "chemcpa",
        "trvae",
        "trvae-replacement",
        "scgen",
        "gears",
        "scpram",
    } <= set(models)
    assert "binary-split" in models["baselines"]["includes"]
    assert "ridge" in models["classical"]["includes"]
    assert "chemCPA" in models["chemcpa"]["includes"]
    assert models["trvae"]["create_supported"] is False
    assert models["trvae"]["replaced_by"] == "trvae-replacement"
    assert models["trvae-replacement"]["replaces"] == "trvae"
    assert "conditional-perturbation-vae" in models["trvae-replacement"]["includes"]
    assert models["scgen"]["upstream_package_assessed"] == "scgen==2.1.0"
    assert "scgen-perturbation-adapter" in models["scgen"]["includes"]
    assert "upstream_blocker" in models["scgen"]
    assert models["gears"]["upstream_package"] == "cell-gears==0.1.2"
    assert "cell-gears" in models["gears"]["includes"]
    assert "base Lamin env" in models["gears"]["isolation_policy"]
    assert models["scpram"]["install_local"] is False
    assert models["scpram"]["torch_backend"] == "cpu"


def test_model_env_manifest_uses_isolated_paths_and_safe_installs() -> None:
    with CONFIG.open("rb") as handle:
        config = tomllib.load(handle)

    assert config["defaults"]["layout"].startswith(".venv-models/")
    assert "--no-deps -e ." in config["defaults"]["local_package_install"]

    for name, spec in config["models"].items():
        assert (
            spec["install_command"] == f"uv run python tools/model_env.py create {name}"
        )
        assert spec["smoke_command"] == f"uv run python tools/model_env.py smoke {name}"
        data_policy = spec["data_policy"].lower()
        assert "lamin" in data_policy or "read-only" in data_policy
