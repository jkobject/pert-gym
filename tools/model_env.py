#!/usr/bin/env python3
"""Create and smoke isolated per-model uv environments for pert-gym.

This tool intentionally creates envs lazily under .venv-models/<model>. It does
not modify the base .venv and its smoke commands are read-only.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "model_envs.toml"
ENV_ROOT = ROOT / ".venv-models"


def load_config() -> dict[str, Any]:
    with CONFIG.open("rb") as handle:
        return tomllib.load(handle)


def model_specs() -> dict[str, dict[str, Any]]:
    return load_config()["models"]


def env_path(name: str) -> Path:
    return ENV_ROOT / name


def python_path(name: str) -> Path:
    env = env_path(name)
    if os.name == "nt":
        return env / "Scripts" / "python.exe"
    return env / "bin" / "python"


def run(cmd: list[str], *, dry_run: bool) -> None:
    printable = " ".join(shlex.quote(part) for part in cmd)
    print(f"$ {printable}")
    if dry_run:
        return
    subprocess.run(cmd, cwd=ROOT, check=True)


def require_model(name: str) -> dict[str, Any]:
    specs = model_specs()
    if name not in specs:
        valid = ", ".join(sorted(specs))
        raise SystemExit(f"unknown model env {name!r}; valid: {valid}")
    return specs[name]


def unsupported_message(name: str, spec: dict[str, Any]) -> str:
    blocker = spec.get("blocker", "upstream dependency stack is not installable here")
    upstream = spec.get("upstream_url")
    suffix = f"\nupstream: {upstream}" if upstream else ""
    return f"model env {name!r} is documented but not create-supported: {blocker}{suffix}"


def create(name: str, *, dry_run: bool = False) -> None:
    spec = require_model(name)
    if spec.get("create_supported", True) is False:
        raise SystemExit(unsupported_message(name, spec))

    py_version = str(spec["python"])
    env = env_path(name)
    py = python_path(name)

    if not py.exists():
        run(["uv", "venv", str(env), "--python", py_version], dry_run=dry_run)
    else:
        print(f"env exists: {env}")

    if spec.get("install_local", True):
        run(["uv", "pip", "install", "--python", str(py), "--no-deps", "-e", "."], dry_run=dry_run)
    else:
        print("local pert-gym editable install skipped by spec (install_local=false)")

    deps = list(spec.get("deps", []))
    if deps:
        cmd = ["uv", "pip", "install", "--python", str(py)]
        if torch_backend := spec.get("torch_backend"):
            cmd.extend(["--torch-backend", str(torch_backend)])
        cmd.extend(deps)
        run(cmd, dry_run=dry_run)
    else:
        print("no model-specific dependencies")


def smoke(name: str, *, dry_run: bool = False) -> None:
    spec = require_model(name)
    if spec.get("create_supported", True) is False:
        raise SystemExit(unsupported_message(name, spec))
    py = python_path(name)
    if not dry_run and not py.exists():
        raise SystemExit(f"missing env for {name}: run create first ({env_path(name)})")
    run([str(py), "tools/smoke_model_env.py", "--model", name], dry_run=dry_run)


def list_envs() -> None:
    for name, spec in sorted(model_specs().items()):
        deps = spec.get("deps", [])
        dep_summary = "none" if not deps else ", ".join(deps)
        support = "supported" if spec.get("create_supported", True) else "blocked"
        print(f"{name}: python {spec['python']} | {support} | deps: {dep_summary}")
        print(f"  includes: {', '.join(spec.get('includes', []))}")
        print(f"  env: {env_path(name)}")
        if spec.get("create_supported", True) is False:
            print(f"  blocker: {spec.get('blocker')}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")

    create_parser = sub.add_parser("create")
    create_parser.add_argument("model")
    create_parser.add_argument("--dry-run", action="store_true")

    smoke_parser = sub.add_parser("smoke")
    smoke_parser.add_argument("model")
    smoke_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "list":
        list_envs()
    elif args.command == "create":
        create(args.model, dry_run=args.dry_run)
    elif args.command == "smoke":
        smoke(args.model, dry_run=args.dry_run)
    else:  # pragma: no cover
        parser.error(f"unknown command {args.command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
