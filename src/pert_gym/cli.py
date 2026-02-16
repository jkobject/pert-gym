"""CLI for pert-gym."""

from __future__ import annotations

import argparse
from pathlib import Path

from . import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pert-gym",
        description="Command line interface for pert-gym.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the default pipeline.")
    run_parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/base.yml"),
        help="Path to YAML config file (default: config/base.yml).",
    )

    subparsers.add_parser("info", help="Print environment and project metadata.")
    subparsers.add_parser("check", help="Run a quick project health check.")
    return parser


def cmd_run(config: Path) -> int:
    print(f"Running pert-gym with config: {config}")
    if not config.exists():
        print(f"Warning: config file does not exist: {config}")
    return 0


def cmd_info() -> int:
    print("Project: pert-gym")
    print(f"Package: pert_gym")
    print(f"Version: {__version__}")
    return 0


def cmd_check() -> int:
    required = [Path("pyproject.toml"), Path("README.md"), Path("src")]
    missing = [p for p in required if not p.exists()]
    if missing:
        print("Missing required files:")
        for item in missing:
            print(f"- {item}")
        return 1
    print("Project health check passed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return cmd_run(args.config)
    if args.command == "info":
        return cmd_info()
    if args.command == "check":
        return cmd_check()

    parser.print_help()
    return 1
