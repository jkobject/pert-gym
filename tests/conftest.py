"""Pytest import guards for repo-local helper modules."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT_STR = str(REPO_ROOT)

# Some embedding runtimes prepend their own top-level ``tools`` package to
# PYTHONPATH. The tests intentionally exercise pert-gym's repo-local tools/
# scripts, so make the repository root win before test modules are imported.
if sys.path[0] != REPO_ROOT_STR:
    sys.path.insert(0, REPO_ROOT_STR)

loaded_tools = sys.modules.get("tools")
loaded_tools_file = getattr(loaded_tools, "__file__", "") if loaded_tools else ""
if loaded_tools_file and not Path(loaded_tools_file).resolve().is_relative_to(
    REPO_ROOT / "tools"
):
    del sys.modules["tools"]
