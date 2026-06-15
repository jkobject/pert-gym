"""LaminDB connection helpers for pert-gym.

Use the Python API only: other local sessions may keep the Lamin CLI pointed at
different instances such as jkobject/jouvencekb.
"""

from __future__ import annotations

from pathlib import Path
import os


PERTDATA_INSTANCE = "laminlabs/pertdata"
PERTDATA_BRANCH_UID = "GCjqQtGwPzkY"
PERTDATA_BRANCH_NAME = "jkobject"


def ensure_project_cache() -> Path:
    """Keep Lamin's local artifact cache inside this repo, not shared ~/.cache."""
    cache_root = Path(os.environ.get("XDG_CACHE_HOME", Path.cwd() / ".lamin-cache"))
    os.environ["XDG_CACHE_HOME"] = str(cache_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    return cache_root


def ensure_pertdata_branch_file() -> Path:
    """Persist the intended pertdata branch without touching other instances."""
    ensure_project_cache()
    settings_root = Path(
        os.environ.get("LAMIN_SETTINGS_DIR", Path.cwd() / ".lamin-pertgym")
    )
    os.environ["LAMIN_SETTINGS_DIR"] = str(settings_root)
    os.environ["LAMIN_CURRENT_INSTANCE"] = PERTDATA_INSTANCE

    global_lamin_dir = Path.home() / ".lamin"
    lamin_dir = settings_root / ".lamin"
    lamin_dir.mkdir(parents=True, exist_ok=True)
    for filename in ["current_user.env", "user--jkobject.env"]:
        target = lamin_dir / filename
        source = global_lamin_dir / filename
        if not target.exists() and source.exists():
            target.write_bytes(source.read_bytes())

    branch_path = lamin_dir / "current-branch--laminlabs--pertdata.txt"
    branch_path.parent.mkdir(parents=True, exist_ok=True)
    branch_path.write_text(
        f"{PERTDATA_BRANCH_UID}\n{PERTDATA_BRANCH_NAME}", encoding="utf-8"
    )
    return branch_path


def connect_pertdata():
    """Connect to laminlabs/pertdata on the writable jkobject branch."""
    ensure_project_cache()
    ensure_pertdata_branch_file()

    import lamindb as ln

    ln.connect(PERTDATA_INSTANCE)
    branch = ln.setup.settings.branch
    if branch.name != PERTDATA_BRANCH_NAME or branch.uid != PERTDATA_BRANCH_UID:
        raise RuntimeError(
            "Expected Lamin branch "
            f"{PERTDATA_BRANCH_NAME}/{PERTDATA_BRANCH_UID}, got "
            f"{branch.name}/{branch.uid}."
        )
    return ln
