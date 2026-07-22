#!/usr/bin/env python3
"""Read-only Arc VCC OBS/X/VAR identity and metadata probe.

This script never calls ``ln.track`` or any save/update API. It resolves every
link from the exact current OBS artifact and loads only OBS/VAR frames.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.lamin_context import connect_pertdata

DATASET_ID = "arc-vcc/2025"
EXPECTED = (
    ("arc_vcc/2025/test/chunk_0000/obs.parquet", "mGQwo6Kqs9hOdzLl0000", 100_000, "35UCTnLWm1E8AI8l0001"),
    ("arc_vcc/2025/test/chunk_0001/obs.parquet", "jO1U5UVWKJ6gpS0H0000", 70_846, "jMYzsqDae2UOGaLS0001"),
    ("arc_vcc/2025/train/chunk_0000/obs.parquet", "kcoW2Hh7iua05uC30000", 100_000, "BEdeusbAG0YhvWen0001"),
    ("arc_vcc/2025/train/chunk_0001/obs.parquet", "GRgJf9TdsDZqOqGU0000", 100_000, "HWI9cAQzG4x0j3nn0001"),
    ("arc_vcc/2025/train/chunk_0002/obs.parquet", "RJ3rYBXHNl8c4Lym0000", 21_273, "MvUVFg0pX9Z19W0A0001"),
    ("arc_vcc/2025/validation/chunk_0000/obs.parquet", "Zk9b1xHT1OX9cUgZ0000", 98_927, "TPAZTaNsYFxzqLXE0001"),
)
FROZEN = {
    "artifacts/schema_audit/real_biological_dataset_crosswalk_120_families_20260716.json": "65388d3d575d99961e2f8fb62d35dd38366d50268068ff144445af6530b54a9b",
    "artifacts/schema_audit/final_real_dataset_obs_var_20260717.json": "60530cc3a14fe28e1dbf06c9f62b3e993649750069287083e4351aea3f8318df",
    "artifacts/schema_audit/var_ensembl_real_datasets_70_baseline_20260717.json": "a073c934e66423c1ae38f0d7a01dc60f00d4f0d05f510ac169b4a18c4ac090f7",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def resolve_artifact(ln: Any, value: Any):
    if hasattr(value, "uid") and hasattr(value, "key"):
        return value
    if isinstance(value, str):
        for field in ("uid", "key"):
            rows = list(ln.Artifact.filter(**{field: value}).all())
            if rows:
                return rows[-1]
    raise TypeError(f"cannot resolve Artifact feature value {value!r}")


def values_for(artifact: Any) -> dict[str, Any]:
    values = artifact.features.get_values()
    if not isinstance(values, dict):
        raise TypeError(f"feature values are not a dict for {artifact.uid}: {type(values)!r}")
    return values


def frame_summary(frame: Any) -> dict[str, Any]:
    columns: dict[str, Any] = {}
    for name in frame.columns:
        series = frame[name]
        nonmissing = int(series.notna().sum())
        text = series.astype("string")
        substantive = text.notna() & ~text.str.strip().str.lower().isin(
            {"", "unknown", "missing", "nan", "none", "null"}
        )
        examples = [str(value) for value in text[substantive].drop_duplicates().head(8).tolist()]
        columns[str(name)] = {
            "dtype": str(series.dtype),
            "nonmissing": nonmissing,
            "substantive": int(substantive.sum()),
            "unique_nonmissing": int(text.dropna().nunique()),
            "examples": examples,
        }
    return {
        "rows": int(frame.shape[0]),
        "columns": int(frame.shape[1]),
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_examples": [str(value) for value in frame.index[:5].tolist()],
        "fields": columns,
    }


def main() -> int:
    root = Path.cwd()
    receipt: dict[str, Any] = {
        "schema": "arc_vcc_obs_var_readonly_probe/v1",
        "dataset_id": DATASET_ID,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "pid": os.getpid(),
        "mode": "read_only",
        "frozen_files": {},
        "members": [],
    }
    for relative, expected in FROZEN.items():
        path = root / relative
        actual = sha256(path) if path.exists() else None
        receipt["frozen_files"][relative] = {
            "exists": path.exists(),
            "expected_sha256": expected,
            "actual_sha256": actual,
            "match": actual == expected,
        }

    ln = connect_pertdata()
    receipt["lamin"] = {
        "instance": ln.setup.settings.instance.slug,
        "branch_name": ln.setup.settings.branch.name,
        "branch_uid": ln.setup.settings.branch.uid,
    }
    if receipt["lamin"] != {
        "instance": "laminlabs/pertdata",
        "branch_name": "jkobject",
        "branch_uid": "GCjqQtGwPzkY",
    }:
        raise RuntimeError(f"unexpected Lamin target: {receipt['lamin']}")

    total_rows = 0
    for expected_key, expected_obs_uid, expected_rows, expected_var_uid in EXPECTED:
        obs_artifact = ln.Artifact.get(uid=expected_obs_uid)
        if obs_artifact.key != expected_key:
            raise RuntimeError(
                f"OBS key drift for {expected_obs_uid}: {obs_artifact.key!r} != {expected_key!r}"
            )
        obs_values = values_for(obs_artifact)
        x_artifact = resolve_artifact(ln, obs_values["X"])
        x_values = values_for(x_artifact)
        var_artifact = resolve_artifact(ln, x_values["var"])
        if var_artifact.uid != expected_var_uid:
            raise RuntimeError(
                f"VAR UID drift for {expected_key}: {var_artifact.uid!r} != {expected_var_uid!r}"
            )
        obs = obs_artifact.load()
        var = var_artifact.load()
        if len(obs) != expected_rows or len(var) != 18_080:
            raise RuntimeError(
                f"axis drift for {expected_key}: obs={len(obs)}, var={len(var)}"
            )
        total_rows += len(obs)
        receipt["members"].append(
            {
                "obs": {
                    "uid": obs_artifact.uid,
                    "key": obs_artifact.key,
                    "hash": obs_artifact.hash,
                    "path": str(obs_artifact.path),
                    "frame": frame_summary(obs),
                },
                "x": {
                    "uid": x_artifact.uid,
                    "key": x_artifact.key,
                    "hash": x_artifact.hash,
                    "path": str(x_artifact.path),
                    "n_observations": x_artifact.n_observations,
                },
                "var": {
                    "uid": var_artifact.uid,
                    "key": var_artifact.key,
                    "hash": var_artifact.hash,
                    "path": str(var_artifact.path),
                    "frame": frame_summary(var),
                },
            }
        )
    receipt["total_rows"] = total_rows
    receipt["expected_total_rows"] = 491_046
    receipt["ok"] = total_rows == 491_046
    print(json.dumps(receipt, indent=2, sort_keys=True, default=str))
    return 0 if receipt["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

