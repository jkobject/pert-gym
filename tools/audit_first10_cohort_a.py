#!/usr/bin/env python3
"""Read-only bounded audit for the first-10 cohort-A cleaned datasets.

This script is intended to run on ``pert-gym-worker-eu`` through
``tools/launch_pert_gym_heavy.py --verify-only``. It does not call ``ln.track``
and never creates, revises, links, or deletes Lamin/GCS records. JSON is emitted
to stdout so the caller can retain the evidence packet in Git.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h5py
import pandas as pd

ROOT = Path(os.environ.get("PERT_GYM_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_2122b5f4"
BILLING_PROJECT = "jkobject-1549353370965"
DATASETS = {
    "E-MTAB-9304": {
        "canonical_obs_uid": "rt5eRz8opcJXtybp0000",
        "fallback_prefix": "temporal_pretraining/gxa/E-MTAB-9304_drosophila_dorsal_ventral_patterning/chunk_0000",
        "search_tokens": ["E-MTAB-9304"],
    },
    "GSE130238": {
        "canonical_prefix": "data/cleaned/GSE130238",
        "search_tokens": ["GSE130238", "ODD001111"],
    },
    "GSE138002": {
        "canonical_prefix": "data/cleaned/GSE138002",
        "search_tokens": ["GSE138002", "ODD001099"],
    },
}
ARTIFACT_FIELDS = (
    "key",
    "uid",
    "hash",
    "size",
    "suffix",
    "otype",
    "n_observations",
    "created_at",
    "is_latest",
    "description",
)


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if hasattr(value, "item"):
        try:
            return jsonable(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    return str(value)


def artifact_record(artifact: Any) -> dict[str, Any]:
    return {
        field: jsonable(getattr(artifact, field, None)) for field in ARTIFACT_FIELDS
    }


def series_summary(series: pd.Series, *, top_n: int = 20) -> dict[str, Any]:
    missing = int(series.isna().sum())
    text = series.dropna().astype(str)
    counts = text.value_counts(dropna=False).head(top_n)
    return {
        "dtype": str(series.dtype),
        "rows": int(len(series)),
        "missing": missing,
        "missing_fraction": missing / len(series) if len(series) else None,
        "unique_non_null": int(series.nunique(dropna=True)),
        "empty_string": int(text.str.strip().eq("").sum()),
        "has_control_character": int(text.str.contains(r"[\t\r\n]", regex=True).sum()),
        "top_values": [
            {"value": str(value), "count": int(count)}
            for value, count in counts.items()
        ],
    }


def dataframe_summary(frame: pd.DataFrame) -> dict[str, Any]:
    index = pd.Series(frame.index.astype(str), dtype="string")
    return {
        "shape": [int(frame.shape[0]), int(frame.shape[1])],
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256_ordered": ordered_string_sha256(frame.index.astype(str)),
        "index_summary": series_summary(index),
        "columns": {
            str(column): series_summary(frame[column]) for column in frame.columns
        },
    }


def ordered_string_sha256(values: Any) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = str(value).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def var_identifier_audit(index: pd.Index) -> dict[str, Any]:
    values = pd.Series(index.astype(str), dtype="string")
    patterns = {
        "actual_tab": r"\t",
        "literal_backslash_t": r"\\t",
        "literal_slash_t": r"/t",
        "carriage_return_or_newline": r"[\r\n]",
        "leading_or_trailing_whitespace": r"^\s|\s$",
        "ensembl_gene": r"^ENS[A-Z]*G\d+(?:\.\d+)?$",
        "flybase_gene": r"^FBgn\d+$",
    }
    matches: dict[str, Any] = {}
    for name, pattern in patterns.items():
        mask = values.str.contains(pattern, regex=True, na=False)
        matches[name] = {
            "count": int(mask.sum()),
            "examples": values[mask].head(20).tolist(),
        }
    split_tabs = values.str.split("\t", n=2, expand=True)
    tab_structure = {
        "parts": int(split_tabs.shape[1]),
        "first_field_unique": bool(split_tabs[0].is_unique),
        "first_field_blank": int(split_tabs[0].str.strip().eq("").sum()),
        "first_field_examples": split_tabs[0].head(20).tolist(),
    }
    if split_tabs.shape[1] > 1:
        tab_structure.update(
            {
                "second_field_blank": int(
                    split_tabs[1].fillna("").str.strip().eq("").sum()
                ),
                "second_field_examples": split_tabs[1].head(20).tolist(),
            }
        )
    return {
        "rows": int(len(values)),
        "unique": bool(index.is_unique),
        "duplicate_count": int(index.duplicated().sum()),
        "patterns": matches,
        "tab_split_structure": tab_structure,
    }


def feature_values(artifact: Any) -> dict[str, Any]:
    try:
        values = artifact.features.get_values()
    except Exception as exc:  # evidence must preserve API/readback failures
        return {"error": f"{type(exc).__name__}: {exc}"}
    result: dict[str, Any] = {}
    for name, value in values.items():
        if hasattr(value, "key") or hasattr(value, "uid"):
            result[str(name)] = {
                "key": getattr(value, "key", None),
                "uid": getattr(value, "uid", None),
            }
        elif isinstance(value, (list, tuple)):
            result[str(name)] = [
                {
                    "key": getattr(item, "key", None),
                    "uid": getattr(item, "uid", None),
                }
                if hasattr(item, "key") or hasattr(item, "uid")
                else jsonable(item)
                for item in value
            ]
        else:
            result[str(name)] = jsonable(value)
    return result


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def materialize_artifact(artifact: Any, target: Path) -> dict[str, Any]:
    """Copy one exact payload with explicit Requester Pays billing evidence."""
    uri = str(artifact.path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if uri.startswith("gs://"):
        stat = subprocess.run(
            [
                "gcloud",
                "storage",
                "ls",
                f"--billing-project={BILLING_PROJECT}",
                "-L",
                uri,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if stat.returncode:
            return {
                "uri": uri,
                "path_exists": False,
                "error": (stat.stdout + stat.stderr).strip()[-2000:],
            }
        copied = subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                f"--billing-project={BILLING_PROJECT}",
                uri,
                str(target),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if copied.returncode:
            return {
                "uri": uri,
                "path_exists": False,
                "remote_stat": stat.stdout,
                "error": (copied.stdout + copied.stderr).strip()[-2000:],
            }
        stat_evidence = stat.stdout
    else:
        cached = Path(artifact.cache())
        target.write_bytes(cached.read_bytes())
        stat_evidence = "non-GCS artifact materialized through Lamin cache"
    return {
        "uri": uri,
        "path_exists": target.is_file(),
        "downloaded_bytes": target.stat().st_size,
        "downloaded_sha256": file_sha256(target),
        "remote_stat": stat_evidence,
    }


def linked_artifact_identity(value: Any) -> tuple[str | None, str | None]:
    if hasattr(value, "key") or hasattr(value, "uid"):
        return getattr(value, "key", None), getattr(value, "uid", None)
    if isinstance(value, str):
        return value, None
    return None, None


def latest_exact_key_candidates(ln: Any, key: str) -> list[dict[str, Any]]:
    candidates = list(
        ln.Artifact.filter(is_latest=True, key=key).order_by("uid").all()[:3]
    )
    return [artifact_record(candidate) for candidate in candidates]


def exact_link(ln: Any, artifact: Any, name: str) -> dict[str, Any]:
    try:
        values = artifact.features.get_values()
        value = values.get(name)
    except Exception as exc:
        return {"resolved": False, "error": f"{type(exc).__name__}: {exc}"}
    if isinstance(value, (list, tuple)):
        if len(value) != 1:
            return {
                "resolved": False,
                "cardinality": len(value),
                "raw": jsonable(value),
            }
        value = value[0]
    raw_key, raw_uid = linked_artifact_identity(value)
    if raw_key is None and raw_uid is None:
        return {"resolved": False, "raw": jsonable(value)}
    try:
        if raw_key is None:
            raw_key = ln.Artifact.get(uid=raw_uid).key
        candidates = list(
            ln.Artifact.filter(is_latest=True, key=raw_key).order_by("uid").all()[:3]
        )
    except Exception as exc:
        return {
            "resolved": False,
            "raw_key": raw_key,
            "raw_uid": raw_uid,
            "error": f"{type(exc).__name__}: {exc}",
        }
    if len(candidates) != 1:
        return {
            "resolved": False,
            "raw_key": raw_key,
            "raw_uid": raw_uid,
            "latest_exact_key_candidate_count": len(candidates),
            "latest_exact_key_candidates": [
                artifact_record(candidate) for candidate in candidates
            ],
        }
    resolved = candidates[0]
    if raw_uid is not None and raw_uid != resolved.uid:
        return {
            "resolved": False,
            "raw_key": raw_key,
            "raw_uid": raw_uid,
            "key": resolved.key,
            "uid": resolved.uid,
            "error": "feature UID conflicts with the unique current artifact for its key",
        }
    return {
        "resolved": True,
        "raw_key": raw_key,
        "raw_uid": raw_uid,
        "key": resolved.key,
        "uid": resolved.uid,
        "latest_exact_key_candidate_count": 1,
    }


def _h5_string_list(dataset: h5py.Dataset) -> list[str]:
    raw = dataset.asstr()[...]
    return [str(value) for value in raw.tolist()]


def _axis_index(handle: h5py.File, axis: str) -> list[str]:
    group = handle[axis]
    index_name = group.attrs.get("_index", "_index")
    index_name = jsonable(index_name)
    if not isinstance(index_name, str) or index_name not in group:
        if "_index" in group:
            index_name = "_index"
        else:
            raise KeyError(f"cannot resolve {axis} index dataset")
    return _h5_string_list(group[index_name])


def h5ad_metadata(path: Path) -> dict[str, Any]:
    with h5py.File(path, "r") as handle:
        obs_index = _axis_index(handle, "obs")
        var_index = _axis_index(handle, "var")
        x = handle["X"]
        shape = x.attrs.get("shape")
        if shape is None and isinstance(x, h5py.Dataset):
            shape = x.shape
        if shape is None:
            shape = [len(obs_index), len(var_index)]
        return {
            "shape": [int(item) for item in shape],
            "x_hdf5_type": type(x).__name__,
            "x_encoding_type": jsonable(x.attrs.get("encoding-type")),
            "x_encoding_version": jsonable(x.attrs.get("encoding-version")),
            "x_dtype": str(x.dtype)
            if isinstance(x, h5py.Dataset)
            else str(x["data"].dtype),
            "x_group_members": sorted(x.keys()) if isinstance(x, h5py.Group) else [],
            "obs_index_sha256_ordered": ordered_string_sha256(obs_index),
            "var_index_sha256_ordered": ordered_string_sha256(var_index),
            "obs_index_unique": len(obs_index) == len(set(obs_index)),
            "var_index_unique": len(var_index) == len(set(var_index)),
        }


def load_manifest(path: Path) -> Any:
    try:
        return jsonable(json.loads(path.read_text(encoding="utf-8")))
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def resolve_canonical(ln: Any, dataset: str) -> tuple[str, dict[str, Any]]:
    config = DATASETS[dataset]
    uid = config.get("canonical_obs_uid")
    if uid:
        obs = ln.Artifact.get(uid=uid)
        if not obs.key.endswith("/obs.parquet"):
            raise ValueError(f"{uid} is not an obs artifact: {obs.key}")
        prefix = obs.key[: -len("/obs.parquet")]
    else:
        prefix = config["canonical_prefix"]
    artifacts = {
        role: ln.Artifact.get(key=f"{prefix}/{suffix}")
        for role, suffix in {
            "obs": "obs.parquet",
            "X": "X.h5ad",
            "var": "var.parquet",
        }.items()
    }
    return prefix, artifacts


def resolve_prefix(ln: Any, prefix: str) -> dict[str, Any]:
    return {
        role: ln.Artifact.get(key=f"{prefix}/{suffix}")
        for role, suffix in {
            "obs": "obs.parquet",
            "X": "X.h5ad",
            "var": "var.parquet",
        }.items()
    }


def query_related(ln: Any, dataset: str, prefix: str) -> list[dict[str, Any]]:
    records: dict[str, Any] = {}
    for token in DATASETS[dataset]["search_tokens"]:
        for artifact in ln.Artifact.filter(key__icontains=token).order_by("key").all():
            records[artifact.uid] = artifact
        for artifact in (
            ln.Artifact.filter(description__icontains=token).order_by("key").all()
        ):
            records[artifact.uid] = artifact
    for artifact in (
        ln.Artifact.filter(key__startswith=f"{prefix}/").order_by("key").all()
    ):
        records[artifact.uid] = artifact
    return [artifact_record(records[uid]) for uid in sorted(records)]


def audit_dataset(ln: Any, dataset: str) -> dict[str, Any]:
    prefix, artifacts = resolve_canonical(ln, dataset)
    with tempfile.TemporaryDirectory(prefix=f"first10-a-{dataset}-") as tmp:
        local = {
            "obs": Path(tmp) / "obs.parquet",
            "X": Path(tmp) / "X.h5ad",
            "var": Path(tmp) / "var.parquet",
        }
        payload_evidence = {
            role: materialize_artifact(artifact, local[role])
            for role, artifact in artifacts.items()
        }
        inspection_prefix = prefix
        inspection_artifacts = artifacts
        inspection_evidence = payload_evidence
        inspection_relation = "canonical"
        if not all(item.get("path_exists") for item in payload_evidence.values()):
            fallback_prefix = DATASETS[dataset].get("fallback_prefix")
            if fallback_prefix is None:
                missing = [
                    role
                    for role, item in payload_evidence.items()
                    if not item.get("path_exists")
                ]
                raise RuntimeError(f"canonical payloads unavailable: {missing}")
            inspection_prefix = fallback_prefix
            inspection_artifacts = resolve_prefix(ln, fallback_prefix)
            local = {
                "obs": Path(tmp) / "fallback" / "obs.parquet",
                "X": Path(tmp) / "fallback" / "X.h5ad",
                "var": Path(tmp) / "fallback" / "var.parquet",
            }
            inspection_evidence = {
                role: materialize_artifact(artifact, local[role])
                for role, artifact in inspection_artifacts.items()
            }
            missing = [
                role
                for role, item in inspection_evidence.items()
                if not item.get("path_exists")
            ]
            if missing:
                raise RuntimeError(f"fallback payloads unavailable: {missing}")
            inspection_relation = "fallback_noncanonical_partial_payload"

        obs = pd.read_parquet(local["obs"])
        var = pd.read_parquet(local["var"])
        x_meta = h5ad_metadata(local["X"])

        manifest_artifacts = [
            item
            for item in query_related(ln, dataset, prefix)
            if str(item.get("key", "")).endswith("manifest.json")
        ]
        manifests = []
        for index, item in enumerate(manifest_artifacts):
            artifact = ln.Artifact.get(uid=item["uid"])
            manifest_path = Path(tmp) / f"manifest-{index}.json"
            manifest_payload = materialize_artifact(artifact, manifest_path)
            manifests.append(
                {
                    "artifact": item,
                    "payload_evidence": manifest_payload,
                    "payload": load_manifest(manifest_path)
                    if manifest_payload.get("path_exists")
                    else {"error": "manifest payload is unavailable"},
                }
            )

    obs_summary = dataframe_summary(obs)
    var_summary = dataframe_summary(var)

    obs_link = exact_link(ln, inspection_artifacts["obs"], "X")
    var_link = exact_link(ln, inspection_artifacts["X"], "var")
    expected_x_key = inspection_artifacts["X"].key
    expected_var_key = inspection_artifacts["var"].key
    return {
        "format": "pert-gym.first10-cohort-a-live-audit/v1",
        "task_id": TASK_ID,
        "dataset": dataset,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "canonical_prefix": prefix,
        "canonical_artifacts": {
            role: {
                **artifact_record(artifact),
                "payload_evidence": payload_evidence[role],
                "feature_values": feature_values(artifact),
                "latest_exact_key_candidates": latest_exact_key_candidates(
                    ln, artifact.key
                ),
            }
            for role, artifact in artifacts.items()
        },
        "payload_inspection": {
            "prefix": inspection_prefix,
            "relation_to_canonical": inspection_relation,
            "artifacts": {
                role: {
                    **artifact_record(artifact),
                    "payload_evidence": inspection_evidence[role],
                    "latest_exact_key_candidates": latest_exact_key_candidates(
                        ln, artifact.key
                    ),
                }
                for role, artifact in inspection_artifacts.items()
            },
        },
        "obs": obs_summary,
        "var": {
            **var_summary,
            "identifier_audit": var_identifier_audit(var.index),
        },
        "X": x_meta,
        "triplet_validation": {
            "obs_rows_equal_X_rows": obs.shape[0] == x_meta["shape"][0],
            "var_rows_equal_X_columns": var.shape[0] == x_meta["shape"][1],
            "obs_index_equal_X_obs_index": obs_summary["index_sha256_ordered"]
            == x_meta["obs_index_sha256_ordered"],
            "var_index_equal_X_var_index": var_summary["index_sha256_ordered"]
            == x_meta["var_index_sha256_ordered"],
            "obs_X_link": obs_link,
            "X_var_link": var_link,
            "obs_X_link_is_exact": obs_link.get("resolved") is True
            and obs_link.get("key") == expected_x_key
            and obs_link.get("uid") == inspection_artifacts["X"].uid,
            "X_var_link_is_exact": var_link.get("resolved") is True
            and var_link.get("key") == expected_var_key
            and var_link.get("uid") == inspection_artifacts["var"].uid,
            "same_prefix_var": inspection_artifacts["var"].key
            == f"{inspection_prefix}/var.parquet",
            "same_prefix_var_latest_candidates": latest_exact_key_candidates(
                ln, f"{inspection_prefix}/var.parquet"
            ),
        },
        "related_lamin_artifacts": query_related(ln, dataset, prefix),
        "manifests": manifests,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=sorted(DATASETS))
    parser.add_argument(
        "--output",
        type=Path,
        help="optional JSON output path (stdout remains the default)",
    )
    args = parser.parse_args()
    ensure_project_cache()
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise RuntimeError("unexpected Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise RuntimeError("unexpected Lamin branch")
    rendered = json.dumps(audit_dataset(ln, args.dataset), indent=2, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
