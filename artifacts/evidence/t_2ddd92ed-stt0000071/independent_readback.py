#!/usr/bin/env python3
"""Zero-write independent readback for the frozen STT0000071 candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import scipy.sparse as sp
import zarr

BILLING_PROJECT = "jkobject-1549353370965"


def run(*args: str) -> str:
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generation_uri(identity: dict[str, Any]) -> str:
    return f"{identity['uri']}#{identity['generation']}"


def download(identity: dict[str, Any], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    run(
        "gcloud",
        "storage",
        "cp",
        f"--billing-project={BILLING_PROJECT}",
        generation_uri(identity),
        str(target),
    )
    observed = {"size": target.stat().st_size, "sha256": sha256(target)}
    expected = {"size": int(identity["size"]), "sha256": identity["sha256"]}
    if observed != expected:
        raise RuntimeError(
            f"physical readback mismatch for {identity['uri']}: {observed} != {expected}"
        )


def read_matrix(path: Path) -> sp.csr_matrix:
    with zarr.storage.ZipStore(str(path), mode="r") as store:
        group = zarr.open_group(store=store, mode="r")
        matrix = sp.csr_matrix(
            (
                np.asarray(group["data"][:]),
                np.asarray(group["indices"][:]),
                np.asarray(group["indptr"][:]),
            ),
            shape=tuple(group.attrs["shape"]),
        )
    matrix.sort_indices()
    return matrix


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate(manifest_path: Path, output_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    prefix = manifest["candidate_uri"]
    listed = json.loads(
        run(
            "gcloud",
            "storage",
            "ls",
            "--json",
            "--recursive",
            f"--billing-project={BILLING_PROJECT}",
            f"{prefix}/**",
        )
    )
    live = {
        f"gs://{item['metadata']['bucket']}/{item['metadata']['name']}": item[
            "metadata"
        ]
        for item in listed
        if item.get("type") == "cloud_object"
    }
    manifest_uri = f"{prefix}/manifest.json"
    if len(live) != 96 or manifest_uri not in live:
        raise RuntimeError(
            f"expected 96 live objects including manifest, observed {len(live)}"
        )

    identities = manifest["physical_outputs"]
    expected_uris = {item["uri"] for item in identities} | {manifest_uri}
    if set(live) != expected_uris:
        raise RuntimeError(
            "live object set does not exactly match manifest plus completion marker"
        )
    for identity in identities:
        metadata = live[identity["uri"]]
        observed = {
            "generation": str(metadata["generation"]),
            "size": int(metadata["size"]),
        }
        expected = {
            "generation": str(identity["generation"]),
            "size": int(identity["size"]),
        }
        if observed != expected:
            raise RuntimeError(
                f"live identity drift for {identity['uri']}: {observed} != {expected}"
            )

    manifest_created = parse_time(live[manifest_uri]["timeCreated"])
    payload_latest = max(
        parse_time(live[item["uri"]]["timeCreated"]) for item in identities
    )
    if manifest_created <= payload_latest:
        raise RuntimeError(
            f"manifest is not last: {manifest_created=} {payload_latest=}"
        )

    section_by_index = {item["chunk_index"]: item for item in manifest["sections"]}
    role_index = {(item["role"], item.get("chunk_index")): item for item in identities}
    totals = {"n_obs": 0, "nnz": 0, "sum_counts": 0}
    section_results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="stt0000071-independent-") as temporary:
        root = Path(temporary)
        var_identity = role_index[("shared_var", None)]
        var_path = root / "shared-var.parquet"
        download(var_identity, var_path)
        var = pd.read_parquet(var_path)
        if (
            list(var.columns)
            != [
                "gene_symbol",
                "organism",
                "feature_namespace",
                "ensembl_gene_id",
                "ensembl_missingness_reason",
            ]
            or len(var) != manifest["dimensions"]["n_vars"]
            or not var["gene_symbol"].is_unique
            or var.index.name != "var_id"
            or not var.index.equals(pd.Index(var["gene_symbol"], name="var_id"))
        ):
            raise RuntimeError("shared-var readback mismatch")

        source_identity = role_index[("source_inventory", None)]
        source_path = root / "source-inventory.json"
        download(source_identity, source_path)
        source_inventory = json.loads(source_path.read_text())
        if len(source_inventory["objects"]) != 138:
            raise RuntimeError(
                "source inventory does not contain 138 immutable non-TIFF identities"
            )

        missingness_identity = role_index[("metadata_quality_missingness", None)]
        missingness_path = root / "metadata-quality-missingness.json"
        download(missingness_identity, missingness_path)
        missingness = json.loads(missingness_path.read_text())
        if (
            missingness["dataset_age"]["reason"] != "source_not_reported"
            or missingness["quality"]["reason"]
            != "source_not_reported_or_no_reviewed_threshold"
            or missingness["images"]["status"]
            != "explicitly_excluded_from_this_expression_component"
        ):
            raise RuntimeError(
                "explicit metadata/quality missingness contract mismatch"
            )

        for chunk_index in range(46):
            section = section_by_index[chunk_index]
            obs_identity = section["obs"]
            matrix_identity = section["X"]
            obs_path = root / f"{chunk_index:03d}-obs.parquet"
            matrix_path = root / f"{chunk_index:03d}-X.zarr.zip"
            download(obs_identity, obs_path)
            download(matrix_identity, matrix_path)
            obs = pd.read_parquet(obs_path)
            matrix = read_matrix(matrix_path)
            observed = {
                "shape": [int(matrix.shape[0]), int(matrix.shape[1])],
                "nnz": int(matrix.nnz),
                "sum": int(matrix.sum()),
                "n_obs": int(len(obs)),
            }
            expected = {
                "shape": [
                    int(section["stats"]["n_obs"]),
                    int(manifest["dimensions"]["n_vars"]),
                ],
                "nnz": int(section["stats"]["nnz"]),
                "sum": int(section["stats"]["sum"]),
                "n_obs": int(section["stats"]["n_obs"]),
            }
            if observed != expected or not obs.index.is_unique:
                raise RuntimeError(
                    f"section {chunk_index} readback mismatch: {observed} != {expected}"
                )
            totals["n_obs"] += observed["n_obs"]
            totals["nnz"] += observed["nnz"]
            totals["sum_counts"] += observed["sum"]
            section_results.append({"chunk_index": chunk_index, **observed})

    expected_totals = {key: int(manifest["dimensions"][key]) for key in totals}
    if totals != expected_totals:
        raise RuntimeError(
            f"aggregate readback mismatch: {totals} != {expected_totals}"
        )
    report = {
        "format": "pert-gym.independent-component-readback/v1",
        "status": "PASS",
        "zero_write": True,
        "candidate_uri": prefix,
        "manifest": {
            "uri": manifest_uri,
            "generation": str(live[manifest_uri]["generation"]),
            "sha256": sha256(manifest_path),
            "time_created": live[manifest_uri]["timeCreated"],
            "manifest_last": True,
            "payload_latest_time_created": payload_latest.isoformat(),
        },
        "live_objects": len(live),
        "physical_payloads_verified": len(identities),
        "source_identities_bound": 138,
        "sections_verified": len(section_results),
        "shared_var_rows": int(manifest["dimensions"]["n_vars"]),
        "metadata_schemas": sorted(
            {item["stats"]["metadata_schema"] for item in manifest["sections"]}
        ),
        "dimensions": {**totals, "n_vars": int(manifest["dimensions"]["n_vars"])},
        "missingness": {
            "age": "source_not_reported",
            "quality": "source_not_reported_or_no_reviewed_threshold",
            "images": "explicitly_excluded_from_this_expression_component",
        },
        "mismatch": 0,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(validate(args.manifest, args.output), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
