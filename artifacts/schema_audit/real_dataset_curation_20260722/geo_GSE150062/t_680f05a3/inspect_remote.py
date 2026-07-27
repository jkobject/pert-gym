#!/usr/bin/env python3
"""Read-only source and live-triplet inspection for GEO GSE150062."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import re
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_680f05a3"
PREFIX = "prism_collection/GSE150062"
EXPECTED_N_OBS = 78_393
EXPECTED_N_VARS = 60_497
EXPECTED = {
    "obs": {"uid": "CkcQf1IYkOkbxKed0000", "key": f"{PREFIX}/obs.parquet"},
    "var": {"uid": "rRlvtvSEpbFnek7K0001", "key": f"{PREFIX}/var.parquet"},
}
GEO_ROOT = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE150nnn/GSE150062/suppl"
SOURCE_FILES = {
    "GSE150062_perturbseq_barcodes.tsv.gz": (338_738, "d86f132cc056002f55f67ce7658b6dcb1930e0bb577dc173cc958226d7fe4e37"),
    "GSE150062_perturbseq_features.tsv.gz": (222_231, "90e1a3254c97215191bf9dbae0a603e6fb1b908da731e5505395a805fb790b28"),
    "GSE150062_perturbseq_genes.tsv.gz": (222_165, "b6d13a64dca80162b49414a0967522326e77b58b40b08da93a672ffe0b6afa57"),
    "GSE150062_sgrna_barcodes.tsv.gz": (338_738, "d86f132cc056002f55f67ce7658b6dcb1930e0bb577dc173cc958226d7fe4e37"),
    "GSE150062_sgrna_features.tsv.gz": (2_763, "e0b66b2ba30c18d07c33ba861793857b2984c6aa2251e6c4ccd4cb1695b6bae3"),
    "GSE150062_sgrna_genes.tsv.gz": (1_428, "fc7f4c16c63f5238cf57fbe0e56c670c05f3fb31d1f278a2068979743fb896d4"),
}
PMC_TABLE_URL = "https://pmc.ncbi.nlm.nih.gov/articles/instance/9903861/bin/mmc6.xlsx"
PMC_TABLE_SIZE = 5_338_747
PMC_TABLE_SHA256 = "fd307e6aec5a0044e0ec135594ed1d3071d3efb609809faa1f0ef91d111b465c"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: pd.Index) -> str:
    return hashlib.sha256("\n".join(values.astype(str)).encode()).hexdigest()


def download(url: str, path: Path, size: int, sha256: str) -> None:
    if not path.exists() or path.stat().st_size != size or sha256_file(path) != sha256:
        request = urllib.request.Request(url, headers={"User-Agent": "pert-gym-source-audit/1"})
        with urllib.request.urlopen(request, timeout=120) as source, path.open("wb") as target:
            while block := source.read(8 * 1024 * 1024):
                target.write(block)
    if path.stat().st_size != size or sha256_file(path) != sha256:
        raise AssertionError(f"source identity drift: {url}")


def download_pmc_table(path: Path) -> None:
    if path.exists() and path.stat().st_size == PMC_TABLE_SIZE and sha256_file(path) == PMC_TABLE_SHA256:
        return
    request = urllib.request.Request(PMC_TABLE_URL, headers={"User-Agent": "Mozilla/5.0"})
    html = urllib.request.urlopen(request, timeout=120).read().decode()
    challenge_match = re.search(r'const POW_CHALLENGE = "([^"]+)"', html)
    difficulty_match = re.search(r'const POW_DIFFICULTY = "(\d+)"', html)
    if not challenge_match or not difficulty_match:
        raise AssertionError("PMC proof-of-work challenge absent")
    challenge = challenge_match.group(1)
    prefix = "0" * int(difficulty_match.group(1))
    nonce = 0
    while not hashlib.sha256((challenge + str(nonce)).encode()).hexdigest().startswith(prefix):
        nonce += 1
    opener = urllib.request.build_opener()
    opener.addheaders = [
        ("User-Agent", "Mozilla/5.0"),
        ("Cookie", f"cloudpmc-viewer-pow={challenge},{nonce}"),
        ("Referer", PMC_TABLE_URL),
    ]
    path.write_bytes(opener.open(PMC_TABLE_URL, timeout=120).read())
    if path.stat().st_size != PMC_TABLE_SIZE or sha256_file(path) != PMC_TABLE_SHA256:
        raise AssertionError("PMC Table S5 identity drift")


def read_lines(path: Path) -> list[str]:
    with gzip.open(path, "rt") as handle:
        return [line.rstrip("\n") for line in handle]


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "columns": list(map(str, frame.columns)),
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(frame.index),
        "index_sample": frame.index.astype(str)[:8].tolist(),
        "dtypes": {str(column): str(frame[column].dtype) for column in frame.columns},
        "non_null": {str(column): int(frame[column].notna().sum()) for column in frame.columns},
        "nunique": {str(column): int(frame[column].dropna().astype(str).nunique()) for column in frame.columns},
        "value_samples": {
            str(column): frame[column].dropna().astype(str).drop_duplicates().head(12).tolist()
            for column in frame.columns
        },
    }


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"newest Artifact is not latest: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve feature Artifact: {value}")
    return records[-1]


def collection_snapshot(ln: Any, obs_key: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for collection in ln.Collection.filter().all():
        key = str(collection.key)
        if not key.startswith("pert-gym/"):
            continue
        matches = list(collection.artifacts.filter(key=obs_key).only("uid", "key").all())
        if matches:
            result[key] = {
                "uid": str(collection.uid),
                "hash": str(collection.hash),
                "member_count": collection.artifacts.count(),
                "target_matches": [{"uid": str(item.uid), "key": str(item.key)} for item in matches],
            }
    return result


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse150062-sources"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name, (size, sha256) in SOURCE_FILES.items():
        path = root / name
        download(f"{GEO_ROOT}/{name}", path, size, sha256)
        paths[name] = path
    table_path = root / "mmc6.xlsx"
    download_pmc_table(table_path)

    barcodes = pd.Index(read_lines(paths["GSE150062_perturbseq_barcodes.tsv.gz"]), dtype="string")
    sgrna_barcodes = pd.Index(read_lines(paths["GSE150062_sgrna_barcodes.tsv.gz"]), dtype="string")
    genes = pd.Index(read_lines(paths["GSE150062_perturbseq_genes.tsv.gz"]), dtype="string")
    features = pd.Index(read_lines(paths["GSE150062_perturbseq_features.tsv.gz"]), dtype="string")
    sgrna_genes = pd.Index(read_lines(paths["GSE150062_sgrna_genes.tsv.gz"]), dtype="string")
    sgrna_features = pd.Index(read_lines(paths["GSE150062_sgrna_features.tsv.gz"]), dtype="string")
    table = pd.read_excel(table_path, sheet_name="TableS5", header=2, dtype="string")
    table = table.set_index("Cell barcode", drop=False)
    if len(table) != EXPECTED_N_OBS or not table.index.is_unique:
        raise AssertionError("Table S5 barcode denominator/uniqueness drift")
    if not barcodes.equals(sgrna_barcodes) or not barcodes.equals(table.index):
        raise AssertionError("source barcode order drift")
    if len(genes) != EXPECTED_N_VARS or features[1:].tolist() != genes.tolist():
        raise AssertionError("source expression feature axis drift")
    if (
        len(sgrna_genes) != 492
        or not sgrna_genes.is_unique
        or len(sgrna_features) != 493
        or sgrna_features[0] != "."
        or not sgrna_features[1:].is_unique
    ):
        raise AssertionError("source sgRNA feature axis drift")

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    obs_artifact, obs_history = latest_artifact(ln, EXPECTED["obs"]["key"])
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    if len(obs) != EXPECTED_N_OBS or len(var) != EXPECTED_N_VARS:
        raise AssertionError("accepted triplet denominator drift")
    x_path = Path(x_artifact.cache())
    backed = ad.read_h5ad(x_path, backed="r")
    try:
        x_summary = {
            "shape": list(backed.shape),
            "obs_names_sha256": ordered_sha256(backed.obs_names),
            "var_names_sha256": ordered_sha256(backed.var_names),
            "obs_names_equal_table": backed.obs_names.astype(str).equals(table.index.astype(str)),
            "var_names_equal_current_var": backed.var_names.astype(str).equals(var.index.astype(str)),
            "x_encoding": type(backed.X).__name__,
            "backed_only": True,
        }
    finally:
        backed.file.close()

    obs_candidates = {"obs_index": obs.index.astype(str)}
    for column in ("original_obs_index", "cell_id", "cell_barcode"):
        if column in obs:
            obs_candidates[column] = pd.Index(obs[column].astype(str))
    axis_relations = {
        name: {
            "unique": bool(values.is_unique),
            "ordered_equals_table_s5": values.equals(table.index.astype(str)),
            "set_equals_table_s5": len(values) == len(table) and set(values) == set(table.index.astype(str)),
            "sha256": ordered_sha256(values),
        }
        for name, values in obs_candidates.items()
    }
    stable = var["stable_feature_id"].astype("string") if "stable_feature_id" in var else pd.Series([], dtype="string")
    report = {
        "format": "pert-gym.gse150062-source-inspection/v1",
        "task_id": TASK_ID,
        "real_dataset_id": "geo/GSE150062",
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {"free_disk_bytes": capacity.free_disk_bytes, "available_memory_bytes": capacity.available_memory_bytes},
        "sources": {
            **{name: {"url": f"{GEO_ROOT}/{name}", "size": size, "sha256": sha256} for name, (size, sha256) in SOURCE_FILES.items()},
            "Table_S5_mmc6.xlsx": {"url": PMC_TABLE_URL, "size": PMC_TABLE_SIZE, "sha256": PMC_TABLE_SHA256},
        },
        "source_tables": {
            "barcodes": {"rows": len(barcodes), "unique": bool(barcodes.is_unique), "ordered_sha256": ordered_sha256(barcodes)},
            "genes": {"rows": len(genes), "unique": bool(genes.is_unique), "ordered_sha256": ordered_sha256(genes)},
            "sgrna_features": {
                "rows": len(sgrna_genes),
                "unique_gene_labels": bool(sgrna_genes.is_unique),
                "unique_library_labels": bool(sgrna_features[1:].is_unique),
                "gene_label_ordered_sha256": ordered_sha256(sgrna_genes),
                "library_label_ordered_sha256": ordered_sha256(sgrna_features[1:]),
                "note": "GEO exposes separate one-column gene and feature labels; they are independently bound, not asserted semantically identical",
            },
            "table_s5": frame_summary(table),
        },
        "current": {
            "obs": artifact_identity(obs_artifact),
            "obs_matches_card_prewrite_uid": str(obs_artifact.uid) == EXPECTED["obs"]["uid"],
            "obs_history": [artifact_identity(item) for item in obs_history],
            "obs_frame": frame_summary(obs),
            "x": artifact_identity(x_artifact),
            "x_axis": x_summary,
            "var": artifact_identity(var_artifact),
            "var_matches_card_prewrite_uid": str(var_artifact.uid) == EXPECTED["var"]["uid"],
            "var_frame": frame_summary(var),
            "var_uniqueness": {
                "index_unique": bool(var.index.is_unique),
                "stable_feature_id_present": "stable_feature_id" in var,
                "stable_feature_id_non_null": int(stable.notna().sum()),
                "stable_feature_id_unique_non_null": bool(stable.dropna().is_unique),
                "stable_feature_id_ensg": int(stable.str.fullmatch(r"ENSG\d{11}", na=False).sum()),
            },
            "links": {"obs_to_x": True, "x_to_var": True},
        },
        "axis_relations": axis_relations,
        "collection": collection_snapshot(ln, EXPECTED["obs"]["key"]),
        "invariants": {"writes": 0, "source_table_s5_rows": len(table), "physical_members": 1},
    }
    print("GSE150062_REPORT_JSON=" + canonical(report))


if __name__ == "__main__":
    main()
