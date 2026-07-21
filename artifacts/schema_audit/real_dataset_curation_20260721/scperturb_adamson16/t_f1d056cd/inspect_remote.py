#!/usr/bin/env python3
"""Metadata-only source/live OBS inspection for scperturb/adamson16."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import platform
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_f1d056cd"
DATASET_ID = "scperturb/adamson16"
COMPONENTS = {
    "scperturb/adamson16_GSM2406675_10X001": {
        "gsm": "GSM2406675",
        "experiment": "GSM2406675_10X001",
        "n_obs": 5768,
        "title": "Pilot TF experiment cells",
        "biosample": "SAMN06055348",
        "experiment_accession": "SRX2400165",
        "sequencer": "Illumina HiSeq 2500",
    },
    "scperturb/adamson16_GSM2406677_10X005": {
        "gsm": "GSM2406677",
        "experiment": "GSM2406677_10X005",
        "n_obs": 15006,
        "title": "Epistasis experiment cells",
        "biosample": "SAMN06055346",
        "experiment_accession": "SRX2400167",
        "sequencer": "Illumina HiSeq 4000",
    },
    "scperturb/adamson16_GSM2406681_10X010": {
        "gsm": "GSM2406681",
        "experiment": "GSM2406681_10X010",
        "n_obs": 65337,
        "title": "UPR Perturb-seq experiment cells",
        "biosample": "SAMN06055342",
        "experiment_accession": "SRX2400171",
        "sequencer": "Illumina HiSeq 4000",
    },
}
URLS = {
    "converter": "https://raw.githubusercontent.com/sanderlab/scPerturb/master/dataset_processing/scripts/AdamsonWeissman2016.py",
    "paper_html": "https://pmc.ncbi.nlm.nih.gov/articles/PMC5315571/",
    "paper_table_s1": "https://pmc.ncbi.nlm.nih.gov/articles/instance/5315571/bin/NIHMS832990-supplement-10.xlsx",
    "geo_soft": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE90nnn/GSE90546/soft/GSE90546_family.soft.gz",
    "ena_report": "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=PRJNA354963&result=read_experiment&fields=study_accession,sample_accession,experiment_accession,run_accession,scientific_name,instrument_model,library_name,library_strategy,library_source,library_selection,experiment_title,sample_title&format=tsv",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch(name: str, url: str, root: Path) -> dict[str, Any]:
    suffix = Path(urllib.parse.urlparse(url).path).suffix or ".data"
    path = root / f"{name}{suffix}"
    request = urllib.request.Request(
        url, headers={"User-Agent": "pert-gym-metadata-audit/1"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:  # nosec B310
        data = response.read()
        headers = {key.lower(): value for key, value in response.headers.items()}
        status = getattr(response, "status", None)
        final_url = response.geturl()
    path.write_bytes(data)
    return {
        "path": str(path),
        "url": url,
        "final_url": final_url,
        "http_status": status,
        "bytes": len(data),
        "sha256": sha256(path),
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "content_type": headers.get("content-type"),
    }


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": int(frame.shape[0]),
        "columns": [str(column) for column in frame.columns],
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sample": frame.index.astype(str)[:5].tolist(),
        "dtypes": {str(column): str(dtype) for column, dtype in frame.dtypes.items()},
        "non_null": {
            str(column): int(frame[column].notna().sum()) for column in frame.columns
        },
        "nunique": {
            str(column): int(frame[column].nunique(dropna=True))
            for column in frame.columns
        },
        "value_samples": {
            str(column): frame[column]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .head(15)
            .tolist()
            for column in frame.columns
        },
    }


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": artifact.uid,
        "key": artifact.key,
        "version": getattr(artifact, "version", None),
        "hash": getattr(artifact, "hash", None),
        "size": getattr(artifact, "size", None),
        "description": getattr(artifact, "description", None),
        "created_at": str(getattr(artifact, "created_at", None)),
        "run_uid": getattr(getattr(artifact, "run", None), "uid", None),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        try:
            return ln.Artifact.get(uid=value)
        except Exception:
            return ln.Artifact.get(key=value)
    return value


def main() -> None:
    capacity = preflight()
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-adamson16-inspect-"))
    sources: dict[str, Any] = {}
    for name, url in URLS.items():
        sources[name] = fetch(name, url, root)

    for spec in COMPONENTS.values():
        for suffix in ("barcodes.tsv.gz", "cell_identities.csv.gz"):
            file_name = f"{spec['experiment']}_{suffix}"
            encoded = urllib.parse.quote(file_name)
            url = (
                f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={spec['gsm']}"
                f"&format=file&file={encoded}"
            )
            sources[f"{spec['experiment']}:{suffix}"] = fetch(
                f"{spec['experiment']}-{suffix.replace('.', '_')}", url, root
            )

    source_tables: dict[str, Any] = {}
    ena = pd.read_csv(sources["ena_report"]["path"], sep="\t")
    source_tables["ena_report"] = frame_summary(ena)
    table_s1_path = Path(sources["paper_table_s1"]["path"])
    if table_s1_path.read_bytes()[:2] == b"PK":
        table_s1 = pd.ExcelFile(table_s1_path)
        source_tables["paper_table_s1"] = {
            "status": "parsed",
            "sheet_names": table_s1.sheet_names,
            "sheets": {
                sheet: frame_summary(pd.read_excel(table_s1, sheet_name=sheet))
                for sheet in table_s1.sheet_names
            },
        }
    else:
        source_tables["paper_table_s1"] = {
            "status": "download_wrapper_not_workbook",
            "content_type": sources["paper_table_s1"]["content_type"],
            "bytes": sources["paper_table_s1"]["bytes"],
            "sha256": sources["paper_table_s1"]["sha256"],
        }

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise RuntimeError("wrong Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise RuntimeError("wrong Lamin branch")

    members: dict[str, Any] = {}
    all_obs_uuids: list[str] = []
    for prefix, spec in COMPONENTS.items():
        key = f"{prefix}/obs.parquet"
        records = list(ln.Artifact.filter(key=key).all())
        if not records:
            raise RuntimeError(f"missing OBS artifacts for {key}")
        records.sort(
            key=lambda artifact: (
                str(getattr(artifact, "created_at", "")),
                artifact.uid,
            )
        )
        current = records[-1]
        if not bool(getattr(current, "is_latest", False)):
            raise RuntimeError(f"newest ordered OBS is not latest for {key}")
        frame = current.load()
        if not isinstance(frame, pd.DataFrame):
            raise RuntimeError(f"OBS load did not return DataFrame for {key}")
        if len(frame) != spec["n_obs"]:
            raise RuntimeError(f"row mismatch for {key}: {len(frame)}")
        features = current.features.get_values()
        x_artifact = resolve_artifact(ln, features["X"])
        x_features = x_artifact.features.get_values()
        var_artifact = resolve_artifact(ln, x_features["var"])
        original = pd.read_csv(
            sources[f"{spec['experiment']}:barcodes.tsv.gz"]["path"],
            sep="\t",
            header=None,
            names=["raw_barcode"],
        )
        identities = pd.read_csv(
            sources[f"{spec['experiment']}:cell_identities.csv.gz"]["path"],
            index_col=0,
        )
        source_tables[spec["experiment"]] = {
            "barcodes": frame_summary(original),
            "cell_identities": frame_summary(identities),
            "cell_identity_index_without_dash_unique": bool(
                pd.Index(identities.index.astype(str).str.split("-").str[0]).is_unique
            ),
        }
        uuid_values = frame.get("obs_uuid", pd.Series(dtype=str)).astype(str).tolist()
        all_obs_uuids.extend(uuid_values)
        members[prefix] = {
            "artifact_key": key,
            "history": [artifact_identity(artifact) for artifact in records],
            "current": artifact_identity(current),
            "obs": frame_summary(frame),
            "obs_index_sha256": hashlib.sha256(
                "\n".join(frame.index.astype(str)).encode()
            ).hexdigest(),
            "obs_uuid_sha256": hashlib.sha256(
                "\n".join(uuid_values).encode()
            ).hexdigest(),
            "original_obs_index_sha256": hashlib.sha256(
                "\n".join(frame["original_obs_index"].astype(str)).encode()
            ).hexdigest(),
            "links": {
                "X": artifact_identity(x_artifact),
                "var": artifact_identity(var_artifact),
            },
            "source_spec": spec,
        }

    report = {
        "format": "pert-gym.adamson16-source-exhaustive-inspection/v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "source_objects": sources,
        "source_tables": source_tables,
        "members": members,
        "invariants": {
            "component_count": len(members),
            "total_rows": sum(item["obs"]["rows"] for item in members.values()),
            "all_obs_uuid_unique": len(all_obs_uuids)
            == len(set(all_obs_uuids))
            == 86111,
            "no_x_payload_loaded": True,
        },
    }
    encoded = base64.b64encode(
        gzip.compress(json.dumps(report, sort_keys=True).encode("utf-8"), mtime=0)
    ).decode("ascii")
    print("ADAMSON16_REPORT_GZIP_BASE64_BEGIN")
    print(encoded)
    print("ADAMSON16_REPORT_END")


if __name__ == "__main__":
    main()
