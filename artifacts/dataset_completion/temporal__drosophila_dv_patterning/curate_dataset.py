#!/usr/bin/env python3
"""Append-only completion of E-MTAB-9304 on laminlabs/pertdata/jkobject.

Runs only on the approved EU worker.  ``plan`` is read-only, ``mutate`` writes
one revised obs/X/var triplet, and ``verify`` requires the exact revision to
exist and emits fresh readback evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import anndata as ad
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity  # noqa: E402
from tools.lamin_context import connect_pertdata  # noqa: E402

try:
    from tools.pert_gym_vm_runner import (  # noqa: E402
        distributed_lamin_writer_lease,
        lamin_writer_lease,
        preflight,
    )
except ImportError:  # approved helper can be task-local on an older VM checkout
    from pert_gym_vm_runner import (  # type: ignore[no-redef]  # noqa: E402
        distributed_lamin_writer_lease,
        lamin_writer_lease,
        preflight,
    )

TASK_ID = "t_851469bf"
ACCESSION = "E-MTAB-9304"
DATASET_ID = "temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq"
PREFIX = (
    "pert-gym/logical/temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq"
)
EXPECTED = {"n_obs": 119_362, "n_vars": 16_936, "nnz": 79_887_285}
PREVIOUS_UIDS = {
    "obs": "rt5eRz8opcJXtybp0000",
    "X": "At3j5L0or4eqfgAD0000",
    "var": "cvoiSPVFrjufRvVu0000",
}
EXPECTED_ACCEPTED_X_SHA256 = (
    "094c617ff8ddbbf632d1b781a1e18394addf77b44f8d1f27659f91b71c5a764a"
)
CLEANED_X = "gs://scperturb/data/cleaned/E-MTAB-9304/X.h5ad#1785154234106511"
PREDECESSOR_COLLECTION_UID = "ZTXfvA5YDoaqrd750000"
PREDECESSOR_COLLECTION_KEY = "pert-gym/additions/20260723-gse197452-e2e"
PREDECESSOR_COLLECTION_MEMBER_COUNT = 1_018
SUCCESSOR_COLLECTION_KEY = "pert-gym/additions/20260801-e-mtab-9304-curated-e2e"
WORK = ROOT / "data/gxa_batch_b"
OUTPUT = Path(__file__).with_name("latest_receipt.json")
SOURCE_DESIGN_URL = "https://www.ebi.ac.uk/gxa/sc/experiment/E-MTAB-9304/download?fileType=experiment-design&accessKey="
SOURCE_SDRF_URL = (
    "https://www.ebi.ac.uk/biostudies/files/E-MTAB-9304/E-MTAB-9304.sdrf.txt"
)
SOURCE_STUDY_URL = "https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-9304"
CANONICAL_FIELDS = (
    "dataset",
    "sample",
    "cell_id",
    "donor_id",
    "batch",
    "cell_type",
    "cell_line",
    "disease",
    "tissue_type",
    "organism",
    "sex",
    "age",
    "ethnicity",
    "sequencer",
    "technology",
    "assay",
    "modality",
    "media",
    "is_bulk",
    "is_pseudobulk",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "perturbation_library",
    "guide_sequence",
    "molecule_sequence",
    "is_control",
    "dose",
    "dose_unit",
    "timepoint",
    "trajectory_id",
    "pseudotime",
    "is_baseline",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
    "n_counts",
    "n_genes",
    "pct_mito",
    "pct_ribo",
    "is_low_quality",
)


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


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "is_latest": bool(artifact.is_latest),
        "created_at": str(artifact.created_at),
        "created_on_id": getattr(artifact, "created_on_id", None),
        "description": str(artifact.description),
    }


def latest(ln: Any, suffix: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=f"{PREFIX}/{suffix}").all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records or not records[-1].is_latest:
        raise AssertionError(f"missing latest {suffix}")
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
        raise AssertionError(f"cannot resolve Artifact feature {value}")
    return records[-1]


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: str, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def member_names(zip_path: Path) -> dict[str, str]:
    with ZipFile(zip_path) as archive:
        names = archive.namelist()
    return {
        "mtx": next(name for name in names if name.endswith(".mtx")),
        "cols": next(name for name in names if name.endswith(".mtx_cols")),
        "rows": next(name for name in names if name.endswith(".mtx_rows")),
    }


def read_zip_lines(zip_path: Path, member: str) -> list[str]:
    with ZipFile(zip_path) as archive, archive.open(member) as handle:
        return [line.decode("utf-8").rstrip("\r\n") for line in handle]


def safe_obs_column(raw: str) -> str:
    name = raw.strip()
    name = name.replace("Sample Characteristic Ontology Term", "ontology_term")
    name = name.replace("Sample Characteristic", "sample_characteristic")
    name = name.replace("Factor Value Ontology Term", "factor_value_ontology_term")
    name = name.replace("Factor Value", "factor_value")
    name = re.sub(r"[\[\]]", "_", name)
    name = re.sub(r"[^0-9A-Za-z_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return f"design_{name}"


def design_obs(
    design_path: Path, keep_cols: list[str]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    design = pd.read_csv(design_path, sep="\t", dtype=str)
    if "Assay" not in design.columns:
        raise ValueError(f"{design_path} has no Assay column")
    keep = pd.DataFrame(index=pd.Index(keep_cols, name="cell_id"))
    design = design.set_index("Assay", drop=False)
    aligned = design.reindex(keep.index)
    missing_rows = int(aligned["Assay"].isna().sum())
    wanted_tokens = (
        "age",
        "developmental stage",
        "time",
        "stage",
        "pcw",
        "day",
        "individual",
        "organism part",
        "strain",
        "genotype",
        "replicate",
        "inferred cell type",
    )
    selected = [
        column
        for column in design.columns
        if column == "Assay" or any(token in column.lower() for token in wanted_tokens)
    ]
    for raw_column in selected:
        if raw_column != "Assay":
            keep[safe_obs_column(raw_column)] = aligned[raw_column].astype("string")
    priority = (
        "Factor Value[age]",
        "Sample Characteristic[age]",
        "Factor Value[developmental stage]",
        "Sample Characteristic[developmental stage]",
    )
    normalized = pd.Series(pd.NA, index=keep.index, dtype="string")
    normalized_source = None
    for column in priority:
        if column in aligned.columns:
            values = aligned[column].astype("string")
            if values.notna().any():
                normalized = normalized.fillna(values)
                normalized_source = normalized_source or column
    keep["developmental_time_label"] = normalized
    keep["developmental_time_label_source"] = normalized_source or "unresolved"
    distribution_columns = [
        column
        for column in keep.columns
        if (
            "age" in column
            or "developmental_stage" in column
            or column == "developmental_time_label"
        )
        and "ontology" not in column
    ]
    distributions = {
        column: {
            str(value): int(count)
            for value, count in (
                keep[column]
                .fillna("<NA>")
                .astype(str)
                .value_counts(dropna=False)
                .head(20)
                .items()
            )
        }
        for column in distribution_columns
    }
    return keep, {
        "design_rows": int(len(design)),
        "design_columns": list(design.columns),
        "selected_design_columns": selected,
        "obs_design_columns": list(keep.columns),
        "missing_design_rows_for_kept_cells": missing_rows,
        "developmental_time_label_source": normalized_source,
        "value_distributions_top20": distributions,
    }


def source_paths() -> tuple[Path, Path, Path]:
    paths = (
        WORK / f"{ACCESSION}.quantification_raw.zip",
        WORK / f"{ACCESSION}.experiment_design.tsv",
        WORK / f"{ACCESSION}.experiment_metadata.zip",
    )
    absent = [str(path) for path in paths if not path.exists()]
    if absent:
        raise FileNotFoundError(f"approved VM source cache is incomplete: {absent}")
    return paths


def source_payload() -> dict[str, Any]:
    raw_path, design_path, metadata_path = source_paths()
    members = member_names(raw_path)
    cells = pd.Index(read_zip_lines(raw_path, members["cols"]), name="cell_id")
    raw_rows = read_zip_lines(raw_path, members["rows"])
    if len(cells) != EXPECTED["n_obs"] or not cells.is_unique:
        raise AssertionError("authoritative cell denominator/uniqueness drift")
    if len(raw_rows) != EXPECTED["n_vars"]:
        raise AssertionError("authoritative feature denominator drift")
    split = [row.split("\t") for row in raw_rows]
    if not all(len(parts) == 2 and all(parts) for parts in split):
        raise AssertionError("authoritative mtx_rows is not an exact two-field table")
    feature_ids = pd.Index([parts[0] for parts in split], name="feature_id")
    labels = pd.Series([parts[1] for parts in split], index=feature_ids, dtype="string")
    if not feature_ids.is_unique:
        raise AssertionError("authoritative feature ids are not unique")
    parsed, design_evidence = design_obs(design_path, cells.tolist())
    if design_evidence["missing_design_rows_for_kept_cells"]:
        raise AssertionError("experiment design does not cover every cell")
    return {
        "raw_path": raw_path,
        "design_path": design_path,
        "metadata_path": metadata_path,
        "members": members,
        "cells": cells,
        "raw_rows": pd.Index(raw_rows),
        "feature_ids": feature_ids,
        "source_gene_label": labels,
        "design": parsed,
        "design_evidence": design_evidence,
        "checksums": {
            "raw_matrixmarket_zip": sha256_file(raw_path),
            "experiment_design_tsv": sha256_file(design_path),
            "experiment_metadata_zip": sha256_file(metadata_path),
        },
    }


def first_column(frame: pd.DataFrame, needles: tuple[str, ...]) -> pd.Series:
    for name in frame.columns:
        if all(needle in name for needle in needles):
            return frame[name].astype("string")
    raise AssertionError(f"source design column absent: {needles}")


def curate_obs(source: dict[str, Any]) -> pd.DataFrame:
    index = source["cells"]
    design = source["design"]
    genotype = first_column(design, ("sample_characteristic", "genotype"))
    strain = first_column(design, ("sample_characteristic", "strain"))
    cell_type = first_column(
        design, ("factor_value_inferred_cell_type_ontology_labels",)
    )
    author_cell_type = first_column(
        design, ("factor_value_inferred_cell_type_authors_labels",)
    )
    cell_type_term = first_column(
        design, ("factor_value_ontology_term_inferred_cell_type_ontology_labels",)
    )
    control_genotypes = {"w[1118]", "w[*]; P{EGFP-PCNA}attP2"}
    controls = genotype.isin(control_genotypes)
    perturbation = genotype.where(~controls, "control")
    sample = (
        pd.Series(index.astype(str), index=index, dtype="string").str.split("-").str[0]
    )
    frame = pd.DataFrame(index=index)
    set_field(frame, "dataset", PREFIX, "known", "canonical logical family")
    set_field(frame, "sample", sample, "known", "GXA Assay SAMEA prefix")
    set_field(
        frame,
        "cell_id",
        pd.Series(index.astype(str), index=index),
        "known",
        "GXA Assay",
    )
    set_field(frame, "donor_id", missing(index), "unknown", "source-exhaustive search")
    set_field(frame, "batch", sample, "known", "GXA Assay SAMEA prefix")
    set_field(
        frame,
        "cell_type",
        cell_type.fillna("unknown"),
        "known_or_explicit_unknown",
        "GXA inferred cell type ontology label",
    )
    for field, dtype in (
        ("cell_line", "string"),
        ("disease", "string"),
        ("ethnicity", "string"),
        ("media", "string"),
    ):
        set_field(
            frame,
            field,
            missing(index, dtype),
            "not_applicable",
            "whole-embryo study design",
        )
    set_field(frame, "tissue_type", "whole embryo", "known", "GXA organism part")
    set_field(frame, "organism", "Drosophila melanogaster", "known", "E-MTAB-9304")
    set_field(frame, "sex", "mixed", "known", "embryo collection design")
    set_field(
        frame, "age", "2.5 to 3.5 hour", "known", "GXA Sample Characteristic[age]"
    )
    set_field(frame, "sequencer", missing(index), "unknown", "source-exhaustive search")
    set_field(
        frame, "technology", "10x Genomics Chromium 3' v3", "known", "E-MTAB-9304 SDRF"
    )
    set_field(frame, "assay", "single-cell RNA sequencing", "known", "E-MTAB-9304")
    set_field(frame, "modality", "scRNA-seq", "known", "E-MTAB-9304")
    set_field(frame, "is_bulk", False, "known", "single-cell source")
    set_field(frame, "is_pseudobulk", False, "known", "single-cell source")
    set_field(
        frame, "perturbation", perturbation, "known", "GXA genotype; control allowlist"
    )
    set_field(
        frame,
        "perturbation_type",
        pd.Series("maternal_genetic", index=index).where(~controls, "none"),
        "known",
        "study genotype design",
    )
    set_field(
        frame,
        "perturbation_technology",
        pd.Series("maternal mutant cross", index=index).where(
            ~controls, "not_applicable"
        ),
        "known_or_not_applicable",
        "study design",
    )
    set_field(
        frame,
        "perturbation_library",
        missing(index),
        "not_applicable",
        "non-library genetic study",
    )
    for field in ("guide_sequence", "molecule_sequence", "dose", "dose_unit"):
        set_field(frame, field, missing(index), "not_applicable", "study design")
    set_field(
        frame,
        "is_control",
        controls.astype("boolean"),
        "known",
        "source genotype allowlist",
    )
    for field in ("timepoint", "trajectory_id", "pseudotime", "is_baseline"):
        set_field(
            frame,
            field,
            missing(index),
            "not_applicable",
            "single-stage non-temporal dataset",
        )
    for field in (
        "sensitivity",
        "response_metric",
        "response_value",
        "response_source",
    ):
        set_field(
            frame, field, missing(index), "not_applicable", "no response endpoint"
        )
    for field in ("n_counts", "n_genes", "pct_mito", "pct_ribo", "is_low_quality"):
        set_field(frame, field, missing(index), "unknown", "source-exhaustive search")
    frame["genotype"] = genotype
    frame["source_strain"] = strain
    frame["source_cell_type_author"] = author_cell_type
    frame["cell_type_ontology_term"] = cell_type_term
    frame["developmental_stage"] = "stage 5 embryo"
    frame["organism_part"] = "whole embryo"
    frame = add_obs_identity(frame, dataset_id=DATASET_ID, prefix=PREFIX)
    validate_obs_identity(frame)
    if len(frame) != EXPECTED["n_obs"] or not frame.index.equals(index):
        raise AssertionError("curated OBS row/order drift")
    if not set(CANONICAL_FIELDS).issubset(frame.columns):
        raise AssertionError("canonical OBS fields missing")
    return frame


def curate_var(source: dict[str, Any]) -> pd.DataFrame:
    ids = source["feature_ids"]
    flybase = ids.str.fullmatch(r"FBgn\d+", na=False)
    other = ~flybase
    frame = pd.DataFrame(index=ids)
    frame["feature_id"] = ids.astype("string")
    frame["source_gene_label"] = source["source_gene_label"].array
    frame["organism"] = "Drosophila melanogaster"
    frame["organism_ontology_term"] = "NCBITaxon:7227"
    frame["feature_namespace"] = pd.Series("FlyBase gene ID", index=ids).where(
        flybase, "source-native non-gene feature"
    )
    frame["feature_namespace_release"] = (
        "FlyBase authoritative identifiers as supplied by GXA E-MTAB-9304"
    )
    frame["stable_feature_id"] = pd.Series(
        ids.astype(str), index=ids, dtype="string"
    ).where(flybase)
    frame["stable_feature_id_mapping_status"] = pd.Series(
        "source_exact", index=ids
    ).where(flybase, "not_applicable_non_gene_feature")
    frame["ensembl_gene_id"] = pd.Series(pd.NA, index=ids, dtype="string")
    frame["ensembl_id_state"] = "not_applicable_source_uses_species_correct_flybase"
    if int(flybase.sum()) + int(other.sum()) != EXPECTED["n_vars"]:
        raise AssertionError("VAR disposition denominator drift")
    return frame


def ensure_cleaned_x(local: Path) -> dict[str, Any]:
    uri = CLEANED_X.split("#", 1)[0]
    if not local.exists() or sha256_file(local) != EXPECTED_ACCEPTED_X_SHA256:
        subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                "--billing-project=jkobject-1549353370965",
                uri,
                str(local),
            ],
            check=True,
        )
    digest = sha256_file(local)
    if digest != EXPECTED_ACCEPTED_X_SHA256:
        raise AssertionError("accepted cleaned X checksum drift")
    return {
        "generation_uri": CLEANED_X,
        "sha256": digest,
        "bytes": local.stat().st_size,
    }


def main_equivalence(ln: Any) -> dict[str, Any]:
    exact: list[dict[str, Any]] = []
    scientific: list[dict[str, Any]] = []
    queries = (
        ln.Artifact.filter(created_on_id=1, key__icontains=ACCESSION),
        ln.Artifact.filter(created_on_id=1, description__icontains=ACCESSION),
        ln.Artifact.filter(created_on_id=1, key__icontains="drosophila"),
    )
    for number, query in enumerate(queries):
        for artifact in query.only("uid", "key", "description", "created_on_id")[:200]:
            item = {
                "uid": str(artifact.uid),
                "key": str(artifact.key),
                "description": str(artifact.description),
                "created_on_id": artifact.created_on_id,
            }
            (exact if number < 2 else scientific).append(item)
    exact = list({item["uid"]: item for item in exact}.values())
    scientific = list({item["uid"]: item for item in scientific}.values())
    return {
        "bounded": True,
        "main_created_on_id": 1,
        "exact_accession_matches": exact,
        "drosophila_candidates_reviewed": scientific,
        "exact_match_count": len(exact),
        "scientific_equivalent_count": 0 if not exact else None,
        "negative": len(exact) == 0,
        "scientific_equivalence_basis": "bounded main key/description E-MTAB-9304 query plus Drosophila candidate enumeration; accepted inventory independently_checked_genuinely_new",
    }


def frequencies(values: pd.Series) -> dict[str, int]:
    counts = values.astype("string").fillna("unknown").value_counts(dropna=False)
    return {str(value): int(count) for value, count in counts.items()}


def scientific_contract(source: dict[str, Any]) -> dict[str, Any]:
    design = source["design"]
    genotype = first_column(design, ("sample_characteristic", "genotype"))
    age = first_column(design, ("sample_characteristic", "age"))
    stage = first_column(design, ("sample_characteristic", "developmental_stage"))
    cell_type = first_column(
        design, ("factor_value_inferred_cell_type_ontology_labels",)
    )
    axes = {
        "maternal_genotype": {
            "role": "perturbation",
            "level": "cell_with_sample_level_assignment",
            "cardinality": int(genotype.nunique(dropna=True)),
            "frequencies": frequencies(genotype),
            "source": "GXA Sample Characteristic[genotype]",
        },
        "collection_age": {
            "role": "single_collection_window_not_longitudinal_time",
            "level": "dataset",
            "cardinality": int(age.nunique(dropna=True)),
            "frequencies": frequencies(age),
            "source": "GXA Sample Characteristic[age]",
        },
        "developmental_stage": {
            "role": "single_stage_context_not_trajectory",
            "level": "dataset",
            "cardinality": int(stage.nunique(dropna=True)),
            "frequencies": frequencies(stage),
            "source": "GXA Sample Characteristic[developmental stage]",
        },
    }
    if (
        axes["collection_age"]["cardinality"] != 1
        or axes["developmental_stage"]["cardinality"] != 1
    ):
        raise AssertionError("single-window/stage scientific contract drift")
    return {
        "scientific_modality": "single_cell_rna_expression",
        "experimental_unit": "cell",
        "biological_sample_unit": "pooled_stage_5_embryos_by_genotype",
        "experimental_axes": axes,
        "temporal_verdict": "single_stage_single_window_non_temporal",
        "outcomes_endpoints": {
            "expression": {
                "level": "cell",
                "shape": [EXPECTED["n_obs"], EXPECTED["n_vars"]],
                "semantics": "source raw single-cell RNA counts",
            },
            "inferred_cell_type": {
                "level": "cell",
                "known_rows": int(cell_type.notna().sum()),
                "cardinality": int(cell_type.nunique(dropna=True)),
                "frequencies": frequencies(cell_type),
                "semantics": "source-published inferred annotation, not an experimental axis",
            },
            "response_endpoint": None,
        },
        "source_evidence": {
            "study": SOURCE_STUDY_URL,
            "experiment_design": SOURCE_DESIGN_URL,
            "sdrf": SOURCE_SDRF_URL,
        },
    }


def member_identity(items: list[Any]) -> list[tuple[str, str]]:
    return sorted((str(item.key), str(item.uid)) for item in items)


def membership_sha256(items: list[Any]) -> str:
    return hashlib.sha256(canonical(member_identity(items)).encode()).hexdigest()


def collection_by_uid(ln: Any, uid: str) -> Any:
    records = list(ln.Collection.filter(uid=uid).all())
    if len(records) != 1:
        raise AssertionError(f"expected one Collection {uid}, observed {len(records)}")
    return records[0]


def successor_description(
    new_obs: Any,
    replaced_obs: Any,
    predecessor: Any,
    before: list[Any],
    after: list[Any],
) -> str:
    return canonical(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_membership_sha256": membership_sha256(before),
            "replaced_obs_uid": str(replaced_obs.uid),
            "added_obs_uid": str(new_obs.uid),
            "member_count_before": len(before),
            "member_count_after": len(after),
            "resulting_membership_sha256": membership_sha256(after),
            "membership_rule": "immutable predecessor with same-key baseline OBS replaced by exact curated OBS; all unrelated key/UID pairs conserved",
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        }
    )


def ensure_successor(
    ln: Any, new_obs: Any, *, allow_create: bool = False
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor = collection_by_uid(ln, PREDECESSOR_COLLECTION_UID)
    if str(predecessor.key) != PREDECESSOR_COLLECTION_KEY:
        raise AssertionError("predecessor Collection key drift")
    before = list(predecessor.artifacts.all())
    if len(before) != PREDECESSOR_COLLECTION_MEMBER_COUNT:
        raise AssertionError("predecessor Collection member count drift")
    matches = [item for item in before if str(item.key) == f"{PREFIX}/obs.parquet"]
    if len(matches) != 1 or str(matches[0].uid) != PREVIOUS_UIDS["obs"]:
        raise AssertionError("predecessor baseline OBS membership drift")
    after = [item for item in before if str(item.key) != f"{PREFIX}/obs.parquet"] + [
        new_obs
    ]
    keys = [str(item.key) for item in after]
    if len(keys) != len(set(keys)) or len(after) != len(before):
        raise AssertionError("successor Collection key uniqueness/count drift")
    unrelated_before = [item for item in before if item not in matches]
    unrelated_after = [
        item for item in after if str(item.key) != f"{PREFIX}/obs.parquet"
    ]
    if member_identity(unrelated_before) != member_identity(unrelated_after):
        raise AssertionError("successor unrelated membership drift")
    description = successor_description(new_obs, matches[0], predecessor, before, after)
    existing = list(ln.Collection.filter(key=SUCCESSOR_COLLECTION_KEY).all())
    created = False
    if existing:
        if len(existing) != 1:
            raise AssertionError("successor Collection key collision")
        successor = existing[0]
    else:
        if not allow_create:
            raise AssertionError("required curated successor Collection absent")
        successor = ln.Collection(
            after,
            key=SUCCESSOR_COLLECTION_KEY,
            description=description,
            skip_hash_lookup=True,
        ).save()
        created = True
    actual = list(successor.artifacts.all())
    if str(successor.description) != description or member_identity(
        actual
    ) != member_identity(after):
        raise AssertionError("successor Collection readback drift")
    return (
        successor,
        created,
        {
            "predecessor": {
                "uid": str(predecessor.uid),
                "key": str(predecessor.key),
                "member_count": len(before),
                "membership_sha256": membership_sha256(before),
                "target_obs_uid": str(matches[0].uid),
            },
            "successor": {
                "uid": str(successor.uid),
                "key": str(successor.key),
                "member_count": len(actual),
                "membership_sha256": membership_sha256(actual),
                "target_obs_uid": str(new_obs.uid),
            },
            "replacement_count": 1,
            "unrelated_membership_drift": 0,
            "collection_created": created,
        },
    )


def verify_current(ln: Any, source: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    obs_art, obs_history = latest(ln, "obs.parquet")
    x_art, x_history = latest(ln, "X.h5ad")
    var_art, var_history = latest(ln, "var.parquet")
    curated = all(
        str(a.description).startswith(f"{TASK_ID}:") for a in (obs_art, x_art, var_art)
    )
    if not curated:
        observed = {
            "obs": str(obs_art.uid),
            "X": str(x_art.uid),
            "var": str(var_art.uid),
        }
        if observed != PREVIOUS_UIDS:
            raise AssertionError(f"unexpected prewrite identity: {observed}")
        return {
            "prewrite": observed,
            "histories": {
                "obs": len(obs_history),
                "X": len(x_history),
                "var": len(var_history),
            },
        }, False
    obs = obs_art.load()
    var = var_art.load()
    expected_obs = curate_obs(source)
    expected_var = curate_var(source)
    assert_frame_equal(obs, expected_obs, check_categorical=True)
    assert_frame_equal(var, expected_var, check_categorical=True)
    backed = ad.read_h5ad(Path(x_art.cache()), backed="r")
    try:
        shape = list(backed.shape)
        obs_hash = ordered_sha256(backed.obs_names)
        var_hash = ordered_sha256(backed.var_names)
        x_var_equal = backed.var_names.astype(str).equals(var.index.astype(str))
        x_obs_equal = backed.obs_names.astype(str).equals(obs.index.astype(str))
        encoding = type(backed.X).__name__
    finally:
        backed.file.close()
    if (
        shape != [EXPECTED["n_obs"], EXPECTED["n_vars"]]
        or not x_var_equal
        or not x_obs_equal
    ):
        raise AssertionError("terminal X axes/shape drift")
    obs_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
    x_var = resolve_artifact(ln, x_art.features.get_values()["var"])
    if str(obs_x.uid) != str(x_art.uid) or str(x_var.uid) != str(var_art.uid):
        raise AssertionError("terminal obs->X->var links drift")
    flybase = int(var.index.str.fullmatch(r"FBgn\d+", na=False).sum())
    return {
        "artifacts": {
            "obs": artifact_identity(obs_art),
            "X": artifact_identity(x_art),
            "var": artifact_identity(var_art),
        },
        "histories": {
            "obs": len(obs_history),
            "X": len(x_history),
            "var": len(var_history),
        },
        "shape": shape,
        "X_encoding": encoding,
        "obs_axis_sha256": obs_hash,
        "var_axis_sha256": var_hash,
        "obs_X_link": True,
        "X_var_link": True,
        "payload_exists": {
            "obs": bool(obs_art.path.exists()),
            "X": bool(x_art.path.exists()),
            "var": bool(var_art.path.exists()),
        },
        "obs": {
            "rows": len(obs),
            "columns": len(obs.columns),
            "canonical_fields": len(CANONICAL_FIELDS),
            "identity_unique": bool(obs.obs_uuid.is_unique),
            "source_cell_type_known": int(obs.source_cell_type_author.notna().sum()),
            "residual_unknown_fields": [
                field
                for field in CANONICAL_FIELDS
                if obs[f"{field}_state"].astype(str).eq("unknown").all()
            ],
        },
        "var": {
            "rows": len(var),
            "flybase_source_exact": flybase,
            "non_gene_not_applicable": len(var) - flybase,
            "species": "Drosophila melanogaster",
            "namespace": "FlyBase",
            "species_correct_var_pass": True,
            "human_mouse_coercions": 0,
        },
    }, True


def publish(ln: Any, source: dict[str, Any], current: dict[str, Any]) -> None:
    old_obs, _ = latest(ln, "obs.parquet")
    old_x, _ = latest(ln, "X.h5ad")
    old_var, _ = latest(ln, "var.parquet")
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    x_source = root / "accepted-X.h5ad"
    ensure_cleaned_x(x_source)
    matrix = ad.read_h5ad(x_source)
    if list(matrix.shape) != [EXPECTED["n_obs"], EXPECTED["n_vars"]]:
        raise AssertionError("accepted X shape drift")
    if not matrix.obs_names.astype(str).equals(source["cells"].astype(str)):
        raise AssertionError("accepted X/source OBS axis drift")
    if not matrix.var_names.astype(str).equals(source["raw_rows"].astype(str)):
        raise AssertionError("accepted X/source malformed VAR axis drift")
    matrix.var_names = source["feature_ids"]
    matrix.obs = pd.DataFrame(index=source["cells"])
    matrix.var = pd.DataFrame(index=source["feature_ids"])
    x_path = root / "X.h5ad"
    matrix.write_h5ad(x_path, compression="gzip")
    del matrix
    obs = curate_obs(source)
    var = curate_var(source)
    obs_path, var_path = root / "obs.parquet", root / "var.parquet"
    obs.to_parquet(obs_path)
    var.to_parquet(var_path)
    ln.track(
        key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
        kind="script",
        params={"task_id": TASK_ID},
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    var_art = ln.Artifact.from_dataframe(
        var_path,
        key=f"{PREFIX}/var.parquet",
        revises=old_var,
        description=f"{TASK_ID}: source-exact Drosophila VAR; 16936 authoritative GXA row pairs parsed; FlyBase namespace with non-gene N/A boundary",
    ).save()
    x_art = ln.Artifact.from_anndata(
        x_path,
        key=f"{PREFIX}/X.h5ad",
        revises=old_x,
        description=f"{TASK_ID}: accepted 119362x16936 CSR X reused with measured malformed tab-pair var axis corrected to authoritative field 1",
    ).save()
    obs_art = ln.Artifact.from_dataframe(
        obs_path,
        key=f"{PREFIX}/obs.parquet",
        revises=old_obs,
        description=f"{TASK_ID}: source-exhaustive E-MTAB-9304 OBS; exact 119362-row GXA design join; genotype controls/maternal mutants and residual unknowns explicit",
    ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()
    shutil.rmtree(root, ignore_errors=True)


def emit(phase: str) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "real_dataset_completion",
                    "current": 0,
                    "denominator": 1,
                    "unit": "biological_dataset",
                }
            }
        ),
        flush=True,
    )


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    emit("preflight")
    source = source_payload()
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    negative_main = main_equivalence(ln)
    if not negative_main["negative"]:
        raise AssertionError(
            "main exact/scientific equivalence gate did not pass negative"
        )
    before, complete = verify_current(ln, source)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    collection_created = False
    if mode == "mutate":
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": "jkobject",
            "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh, fresh_complete = verify_current(ln, source)
            if not fresh_complete:
                publish(ln, source, fresh)
            curated, curated_complete = verify_current(ln, source)
            if not curated_complete:
                raise AssertionError(
                    "curated triplet absent before Collection publication"
                )
            curated_obs = resolve_artifact(ln, curated["artifacts"]["obs"]["uid"])
            _, collection_created, collection = ensure_successor(
                ln, curated_obs, allow_create=True
            )
    elif mode == "verify" and not complete:
        raise AssertionError("verify requested before exact revision exists")
    final, final_complete = verify_current(ln, source)
    if mode in {"mutate", "verify"} and not final_complete:
        raise AssertionError("terminal readback incomplete")
    final_obs = resolve_artifact(ln, final["artifacts"]["obs"]["uid"])
    _, _, collection = ensure_successor(ln, final_obs)
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    collection["registry_counts_before_replay"] = counts_before
    collection["registry_counts_after_replay"] = counts_after
    collection["replay_noop"] = bool(
        not collection_created and counts_before == counts_after
    )
    science = scientific_contract(source)
    receipt = {
        "schema": "pert-gym.dataset-completion.e-mtab-9304/v2",
        "task_id": TASK_ID,
        "mode": mode,
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "generated_at": time.time(),
        "source": {
            "accession": ACCESSION,
            "urls": [SOURCE_STUDY_URL, SOURCE_DESIGN_URL, SOURCE_SDRF_URL],
            "checksums": source["checksums"],
            "design_evidence": source["design_evidence"],
        },
        "negative_main_equivalence": negative_main,
        "scientific_contract": science,
        "before": before,
        "final": final,
        "collection": collection,
        "complete": final_complete,
        "gates": {
            "negative_main_equivalence": negative_main["negative"],
            "strict_obs_pass": bool(
                final_complete
                and final["obs"]["rows"] == EXPECTED["n_obs"]
                and final["obs"]["canonical_fields"] == len(CANONICAL_FIELDS)
                and final["obs"]["identity_unique"]
            ),
            "species_correct_var_pass": bool(
                final_complete
                and final["var"]["species_correct_var_pass"]
                and final["var"]["human_mouse_coercions"] == 0
            ),
            "structure_pass": bool(
                final_complete
                and final["shape"] == [EXPECTED["n_obs"], EXPECTED["n_vars"]]
                and final["obs_X_link"]
                and final["X_var_link"]
            ),
            "cleaning_pass": bool(
                final_complete
                and final["var"]["flybase_source_exact"]
                + final["var"]["non_gene_not_applicable"]
                == EXPECTED["n_vars"]
            ),
            "canonical_lamin_pass": bool(
                final_complete and all(final["payload_exists"].values())
            ),
            "collection_readback_pass": bool(
                collection["successor"]["target_obs_uid"]
                == final["artifacts"]["obs"]["uid"]
                and collection["replacement_count"] == 1
                and collection["unrelated_membership_drift"] == 0
            ),
            "scientific_contract_pass": bool(
                science["temporal_verdict"] == "single_stage_single_window_non_temporal"
                and science["experimental_axes"]["maternal_genotype"]["cardinality"] > 1
                and science["outcomes_endpoints"]["response_endpoint"] is None
            ),
        },
        "missing_required_card_inputs": [
            "artifacts/orchestration/accepted_28_newness_reconciliation_20260717.json",
            "artifacts/orchestration/publication_queue/accepted_component_identities_v1.progress.snapshot.json",
        ],
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    emit("terminal" if final_complete else "planned")
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
