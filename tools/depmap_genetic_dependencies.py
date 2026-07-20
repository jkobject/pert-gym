#!/usr/bin/env python3
"""DepMap genetic essentiality source resolution and converters.

This module intentionally does not connect to Lamin at import time.  DepMap
CRISPR/Chronos/Avana payloads are pooled
essentiality screen readouts over model/cell-line × perturbation-gene pairs.
Their canonical pert-gym representation is an **obs + var only** essentiality
artifact:

    obs.parquet  one row per model/cell-line × perturbation/readout observation
    var.parquet  perturbation gene metadata keyed by stable gene IDs

Dependency or gene-effect scores live in obs columns such as ``effect_score``,
``dependency_score``, and the normalized ``score``/``score_type`` pair.  They
must not be written as a fake expression ``X.h5ad``.  Matched baseline RNA is a
separate expression artifact with its own obs and true expression matrix in
``.X``; join fields here make that split explicit.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from io import StringIO
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx
import pandas as pd

DEPMAP_DOWNLOADS_API = "https://depmap.org/portal/api/download/files"
DEPMAP_GENETIC_RELEASE = "DepMap Public 26Q1"
DEPMAP_BASELINE_RNA_RELEASE = "DepMap Public 26Q1"
DEPMAP_GENETIC_LAMIN_PREFIX = "depmap_genetic_dependencies/26q1"
DEPMAP_BASELINE_RNA_LAMIN_PREFIX = "depmap_ccle/26q1"
SANGER_SCORE_LAMIN_PREFIX = "sanger_score_crispr"

CRISPR_GENE_EFFECT_FILENAME = "CRISPRGeneEffect.csv"
CRISPR_GENE_DEPENDENCY_FILENAME = "CRISPRGeneDependency.csv"
SCREEN_GENE_EFFECT_FILENAME = "ScreenGeneEffect.csv"
SCREEN_GENE_DEPENDENCY_FILENAME = "ScreenGeneDependency.csv"
CRISPR_SCREEN_MAP_FILENAME = "CRISPRScreenMap.csv"
SCREEN_SEQUENCE_MAP_FILENAME = "ScreenSequenceMap.csv"
MODEL_FILENAME = "Model.csv"
GENE_FILENAME = "Gene.csv"
README_FILENAME = "README.txt"

DEPMAP_GENETIC_REQUIRED_FILENAMES = (
    CRISPR_GENE_EFFECT_FILENAME,
    CRISPR_GENE_DEPENDENCY_FILENAME,
    CRISPR_SCREEN_MAP_FILENAME,
    MODEL_FILENAME,
    GENE_FILENAME,
    README_FILENAME,
)
DEPMAP_GENETIC_OPTIONAL_FILENAMES = (
    SCREEN_GENE_EFFECT_FILENAME,
    SCREEN_GENE_DEPENDENCY_FILENAME,
    SCREEN_SEQUENCE_MAP_FILENAME,
)

ESSENTIALITY_OBS_REQUIRED_COLUMNS = (
    "essentiality_observation_id",
    "baseline_join_id",
    "cell_line",
    "model_id",
    "perturbation_gene",
    "perturbation_gene_id",
    "score",
    "score_type",
    "dataset_release",
    "baseline_release",
    "baseline_lamin_prefix",
    "perturbation_type",
    "readout_modality",
    "source",
    "source_filename",
    "assay",
    "is_control",
)
ESSENTIALITY_VAR_REQUIRED_COLUMNS = (
    "perturbation_gene_id",
    "perturbation_gene",
    "gene_label",
    "gene_id_type",
    "organism",
    "dataset_release",
    "source",
)
BASELINE_RNA_CONTRACT = {
    "lamin_prefix": DEPMAP_BASELINE_RNA_LAMIN_PREFIX,
    "release": DEPMAP_BASELINE_RNA_RELEASE,
    "artifact_role": "matched_baseline_expression",
    "expected_outputs": ("obs.parquet", "X.h5ad", "var.parquet"),
    "x_semantics": "RNA expression log2(TPM + 1), not dependency or essentiality scores",
    "join_fields": ("baseline_join_id", "model_id", "depmap_id"),
}
SANGER_SCORE_AUX_CONTRACT = {
    "lamin_prefix": SANGER_SCORE_LAMIN_PREFIX,
    "artifact_role": "essentiality_score_auxiliary_payload",
    "expected_outputs": (
        "obs.parquet",
        "X.h5ad(empty)",
        "var.parquet(empty)",
        "X_score.h5ad",
        "var_score.parquet",
    ),
    "x_semantics": "canonical X is intentionally empty; Project Score values live in typed X_score",
    "score_aux_keys": ("X_score", "var_score"),
}

GENE_LABEL_RE = re.compile(r"^(?P<symbol>.+?) \((?P<entrez_id>[^()]+)\)$")


def infer_depmap_readout_modality(score_column: str) -> str:
    """Map DepMap score columns to controlled readout-family labels."""

    normalized = score_column.lower()
    if "dependency" in normalized:
        return "dependency"
    if "effect" in normalized or "essentiality" in normalized:
        return "essentiality"
    raise ValueError(
        "Cannot infer DepMap readout_modality from score_column "
        f"{score_column!r}; pass a controlled value explicitly."
    )


@dataclass(frozen=True)
class DepMapDownloadFile:
    """Resolved file metadata from DepMap's public download API."""

    release: str
    filename: str
    url: str
    md5_hash: str | None = None
    content_length: int | None = None
    accessible: bool | None = None


def redact_transient_download_url(url: str) -> str:
    """Drop signed/expiring query parameters from a DepMap download URL."""

    parts = urlsplit(str(url))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def durable_manifest_entry(meta: DepMapDownloadFile) -> dict[str, object]:
    """Return committed metadata without transient signed URL credentials."""

    payload = asdict(meta)
    signed_url = str(payload.pop("url"))
    payload["download_api_url"] = DEPMAP_DOWNLOADS_API
    payload["source_url_redacted"] = redact_transient_download_url(signed_url)
    payload["download_url_note"] = (
        "DepMap download URLs are transient signed GCS URLs; resolve a fresh URL "
        "from download_api_url at ingestion time instead of reusing committed metadata."
    )
    return payload


def fetch_depmap_download_index(api_url: str = DEPMAP_DOWNLOADS_API) -> pd.DataFrame:
    """Fetch DepMap's public download index CSV without downloading matrices."""

    response = httpx.get(api_url, timeout=60.0, follow_redirects=True)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def resolve_depmap_files(
    release: str = DEPMAP_GENETIC_RELEASE,
    filenames: Iterable[str] = DEPMAP_GENETIC_REQUIRED_FILENAMES
    + DEPMAP_GENETIC_OPTIONAL_FILENAMES,
    *,
    index: pd.DataFrame | None = None,
    probe: bool = False,
) -> dict[str, DepMapDownloadFile]:
    """Resolve public DepMap URLs for a release and optional accessibility probes."""

    if index is None:
        index = fetch_depmap_download_index()
    assert index is not None

    required_columns = {"release", "filename", "url"}
    missing_columns = required_columns.difference(index.columns)
    if missing_columns:
        raise ValueError(f"DepMap index missing required columns: {sorted(missing_columns)}")

    resolved: dict[str, DepMapDownloadFile] = {}
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        for filename in filenames:
            rows = index[(index["release"] == release) & (index["filename"] == filename)]
            if rows.empty:
                continue
            row = rows.iloc[0]
            content_length: int | None = None
            accessible: bool | None = None
            if probe:
                head = client.head(row["url"])
                accessible = 200 <= head.status_code < 400
                length = head.headers.get("content-length")
                if length and length.isdigit():
                    content_length = int(length)
            resolved[filename] = DepMapDownloadFile(
                release=release,
                filename=filename,
                url=str(row["url"]),
                md5_hash=None if pd.isna(row.get("md5_hash")) else str(row.get("md5_hash")),
                content_length=content_length,
                accessible=accessible,
            )
    return resolved


def download_depmap_genetic_sources(
    output_dir: Path = Path("data/main/depmap_genetic_dependencies"),
    release: str = DEPMAP_GENETIC_RELEASE,
    filenames: Iterable[str] = DEPMAP_GENETIC_REQUIRED_FILENAMES,
    *,
    overwrite: bool = False,
) -> dict[str, Path]:
    """Download selected DepMap genetic-dependency source CSVs.

    This is explicit and opt-in because the main matrices are hundreds of MB
    each.  Callers that download large files should stage them to GCS after local
    verification instead of committing them to Git.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    resolved = resolve_depmap_files(release=release, filenames=filenames)
    missing = [filename for filename in filenames if filename not in resolved]
    if missing:
        raise ValueError(f"Could not resolve DepMap files for {release}: {missing}")

    downloaded: dict[str, Path] = {}
    with httpx.Client(timeout=600.0, follow_redirects=True) as client:
        for filename, meta in resolved.items():
            target = output_dir / filename
            if target.exists() and not overwrite:
                downloaded[filename] = target
                continue
            with client.stream("GET", meta.url) as stream:
                stream.raise_for_status()
                with target.open("wb") as handle:
                    for chunk in stream.iter_bytes(8 * 1024 * 1024):
                        handle.write(chunk)
            downloaded[filename] = target
    return downloaded


def parse_depmap_gene_label(label: str) -> tuple[str, str | None]:
    """Split DepMap matrix columns like ``A1BG (1)`` into symbol/Entrez ID."""

    match = GENE_LABEL_RE.match(str(label))
    if not match:
        return str(label), None
    return match.group("symbol"), match.group("entrez_id")


def load_model_metadata(model_csv: Path | None) -> pd.DataFrame | None:
    """Load selected DepMap Model.csv fields indexed by ModelID."""

    if model_csv is None:
        return None
    model = pd.read_csv(model_csv, low_memory=False)
    if "ModelID" not in model.columns:
        raise ValueError(f"{model_csv} does not contain a ModelID column")
    keep = [
        column
        for column in (
            "ModelID",
            "CellLineName",
            "StrippedCellLineName",
            "CCLEName",
            "OncotreeLineage",
            "OncotreePrimaryDisease",
            "OncotreeSubtype",
            "SangerModelID",
            "COSMICID",
            "Sex",
            "Age",
            "PatientRace",
            "SampleCollectionSite",
        )
        if column in model.columns
    ]
    return model.loc[:, keep].set_index("ModelID", drop=False)


def depmap_matrix_to_obs_var(
    matrix_csv: Path,
    *,
    dataset_release: str = DEPMAP_GENETIC_RELEASE,
    baseline_release: str = DEPMAP_BASELINE_RNA_RELEASE,
    baseline_lamin_prefix: str = DEPMAP_BASELINE_RNA_LAMIN_PREFIX,
    score_column: str,
    perturbation_type: str = "CRISPRko",
    readout_modality: str | None = None,
    model_csv: Path | None = None,
    source_filename: str | None = None,
    chunksize: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert a DepMap cell-line × gene score matrix to obs/var dataframes.

    The returned ``obs`` table is the essentiality/dependency artifact payload:
    one row per model/cell-line × perturbation gene, with score values in obs
    columns.  The returned ``var`` table describes perturbation genes.  No ``X``
    matrix is produced because these values are labels/readouts, not expression.
    """

    if chunksize is not None:
        raise NotImplementedError(
            "Streaming conversion is not implemented yet; use this skeleton on "
            "small samples or add chunked obs parquet writing before full 26Q1 conversion."
        )

    if readout_modality is None:
        readout_modality = infer_depmap_readout_modality(score_column)

    matrix = pd.read_csv(matrix_csv, index_col=0)
    matrix.index = matrix.index.astype(str)
    gene_meta = pd.DataFrame(
        [parse_depmap_gene_label(column) for column in matrix.columns],
        columns=["perturbation_gene", "perturbation_gene_id"],
        index=matrix.columns.astype(str),
    )
    malformed_gene_labels = gene_meta.index[
        gene_meta["perturbation_gene_id"].isna()
    ].tolist()
    if malformed_gene_labels:
        raise ValueError(
            "Malformed DepMap gene labels (expected 'SYMBOL (EntrezID)'): "
            f"{malformed_gene_labels[:5]}"
        )
    gene_meta["gene_label"] = gene_meta.index.astype(str)
    gene_meta["gene_id_type"] = gene_meta["perturbation_gene_id"].map(
        lambda value: "NCBI Entrez Gene ID" if pd.notna(value) and value is not None else None
    )
    gene_meta["organism"] = "human"
    gene_meta["dataset_release"] = dataset_release
    gene_meta["source"] = "DepMap"
    var = gene_meta.loc[:, list(ESSENTIALITY_VAR_REQUIRED_COLUMNS)].reset_index(drop=True)

    obs = (
        matrix.rename_axis("model_id")
        .reset_index()
        .melt(id_vars="model_id", var_name="gene_label", value_name=score_column)
    )
    obs["score"] = pd.to_numeric(obs[score_column], errors="coerce")
    obs["score_type"] = score_column
    obs = obs.merge(
        var[["perturbation_gene_id", "perturbation_gene", "gene_label"]],
        on="gene_label",
        how="left",
    )

    model = load_model_metadata(model_csv)
    if model is not None:
        obs = obs.merge(model, left_on="model_id", right_index=True, how="left")
        obs["cell_line"] = obs["CellLineName"].fillna(obs["model_id"])
    else:
        obs["cell_line"] = obs["model_id"]

    obs["baseline_join_id"] = obs["model_id"]
    obs["dataset_release"] = dataset_release
    obs["baseline_release"] = baseline_release
    obs["baseline_lamin_prefix"] = baseline_lamin_prefix
    obs["perturbation_type"] = perturbation_type
    obs["readout_modality"] = readout_modality
    obs["source_filename"] = source_filename or Path(matrix_csv).name
    obs["source"] = "DepMap"
    normalized_score_column = score_column.lower()
    obs["assay"] = (
        "Chronos_CRISPR_gene_effect"
        if "effect" in normalized_score_column
        else "Chronos_CRISPR_gene_dependency"
    )
    obs["is_control"] = False
    obs["essentiality_observation_id"] = (
        obs["source"].astype(str)
        + ":"
        + obs["dataset_release"].astype(str)
        + ":"
        + obs["model_id"].astype(str)
        + ":"
        + obs["perturbation_gene_id"].fillna(obs["perturbation_gene"]).astype(str)
        + ":"
        + obs["score_type"].astype(str)
    )

    first_columns = list(ESSENTIALITY_OBS_REQUIRED_COLUMNS) + [score_column]
    remaining = [column for column in obs.columns if column not in first_columns]
    obs = obs.loc[:, first_columns + remaining]
    validate_essentiality_obs_var_contract(obs, var)
    return obs, var


def depmap_matrix_to_long_table(
    matrix_csv: Path,
    *,
    dataset_release: str = DEPMAP_GENETIC_RELEASE,
    score_column: str,
    perturbation_type: str = "CRISPRko",
    readout_modality: str | None = None,
    model_csv: Path | None = None,
    source_filename: str | None = None,
    chunksize: int | None = None,
) -> pd.DataFrame:
    """Compatibility wrapper returning the canonical obs table.

    Older review wording called this a long table; in the corrected contract it
    is the obs half of an obs+var essentiality artifact, not an independent
    expression-like matrix artifact.
    """

    obs, _var = depmap_matrix_to_obs_var(
        matrix_csv,
        dataset_release=dataset_release,
        score_column=score_column,
        perturbation_type=perturbation_type,
        readout_modality=readout_modality,
        model_csv=model_csv,
        source_filename=source_filename,
        chunksize=chunksize,
    )
    return obs


def validate_baseline_rna_obs_contract(
    obs: pd.DataFrame,
    *,
    join_fields: Iterable[str] = BASELINE_RNA_CONTRACT["join_fields"],
) -> None:
    """Validate matched baseline RNA obs stays join/provenance-only, not scores."""

    missing = set(join_fields).difference(obs.columns)
    if missing:
        raise ValueError(f"Baseline RNA obs missing stable join fields: {sorted(missing)}")

    join_fields = tuple(join_fields)
    join_values = obs.loc[:, list(join_fields)]
    invalid = join_values.isna() | join_values.apply(
        lambda column: column.astype(str).str.strip().eq("")
    )
    if invalid.any(axis=None):
        bad_rows = list(obs.index[invalid.any(axis=1)])
        raise ValueError(
            "Baseline RNA stable join fields must be non-null and nonblank; "
            f"bad rows: {bad_rows}"
        )

    normalized = join_values.apply(lambda column: column.astype(str).str.strip())
    disagreements = normalized.nunique(axis=1) != 1
    if disagreements.any():
        bad_rows = list(obs.index[disagreements])
        raise ValueError(
            "Baseline RNA stable join fields must agree by exact normalized "
            f"string equality; bad rows: {bad_rows}"
        )
    forbidden_score_columns = {"effect_score", "dependency_score", "score", "gene_effect", "essentiality_score"}
    leaked = forbidden_score_columns.intersection(obs.columns)
    if leaked:
        raise ValueError(f"Baseline RNA obs must not contain essentiality score columns: {sorted(leaked)}")


def validate_essentiality_obs_var_contract(
    obs: pd.DataFrame,
    var: pd.DataFrame,
    *,
    expected_outputs: Iterable[str] = ("obs.parquet", "var.parquet"),
) -> None:
    """Validate essentiality-family obs+var tables without Lamin writes."""

    outputs = set(expected_outputs)
    if "X.h5ad" in outputs:
        raise ValueError("Essentiality/dependency artifacts must not declare X.h5ad outputs")

    missing_obs = set(ESSENTIALITY_OBS_REQUIRED_COLUMNS).difference(obs.columns)
    missing_var = set(ESSENTIALITY_VAR_REQUIRED_COLUMNS).difference(var.columns)
    if missing_obs:
        raise ValueError(f"Essentiality obs missing required columns: {sorted(missing_obs)}")
    if missing_var:
        raise ValueError(f"Essentiality var missing required columns: {sorted(missing_var)}")

    forbidden_obs = {"X", "expression", "rna_expression", "transcriptome"}.intersection(obs.columns)
    if forbidden_obs:
        raise ValueError(f"Essentiality obs contains expression/X columns: {sorted(forbidden_obs)}")

    null_join_columns = [
        column
        for column in ("baseline_join_id", "model_id", "perturbation_gene_id", "score_type")
        if column in obs.columns and obs[column].isna().any()
    ]
    if null_join_columns:
        raise ValueError(f"Essentiality obs has null required join columns: {null_join_columns}")


def write_table(df: pd.DataFrame, output_path: Path) -> Path:
    """Write a table as parquet or CSV based on suffix."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix == ".parquet":
        df.to_parquet(output_path, index=False)
    elif output_path.suffix == ".csv":
        df.to_csv(output_path, index=False)
    elif output_path.suffix == ".tsv":
        df.to_csv(output_path, sep="\t", index=False)
    else:
        raise ValueError(f"Unsupported table suffix for {output_path}")
    return output_path


def write_obs_var(obs: pd.DataFrame, var: pd.DataFrame, obs_path: Path, var_path: Path) -> tuple[Path, Path]:
    """Write canonical essentiality obs/var tables; intentionally no X output."""

    write_table(obs, obs_path)
    write_table(var, var_path)
    return obs_path, var_path


def write_resolved_manifest(
    output_path: Path,
    release: str = DEPMAP_GENETIC_RELEASE,
    *,
    probe: bool = True,
) -> Path:
    """Write durable DepMap genetic source metadata to JSON for audit/retry."""

    resolved = resolve_depmap_files(release=release, probe=probe)
    payload = {
        "release": release,
        "lamin_prefix": DEPMAP_GENETIC_LAMIN_PREFIX,
        "artifact_contract": "essentiality_obs_var_only",
        "essentiality_expected_outputs": ["obs.parquet", "var.parquet"],
        "forbidden_outputs": ["X.h5ad for dependency/gene-effect scores"],
        "baseline_rna_contract": BASELINE_RNA_CONTRACT,
        "required_obs_columns": list(ESSENTIALITY_OBS_REQUIRED_COLUMNS),
        "required_var_columns": list(ESSENTIALITY_VAR_REQUIRED_COLUMNS),
        "same_family_examples": [
            "Sanger SCORE/Project Score CRISPR KO stores the same kind of score signal, "
            "but PR #38 retyped it as empty canonical X plus typed X_score/var_score auxiliaries.",
        ],
        "sanger_score_aux_contract": SANGER_SCORE_AUX_CONTRACT,
        "required_filenames": list(DEPMAP_GENETIC_REQUIRED_FILENAMES),
        "optional_filenames": list(DEPMAP_GENETIC_OPTIONAL_FILENAMES),
        "download_api_url": DEPMAP_DOWNLOADS_API,
        "download_url_policy": (
            "Transient signed GCS query strings are intentionally omitted from this "
            "manifest. Resolve fresh download URLs from the DepMap API immediately "
            "before ingestion or explicit download."
        ),
        "files": {name: durable_manifest_entry(meta) for name, meta in resolved.items()},
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest_parser = subparsers.add_parser("manifest", help="resolve public source URLs")
    manifest_parser.add_argument("--release", default=DEPMAP_GENETIC_RELEASE)
    manifest_parser.add_argument("--output", type=Path, required=True)
    manifest_parser.add_argument("--no-probe", action="store_true")

    convert_parser = subparsers.add_parser("convert", help="convert one matrix CSV to obs+var tables")
    convert_parser.add_argument("--matrix", type=Path, required=True)
    convert_parser.add_argument("--model", type=Path, default=None)
    convert_parser.add_argument("--release", default=DEPMAP_GENETIC_RELEASE)
    convert_parser.add_argument("--score-column", required=True)
    convert_parser.add_argument("--obs-output", type=Path, required=True)
    convert_parser.add_argument("--var-output", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "manifest":
        write_resolved_manifest(args.output, release=args.release, probe=not args.no_probe)
        return 0
    if args.command == "convert":
        obs, var = depmap_matrix_to_obs_var(
            args.matrix,
            dataset_release=args.release,
            score_column=args.score_column,
            model_csv=args.model,
        )
        write_obs_var(obs, var, args.obs_output, args.var_output)
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
