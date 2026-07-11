"""Shared lightweight dataset loaders for perturbation model benchmarks.

The public contract in this module intentionally stays pure-Python: loaders return
small in-memory batches compatible with :mod:`pert_gym.evaluate` and the baseline
model protocol. Production Lamin/AnnData readers should adapt into this contract
without forcing model-specific heavy dependencies into CI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .evaluate import EvaluationBatch
from .models.base import Matrix

CONTROL_PERTURBATIONS = {"control", "ctrl", "vehicle", "ntc", "non-targeting"}
DEFAULT_CONTEXT_FIELDS = ("cell_line", "cell_type", "tissue", "disease", "assay")
DEFAULT_MODEL_READY_COLLECTION_KEY = "pert-gym/model-ready/20260621"
DEFAULT_MODEL_READY_MEMBER_COUNT = 1
EMPTY_RESPONSE_SCREEN_EXCLUSION_REASON = (
    "x_semantics=empty response_screen is not expression-model-ready"
)


@dataclass(frozen=True)
class BenchmarkBatch(EvaluationBatch):
    """Model-ready batch plus row-level covariates and target responses.

    Attributes mirror the canonical benchmark loader contract:
    - ``X`` is the feature/target response matrix extracted from expression-like
      payloads as a small in-memory matrix.
    - ``perturbations`` contains one perturbation identity per row.
    - ``controls`` flags control rows selected from ``obs.is_control`` or from a
      conservative perturbation-name fallback.
    - ``obs_covariates`` contains model-agnostic context metadata copied from obs.
    - ``target_response`` is the supervised response format consumed by the
      current evaluation scaffold; for direct expression prediction it equals
      ``X``.
    - ``feature_names`` are the var/index labels for matrix columns when known.
    - ``compound_features`` optionally carries one chemical feature vector per
      row, e.g. a precomputed Morgan fingerprint for chemCPA-style loaders.
    """

    obs_covariates: Sequence[Mapping[str, Any]] = ()
    target_response: Matrix | None = None
    feature_names: Sequence[str] = ()
    compound_features: Matrix | None = None
    compound_feature_names: Sequence[str] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if len(self.obs_covariates) != len(self.X):
            raise ValueError("obs_covariates must have one mapping per X row.")
        if self.target_response is not None and len(self.target_response) != len(
            self.X
        ):
            raise ValueError("target_response must have one row per X row.")
        if self.feature_names and len(self.feature_names) != _n_features(self.X):
            raise ValueError("feature_names must match the number of X columns.")
        if self.compound_features is not None and len(self.compound_features) != len(
            self.X
        ):
            raise ValueError("compound_features must have one row per X row.")
        if self.compound_feature_names and self.compound_features is not None:
            if len(self.compound_feature_names) != _n_features(self.compound_features):
                raise ValueError(
                    "compound_feature_names must match compound feature width."
                )


@dataclass(frozen=True)
class BenchmarkDataset:
    """Train/validation/test benchmark split for perturbation-response models."""

    train: BenchmarkBatch
    val: BenchmarkBatch
    test: BenchmarkBatch
    split_by: str
    source: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.split_by != "perturbation_identity":
            raise ValueError(
                "BenchmarkDataset splits must be by perturbation identity."
            )
        _validate_split_integrity(self.train, self.val, self.test)
        if not any(self.train.controls or []):
            raise ValueError("train split must include at least one control row.")
        if not any(self.val.controls or []):
            raise ValueError("val split must include at least one control row.")
        if not any(self.test.controls or []):
            raise ValueError("test split must include at least one control row.")


@dataclass(frozen=True)
class ExpressionMemberFilterResult:
    """Expression-loader membership after holding out non-expression screens."""

    included: Sequence[str]
    excluded: Sequence[str]
    excluded_reasons: Mapping[str, str]


def load_tiny_benchmark_dataset(
    *,
    source: str = "synthetic",
    obs_rows: Sequence[Mapping[str, Any]] | None = None,
    X: Matrix | None = None,
    feature_names: Sequence[str] | None = None,
    split_fractions: tuple[float, float, float] = (0.5, 0.25, 0.25),
) -> BenchmarkDataset:
    """Load a tiny model benchmark dataset, falling back to deterministic synthetic data.

    ``obs_rows`` + ``X`` can be supplied by a future model-ready-v0 adapter after
    bounded extraction from Lamin/AnnData. When omitted, a 12-row synthetic panel
    is returned for CI. Splits are by perturbation identity; controls are copied
    into every split so control baselines can be fit/evaluated without leakage of
    held-out perturbation identities.
    """

    if obs_rows is None or X is None:
        obs_rows, X, feature_names = _synthetic_rows()
    if feature_names is None:
        feature_names = [f"gene_{idx}" for idx in range(_n_features(X))]

    _validate_source_rows(obs_rows, X)
    split_names = split_perturbation_identities(obs_rows, split_fractions)
    return _build_dataset(
        obs_rows=obs_rows,
        X=X,
        feature_names=feature_names,
        split_names=split_names,
        source=source,
        metadata={"loader": "tiny", "n_obs": len(X), "n_features": _n_features(X)},
    )


def load_model_ready_v0_or_synthetic(
    *,
    manifest_path: Path | str = Path(
        "artifacts/schema_audit/model_ready_subset_20260621.json"
    ),
    source: str = "model-ready-v0",
) -> BenchmarkDataset:
    """Return a benchmark dataset from model-ready-v0 metadata or synthetic fallback.

    The current v0 model-ready artifact is a Lamin collection manifest, not a
    small checked-in matrix. This loader reads only the manifest metadata and then
    returns the CI-safe synthetic batch while preserving the manifest provenance
    in ``dataset.metadata``. A future bounded extractor can pass materialized
    ``obs_rows``/``X`` into :func:`load_tiny_benchmark_dataset` without changing
    the downstream model contract.
    """

    manifest = _read_json_if_exists(Path(manifest_path))
    collection = manifest.get("model_ready_collection", {})
    expression_members = filter_expression_model_ready_members(
        collection.get("member_keys", []),
        member_metadata=collection.get("member_metadata", {}),
    )
    dataset = load_tiny_benchmark_dataset(source=source)
    metadata = dict(dataset.metadata)
    metadata.update(
        {
            "loader": "model_ready_v0_or_synthetic",
            "fallback": "synthetic",
            "manifest_path": str(manifest_path),
            "model_ready_collection_key": collection.get("key")
            or DEFAULT_MODEL_READY_COLLECTION_KEY,
            "model_ready_member_count": collection.get("member_count")
            or DEFAULT_MODEL_READY_MEMBER_COUNT,
            "model_ready_member_keys": list(expression_members.included),
            "excluded_member_keys": list(expression_members.excluded),
            "excluded_member_reasons": dict(expression_members.excluded_reasons),
        }
    )
    return BenchmarkDataset(
        train=dataset.train,
        val=dataset.val,
        test=dataset.test,
        split_by=dataset.split_by,
        source=source,
        metadata=metadata,
    )


def filter_expression_model_ready_members(
    member_keys: Sequence[str],
    *,
    member_metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> ExpressionMemberFilterResult:
    """Return members safe for expression-only benchmark loaders."""

    metadata_by_key = member_metadata or {}
    included: list[str] = []
    excluded: list[str] = []
    reasons: dict[str, str] = {}
    for key in member_keys:
        key_text = str(key)
        metadata = metadata_by_key.get(key_text, {})
        prefix = _dataset_prefix_from_obs_key(key_text)
        x_semantics = str(metadata.get("x_semantics", "")).lower()
        modality = str(metadata.get("modality", metadata.get("assay", ""))).lower()
        if prefix == "broad_prism_repurposing" or (
            x_semantics == "empty" and "response" in modality
        ):
            excluded.append(key_text)
            reasons[key_text] = EMPTY_RESPONSE_SCREEN_EXCLUSION_REASON
            continue
        included.append(key_text)
    return ExpressionMemberFilterResult(
        included=included, excluded=excluded, excluded_reasons=reasons
    )


def load_response_screen_with_baseline(
    *,
    response_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
) -> BenchmarkBatch:
    """Join bounded response-screen rows to separate baseline RNA expression."""

    if not baseline_rows:
        raise ValueError(
            "response-screen loaders require separate baseline RNA expression"
        )
    baseline_by_id: dict[str, tuple[float, ...]] = {}
    for idx, row in enumerate(baseline_rows):
        stable_id = _stable_depmap_id(row.get("depmap_id") or row.get("ach_id"))
        if not stable_id:
            raise ValueError(f"baseline row {idx} is missing depmap_id/ach_id")
        expression = row.get("expression")
        if expression is None:
            raise ValueError(f"baseline row {idx} is missing baseline RNA expression")
        try:
            expression_values = tuple(float(value) for value in expression)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"baseline row {idx} has malformed baseline RNA expression"
            ) from exc
        existing_expression = baseline_by_id.get(stable_id)
        if existing_expression is not None:
            if expression_values != existing_expression:
                raise ValueError(
                    f"baseline row {idx} has non-identical baseline RNA expression "
                    f"for duplicate {stable_id}"
                )
            continue
        baseline_by_id[stable_id] = expression_values

    X: list[list[float]] = []
    target_response: list[list[float]] = []
    perturbations: list[str] = []
    controls: list[bool] = []
    covariates: list[dict[str, Any]] = []
    for idx, row in enumerate(response_rows):
        stable_id = _stable_depmap_id(row.get("depmap_id") or row.get("ach_id"))
        if not stable_id:
            raise ValueError(f"response row {idx} is missing depmap_id/ach_id")
        if stable_id not in baseline_by_id:
            raise ValueError(
                f"response row {idx} has no baseline RNA expression for {stable_id}"
            )
        response_metric = str(row.get("response_metric", "")).strip().lower()
        response_value = row.get("response_value")
        if response_value is None:
            raise ValueError(f"response row {idx} is missing response_value")
        try:
            numeric_response = float(response_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"response row {idx} has malformed response_value: {response_value!r}"
            ) from exc
        if response_metric in {"", "missing", "none", "nan"}:
            raise ValueError(f"response row {idx} is missing response_metric")
        X.append(list(baseline_by_id[stable_id]))
        target_response.append([numeric_response])
        perturbations.append(str(row.get("perturbation", row.get("broad_id", ""))))
        controls.append(_is_control_row(row))
        covariates.append({"depmap_id": stable_id, "response_metric": response_metric})

    if not X:
        raise ValueError("response-screen loader requires at least one response row")
    if len(feature_names) != _n_features(X):
        raise ValueError("feature_names must match baseline RNA expression width")
    return BenchmarkBatch(
        X=X,
        perturbations=perturbations,
        controls=controls,
        obs_covariates=tuple(covariates),
        target_response=target_response,
        feature_names=tuple(feature_names),
    )


def load_chemcpa_drugseq_tiny(
    *,
    artifact_path: Path | str = Path(
        "artifacts/model_benchmarks/chemcpa_drugseq_tiny_20260622.json"
    ),
) -> BenchmarkDataset:
    """Load the tiny real molecular DRUG-seq chemCPA-ready export.

    The export is generated by ``tools/build_chemcpa_drugseq_tiny.py`` from the
    72-sample DRUG-seq GSE120222 expression triplet plus a frozen PubChem/RDKit
    compound-structure snapshot. It contains real expression rows, DMSO controls,
    compound identity/dose metadata, and precomputed Morgan fingerprints. No
    Lamin connection or RDKit import is needed at loader time.
    """

    artifact = _read_json_if_exists(Path(artifact_path))
    if not artifact:
        raise FileNotFoundError(
            f"chemCPA DRUG-seq artifact not found: {artifact_path}. "
            "Run `uv run --with rdkit python tools/build_chemcpa_drugseq_tiny.py`."
        )
    rows = list(artifact["rows"])
    X = [list(map(float, row["expression"])) for row in rows]
    feature_names = list(artifact["feature_names"])
    metadata = {
        "loader": "chemcpa_drugseq_tiny",
        "artifact_path": str(artifact_path),
        "source": artifact.get("source", {}),
        "selection": artifact.get("selection", {}),
        "compound_metadata": artifact.get("compound_metadata", {}),
        "fallback": None,
    }
    split_names = split_perturbation_identities(rows)
    return _build_dataset(
        obs_rows=rows,
        X=X,
        feature_names=feature_names,
        split_names=split_names,
        source="DRUG-seq/GSE120222-real-molecular-tiny",
        metadata=metadata,
    )


def load_scgen_viperturb_tiny(
    *,
    artifact_path: Path | str = Path(
        "artifacts/model_benchmarks/scgen_real_viperturb_tiny_20260622.json"
    ),
) -> BenchmarkDataset:
    """Load the tiny real VIPerturb scGEN-ready expression export.

    The export is generated by ``tools/build_scgen_viperturb_tiny.py`` from the
    reviewed ``pert-gym/model-ready/20260621`` VIPerturb member. It contains a
    bounded local AnnData path plus a JSON sidecar with real expression rows,
    ``condition``/``control_value`` semantics, controls, and at least three
    non-control perturbation identities. No Lamin connection is needed here.
    """

    artifact = _read_json_if_exists(Path(artifact_path))
    if not artifact:
        raise FileNotFoundError(
            f"scGEN VIPerturb artifact not found: {artifact_path}. "
            "Run `uv run python tools/build_scgen_viperturb_tiny.py`."
        )
    rows = list(artifact["rows"])
    X = [list(map(float, row["expression"])) for row in rows]
    feature_names = list(artifact["feature_names"])
    metadata = {
        "loader": "scgen_viperturb_tiny",
        "artifact_path": str(artifact_path),
        "adata_path": artifact.get("export", {}).get("adata_path"),
        "source": artifact.get("source", {}),
        "selection": artifact.get("selection", {}),
        "export": artifact.get("export", {}),
        "fallback": None,
    }
    split_names = split_perturbation_identities(rows)
    return _build_dataset(
        obs_rows=rows,
        X=X,
        feature_names=feature_names,
        split_names=split_names,
        source="VIPerturbSeq/vimentin_screen-real-scgen-tiny",
        metadata=metadata,
    )


def split_perturbation_identities(
    obs_rows: Sequence[Mapping[str, Any]],
    fractions: tuple[float, float, float] = (0.5, 0.25, 0.25),
) -> Mapping[str, set[str]]:
    """Assign non-control perturbation identities to train/val/test splits."""

    if len(fractions) != 3 or not 0.99 <= sum(fractions) <= 1.01:
        raise ValueError("fractions must contain train/val/test values summing to 1.")

    perturbations = sorted(
        {
            str(row["perturbation"])
            for row in obs_rows
            if not _as_bool(row.get("is_control"))
        }
    )
    if len(perturbations) < 3:
        raise ValueError("Need at least three non-control perturbation identities.")

    n_train = max(1, round(len(perturbations) * fractions[0]))
    n_val = max(1, round(len(perturbations) * fractions[1]))
    if n_train + n_val >= len(perturbations):
        n_train = max(1, len(perturbations) - 2)
        n_val = 1
    return {
        "train": set(perturbations[:n_train]),
        "val": set(perturbations[n_train : n_train + n_val]),
        "test": set(perturbations[n_train + n_val :]),
    }


def write_benchmark_artifacts(
    dataset: BenchmarkDataset,
    *,
    artifact_dir: Path | str = Path("artifacts/model_benchmarks"),
) -> Path:
    """Write a compact benchmark-loader smoke summary JSON and return its path."""

    artifact_dir = Path(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    out_path = artifact_dir / "loader_smoke_summary.json"
    payload = {
        "source": dataset.source,
        "split_by": dataset.split_by,
        "metadata": dict(dataset.metadata),
        "splits": {
            "train": _batch_summary(dataset.train),
            "val": _batch_summary(dataset.val),
            "test": _batch_summary(dataset.test),
        },
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return out_path


def _build_dataset(
    *,
    obs_rows: Sequence[Mapping[str, Any]],
    X: Matrix,
    feature_names: Sequence[str],
    split_names: Mapping[str, set[str]],
    source: str,
    metadata: Mapping[str, Any],
) -> BenchmarkDataset:
    controls_idx = [idx for idx, row in enumerate(obs_rows) if _is_control_row(row)]
    if not controls_idx:
        raise ValueError("Benchmark loader requires at least one control row.")

    batches = {}
    for split, perturbation_names in split_names.items():
        split_idx = [
            idx
            for idx, row in enumerate(obs_rows)
            if _is_control_row(row) or str(row["perturbation"]) in perturbation_names
        ]
        batches[split] = _subset_batch(obs_rows, X, split_idx, feature_names)

    return BenchmarkDataset(
        train=batches["train"],
        val=batches["val"],
        test=batches["test"],
        split_by="perturbation_identity",
        source=source,
        metadata=metadata,
    )


def _subset_batch(
    obs_rows: Sequence[Mapping[str, Any]],
    X: Matrix,
    indices: Sequence[int],
    feature_names: Sequence[str],
) -> BenchmarkBatch:
    matrix = [[float(value) for value in X[idx]] for idx in indices]
    perturbations = [str(obs_rows[idx]["perturbation"]) for idx in indices]
    controls = [_is_control_row(obs_rows[idx]) for idx in indices]
    covariates = [
        {
            key: obs_rows[idx].get(key)
            for key in DEFAULT_CONTEXT_FIELDS
            if key in obs_rows[idx]
        }
        for idx in indices
    ]
    compound_features = None
    if any("compound_fingerprint" in obs_rows[idx] for idx in indices):
        compound_features = [
            [float(value) for value in obs_rows[idx].get("compound_fingerprint", [])]
            for idx in indices
        ]
    return BenchmarkBatch(
        X=matrix,
        perturbations=perturbations,
        controls=controls,
        obs_covariates=covariates,
        target_response=matrix,
        feature_names=tuple(feature_names),
        compound_features=compound_features,
        compound_feature_names=(
            tuple(f"morgan_{idx}" for idx in range(len(compound_features[0])))
            if compound_features
            else ()
        ),
    )


def _synthetic_rows() -> tuple[list[dict[str, Any]], list[list[float]], list[str]]:
    feature_names = ["gene_a", "gene_b", "gene_c"]
    obs_rows: list[dict[str, Any]] = []
    X: list[list[float]] = []
    rows = [
        ("control", True, [1.0, 1.0, 1.0]),
        ("control", True, [1.1, 0.9, 1.0]),
        ("pert_a", False, [2.0, 1.0, 1.0]),
        ("pert_a", False, [2.1, 1.1, 1.0]),
        ("pert_b", False, [1.0, 2.0, 1.0]),
        ("pert_b", False, [1.1, 2.1, 0.9]),
        ("pert_c", False, [1.0, 1.0, 2.0]),
        ("pert_c", False, [0.9, 1.1, 2.1]),
        ("pert_d", False, [2.0, 2.0, 1.0]),
        ("pert_d", False, [2.1, 1.9, 1.1]),
        ("pert_e", False, [1.0, 2.0, 2.0]),
        ("pert_e", False, [1.1, 2.1, 1.9]),
    ]
    for idx, (perturbation, is_control, row) in enumerate(rows):
        obs_rows.append(
            {
                "cell_id": f"cell_{idx:03d}",
                "perturbation": perturbation,
                "perturbation_type": "synthetic" if not is_control else "control",
                "is_control": is_control,
                "cell_line": "synthetic_line",
                "assay": "synthetic_scRNA_smoke",
            }
        )
        X.append(row)
    return obs_rows, X, feature_names


def _validate_source_rows(obs_rows: Sequence[Mapping[str, Any]], X: Matrix) -> None:
    if len(obs_rows) != len(X):
        raise ValueError("obs_rows and X must have matching row counts.")
    for idx, row in enumerate(obs_rows):
        if "perturbation" not in row:
            raise ValueError(f"obs row {idx} is missing perturbation.")
        if "is_control" not in row:
            raise ValueError(f"obs row {idx} is missing is_control.")
    _n_features(X)


def _validate_split_integrity(*batches: BenchmarkBatch) -> None:
    non_control_sets = []
    for batch in batches:
        non_control_sets.append(
            {
                perturbation
                for perturbation, is_control in zip(
                    batch.perturbations, batch.controls or []
                )
                if not is_control
            }
        )
    for left_idx, left in enumerate(non_control_sets):
        for right in non_control_sets[left_idx + 1 :]:
            overlap = left & right
            if overlap:
                raise ValueError(
                    "Non-control perturbation identities must not overlap across splits: "
                    + ", ".join(sorted(overlap))
                )


def _is_control_row(row: Mapping[str, Any]) -> bool:
    explicit = row.get("is_control")
    if explicit is not None:
        return _as_bool(explicit)
    return str(row.get("perturbation", "")).lower() in CONTROL_PERTURBATIONS


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _dataset_prefix_from_obs_key(key: str) -> str:
    suffix = "/obs.parquet"
    if key.endswith(suffix):
        return key[: -len(suffix)]
    return key.split("/", 1)[0]


def _stable_depmap_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text.split("::", 1)[0]


def _n_features(X: Matrix) -> int:
    if not X:
        return 0
    n_features = len(X[0])
    for row in X:
        if len(row) != n_features:
            raise ValueError("All rows in X must have the same number of features.")
    return n_features


def _read_json_if_exists(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _batch_summary(batch: BenchmarkBatch) -> dict[str, Any]:
    controls = list(batch.controls or [])
    return {
        "n_obs": len(batch.X),
        "n_features": _n_features(batch.X),
        "n_compound_features": _n_features(batch.compound_features or []),
        "n_controls": sum(1 for is_control in controls if is_control),
        "non_control_perturbations": sorted(
            {
                perturbation
                for perturbation, is_control in zip(batch.perturbations, controls)
                if not is_control
            }
        ),
        "covariate_fields": sorted(
            {field for covariates in batch.obs_covariates for field in covariates}
        ),
    }
