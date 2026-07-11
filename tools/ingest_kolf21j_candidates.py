#!/usr/bin/env python3
"""Build append-only KOLF2.1J canonical triplet candidates on an approved EU VM.

Downloads directly to the capacity VM with resume + MD5 verification, reads each
source in backed mode, emits same-prefix obs/X/var triplets, and only then writes
candidate Lamin artifacts on branch ``jkobject``.  It refuses Mac/local runs,
existing candidate keys, bad checksums, source schema drift, or a target-count
drift from the validated KOLF2.1J contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import require_heavy_vm  # noqa: E402

FIGSHARE_ARTICLE = 27261219
LICENSE = "CC BY 4.0"
DOI = "10.25452/figshare.plus.27261219"
SOURCE_URL = "https://ndownloader.figshare.com/files/{file_id}"


@dataclass(frozen=True)
class Variant:
    dataset_id: str
    file_id: int
    filename: str
    size_bytes: int
    md5: str
    expected_target_denominator: int

    @property
    def prefix(self) -> str:
        return f"kolf21j/{self.dataset_id}"


VARIANTS = (
    Variant(
        dataset_id="kolf21j_pan_genome_qc_filtered",
        file_id=64650261,
        filename="KOLF_Pan_Genome_QC_Filtered.h5ad",
        size_bytes=189393177972,
        md5="afd30fde1e6ad32969c29868394385d1",
        expected_target_denominator=11692,
    ),
    Variant(
        dataset_id="kolf21j_strong_perturbations",
        file_id=64650852,
        filename="KOLF_Strong_Perturbations.h5ad",
        size_bytes=46718752086,
        md5="28bcfff0679e7c6c35bdd584f3626362",
        expected_target_denominator=11739,
    ),
)


def file_md5(path: Path) -> str:
    digest = hashlib.md5()  # nosec B324: source publisher specifies MD5
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_verified(variant: Variant, source_dir: Path) -> Path:
    source_dir.mkdir(parents=True, exist_ok=True)
    path = source_dir / variant.filename
    if not path.exists() or path.stat().st_size != variant.size_bytes:
        subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--retry",
                "8",
                "--retry-all-errors",
                "--connect-timeout",
                "30",
                "--continue-at",
                "-",
                "--output",
                str(path),
                SOURCE_URL.format(file_id=variant.file_id),
            ],
            check=True,
        )
    if path.stat().st_size != variant.size_bytes:
        raise RuntimeError(
            f"source size mismatch for {variant.filename}: {path.stat().st_size}"
        )
    actual = file_md5(path)
    if actual != variant.md5:
        raise RuntimeError(f"source MD5 mismatch for {variant.filename}: {actual}")
    return path


def build_canonical_obs(source_obs: pd.DataFrame, *, dataset_id: str) -> pd.DataFrame:
    required = {"gene_target", "gene_target_ensembl_id", "gRNA", "perturbed"}
    missing = sorted(required - set(source_obs.columns))
    if missing:
        raise ValueError(f"KOLF source is missing required obs fields: {missing}")
    obs = source_obs.copy()
    target = obs["gene_target"].astype(str)
    perturbed = obs["perturbed"].astype(str).str.lower().eq("true")
    obs["dataset_id"] = dataset_id
    obs["source_accession"] = f"figshare:{FIGSHARE_ARTICLE}"
    obs["perturbation"] = target
    obs["perturbation_target"] = target
    obs["perturbation_target_id"] = obs["gene_target_ensembl_id"].astype(str)
    obs["guide_id"] = obs["gRNA"].astype(str)
    obs["is_control"] = target.eq("NTC")
    obs["is_perturbed"] = perturbed
    obs["perturbation_type"] = "CRISPRi"
    obs["organism"] = "human"
    obs["cell_line"] = "KOLF2.1J"
    obs["modality"] = "scRNA-seq"
    obs["assay"] = "Perturb-seq"
    return obs


def target_denominator(obs: pd.DataFrame) -> int:
    values = obs.loc[~obs["is_control"], "perturbation_target_id"].dropna().astype(str)
    values = values[~values.isin({"", "nan", "None", "NTC"})]
    return int(values.nunique())


def write_x_only_h5ad(source_path: Path, output_path: Path) -> None:
    """Copy only the on-disk X group, preserving sparse encoding without RAM loading."""
    if output_path.exists():
        raise FileExistsError(f"refusing to overwrite X candidate {output_path}")
    source = ad.read_h5ad(source_path, backed="r")
    try:
        shell = ad.AnnData(
            X=sparse.csr_matrix(
                (int(source.n_obs), int(source.n_vars)), dtype=source.X.dtype
            ),
            obs=pd.DataFrame(index=source.obs_names.copy()),
            var=pd.DataFrame(index=source.var_names.copy()),
        )
        shell.write_h5ad(output_path)
        with h5py.File(source_path, "r") as raw, h5py.File(output_path, "r+") as target:
            del target["X"]
            raw.copy("X", target)
    finally:
        source.file.close()


def prepare_x_candidate(source_path: Path, output_dir: Path) -> Path:
    """Create X once, then retain it unchanged for an interrupted publication resume."""
    x_path = output_dir / "X.h5ad"
    if output_dir.exists():
        if not output_dir.is_dir() or not x_path.is_file():
            raise RuntimeError(
                f"incomplete KOLF output directory cannot resume: {output_dir}"
            )
        return x_path
    output_dir.mkdir(parents=True)
    write_x_only_h5ad(source_path, x_path)
    return x_path


def candidate_keys(variant: Variant) -> tuple[str, str, str]:
    return tuple(
        f"{variant.prefix}/{name}" for name in ("obs.parquet", "X.h5ad", "var.parquet")
    )


_TRIPLET_STAGES = ("obs", "x", "var", "var-link", "x-link")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dataframe_identity(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(repr(tuple(frame.columns)).encode())
    digest.update(repr(tuple(frame.dtypes.astype(str))).encode())
    digest.update(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    return digest.hexdigest()


def _journal_path(x_path: Path) -> Path:
    return x_path.with_name("publication-journal.json")


def _load_or_create_journal(
    path: Path, identity: dict[str, object]
) -> dict[str, object]:
    if path.exists():
        try:
            journal = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "KOLF publication journal is malformed or torn"
            ) from error
        if not isinstance(journal, dict):
            raise RuntimeError("KOLF publication journal is malformed or torn")
        completed = journal.get("completed_stages")
        artifact_uids = journal.get("artifact_uids")
        pending = journal.get("pending_stage")
        if (
            journal.get("format") != "pert-gym.kolf21j-triplet.publication-journal/v2"
            or not isinstance(completed, list)
            or not all(isinstance(stage, str) for stage in completed)
            or not isinstance(artifact_uids, dict)
            or not all(
                stage in {"obs", "x", "var"} and isinstance(uid, str) and uid
                for stage, uid in artifact_uids.items()
            )
            or (
                pending is not None
                and (
                    not isinstance(pending, dict)
                    or set(pending) != {"stage", "uid"}
                    or pending.get("stage") not in _TRIPLET_STAGES
                    or not isinstance(pending.get("uid"), str)
                    or not pending["uid"]
                )
            )
        ):
            raise RuntimeError("KOLF publication journal is malformed or torn")
        if journal.get("identity") != identity:
            raise RuntimeError("KOLF publication journal identity mismatch")
        return journal
    journal: dict[str, object] = {
        "format": "pert-gym.kolf21j-triplet.publication-journal/v2",
        "identity": identity,
        "completed_stages": [],
        "artifact_uids": {},
        "pending_stage": None,
    }
    _write_journal(path, journal)
    return journal


def _write_journal(path: Path, journal: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(journal, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        try:
            directory = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            try:
                os.fsync(directory)
            except OSError:
                pass
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _exact_artifact(ln: Any, key: str) -> Any | None:
    records = list(ln.Artifact.filter(key=key).all())
    if len(records) > 1:
        raise RuntimeError(f"duplicate KOLF artifacts returned for exact key {key}")
    if not records:
        return None
    artifact = records[0]
    if getattr(artifact, "key", None) != key or not getattr(artifact, "uid", None):
        raise RuntimeError(
            f"KOLF artifact does not have immutable exact-key identity: {key}"
        )
    if getattr(artifact, "revises", None) is not None:
        raise RuntimeError(f"KOLF artifact is an implicit revision: {key}")
    return artifact


def _artifact_uid(artifact: Any) -> str:
    uid = str(getattr(artifact, "uid", ""))
    if not uid:
        raise RuntimeError("KOLF artifact does not have immutable UID")
    return uid


def _feature_artifact(ln: Any, artifact: Any, name: str) -> Any | None:
    value = artifact.features.get_values().get(name)
    if isinstance(value, str):
        value = ln.Artifact.get(key=value)
    return value


def _assert_feature(ln: Any, artifact: Any, name: str, expected: Any) -> bool:
    linked = _feature_artifact(ln, artifact, name)
    if linked is None:
        return False
    if getattr(linked, "key", None) != getattr(expected, "key", None) or _artifact_uid(
        linked
    ) != _artifact_uid(expected):
        raise RuntimeError(f"KOLF {name} link has a foreign immutable identity")
    return True


def _remote_artifacts(ln: Any, keys: tuple[str, str, str]) -> dict[str, Any | None]:
    obs_key, x_key, var_key = keys
    return {
        "obs": _exact_artifact(ln, obs_key),
        "x": _exact_artifact(ln, x_key),
        "var": _exact_artifact(ln, var_key),
    }


def _remote_stages(ln: Any, artifacts: dict[str, Any | None]) -> list[str]:
    obs_art = artifacts["obs"]
    x_art = artifacts["x"]
    var_art = artifacts["var"]
    stages = [
        stage
        for stage, artifact in (("obs", obs_art), ("x", x_art), ("var", var_art))
        if artifact is not None
    ]
    if (
        x_art is not None
        and var_art is not None
        and _assert_feature(ln, x_art, "var", var_art)
    ):
        stages.append("var-link")
    if (
        obs_art is not None
        and x_art is not None
        and _assert_feature(ln, obs_art, "X", x_art)
    ):
        stages.append("x-link")
    return stages


def _assert_remote_stage_prefix(remote: list[str], completed: list[str]) -> None:
    if remote != list(_TRIPLET_STAGES[: len(remote)]):
        raise RuntimeError(
            "KOLF remote publication stages must form a contiguous prefix"
        )
    if completed != list(_TRIPLET_STAGES[: len(completed)]):
        raise RuntimeError("KOLF journal stages must form a contiguous prefix")
    if len(remote) not in (len(completed), len(completed) + 1):
        raise RuntimeError("KOLF remote stages are inconsistent with the journal")


def _stage_artifact(stage: str, artifacts: dict[str, Any | None]) -> Any:
    artifact_name = {
        "obs": "obs",
        "x": "x",
        "var": "var",
        "var-link": "x",
        "x-link": "obs",
    }[stage]
    artifact = artifacts[artifact_name]
    if artifact is None:
        raise RuntimeError(f"KOLF {stage} stage lacks its immutable artifact")
    return artifact


def _stage_uid_key(stage: str) -> str:
    return {"var-link": "x", "x-link": "obs"}.get(stage, stage)


def _validate_remote_bindings(
    journal: dict[str, object], remote: list[str], artifacts: dict[str, Any | None]
) -> None:
    completed = journal["completed_stages"]
    artifact_uids = journal["artifact_uids"]
    pending = journal["pending_stage"]
    assert isinstance(completed, list)
    assert isinstance(artifact_uids, dict)
    for stage in completed:
        uid = _artifact_uid(_stage_artifact(stage, artifacts))
        if artifact_uids.get(_stage_uid_key(stage)) != uid:
            raise RuntimeError(f"KOLF journal artifact UID mismatch at {stage}")
    if len(remote) == len(completed) + 1:
        stage = remote[-1]
        uid = _artifact_uid(_stage_artifact(stage, artifacts))
        if pending != {"stage": stage, "uid": uid}:
            raise RuntimeError(
                f"KOLF unbound remote stage {stage}; refusing foreign artifact adoption"
            )
    elif pending is not None:
        raise RuntimeError("KOLF journal has an uncommitted stage without remote state")


def _discard_unapplied_pending_stage(
    path: Path, journal: dict[str, object], remote: list[str]
) -> None:
    """Discard a durable intent only when no corresponding remote mutation exists."""
    pending = journal["pending_stage"]
    if pending is None:
        return
    assert isinstance(pending, dict)
    completed = journal["completed_stages"]
    assert isinstance(completed, list)
    if len(remote) != len(completed):
        return
    if (
        len(completed) >= len(_TRIPLET_STAGES)
        or pending.get("stage") != _TRIPLET_STAGES[len(completed)]
    ):
        raise RuntimeError(
            "KOLF journal pending stage is not the next publication stage"
        )
    journal["pending_stage"] = None
    _write_journal(path, journal)


def _begin_stage(
    path: Path, journal: dict[str, object], stage: str, artifact: Any
) -> None:
    pending = {"stage": stage, "uid": _artifact_uid(artifact)}
    if journal["pending_stage"] != pending:
        journal["pending_stage"] = pending
        _write_journal(path, journal)


def _complete_stage(path: Path, journal: dict[str, object], stage: str) -> None:
    completed = journal["completed_stages"]
    artifact_uids = journal["artifact_uids"]
    assert isinstance(completed, list)
    assert isinstance(artifact_uids, dict)
    pending = journal["pending_stage"]
    if not isinstance(pending, dict) or pending.get("stage") != stage:
        raise RuntimeError(f"KOLF journal lacks a durable UID binding for {stage}")
    if stage in {"obs", "x", "var"}:
        artifact_uids[stage] = pending["uid"]
    if stage not in completed:
        completed.append(stage)
    journal["pending_stage"] = None
    _write_journal(path, journal)


def _crash_after_stage(stage: str, stop_after_stage: str | None) -> None:
    if stage == stop_after_stage:
        raise RuntimeError(f"intentional crash after {stage}")


def register_triplet(
    ln: Any,
    variant: Variant,
    obs: pd.DataFrame,
    var: pd.DataFrame,
    x_path: Path,
    *,
    stop_after_stage: str | None = None,
) -> dict[str, Any]:
    """Save an exact-key triplet through an append-only, resumable local journal."""
    if stop_after_stage is not None and stop_after_stage not in _TRIPLET_STAGES:
        raise ValueError("unknown KOLF publication stage")
    obs_key, x_key, var_key = candidate_keys(variant)
    keys = (obs_key, x_key, var_key)
    journal_path = _journal_path(x_path)
    identity: dict[str, object] = {
        "prefix": variant.prefix,
        "keys": list(keys),
        "obs_sha256": _dataframe_identity(obs),
        "x_sha256": _sha256_file(x_path),
        "var_sha256": _dataframe_identity(var),
    }
    journal = _load_or_create_journal(journal_path, identity)
    completed = journal["completed_stages"]
    assert isinstance(completed, list)
    artifacts = _remote_artifacts(ln, keys)
    remote = _remote_stages(ln, artifacts)
    _assert_remote_stage_prefix(remote, completed)
    _discard_unapplied_pending_stage(journal_path, journal, remote)
    _validate_remote_bindings(journal, remote, artifacts)

    def save_stage(stage: str, artifact: Any | None, create: Any) -> Any:
        if stage in completed:
            if artifact is None:
                raise RuntimeError(
                    f"KOLF journal says {stage} completed but it is missing"
                )
            return artifact
        if artifact is None:
            candidate = create()
            _begin_stage(journal_path, journal, stage, candidate)
            expected_uid = _artifact_uid(candidate)
            candidate.save()
            artifact = _exact_artifact(
                ln, {"obs": obs_key, "x": x_key, "var": var_key}[stage]
            )
            if artifact is None or _artifact_uid(artifact) != expected_uid:
                raise RuntimeError(f"KOLF artifact save UID mismatch at {stage}")
            _crash_after_stage(stage, stop_after_stage)
        _complete_stage(journal_path, journal, stage)
        return artifact

    obs_art = save_stage(
        "obs",
        artifacts["obs"],
        lambda: ln.Artifact.from_dataframe(obs, key=obs_key, skip_hash_lookup=True),
    )
    x_art = save_stage(
        "x",
        artifacts["x"],
        lambda: ln.Artifact.from_anndata(
            ad.read_h5ad(x_path, backed="r"), key=x_key, skip_hash_lookup=True
        ),
    )
    var_art = save_stage(
        "var",
        artifacts["var"],
        lambda: ln.Artifact.from_dataframe(var, key=var_key, skip_hash_lookup=True),
    )

    if "var-link" not in completed:
        if not _assert_feature(ln, x_art, "var", var_art):
            _begin_stage(journal_path, journal, "var-link", x_art)
            x_art.features.set_values({"var": var_art})
            if not _assert_feature(ln, x_art, "var", var_art):
                raise RuntimeError(
                    f"KOLF var link readback failed for {variant.prefix}"
                )
            _crash_after_stage("var-link", stop_after_stage)
        _complete_stage(journal_path, journal, "var-link")

    if "x-link" not in completed:
        if not _assert_feature(ln, obs_art, "X", x_art):
            _begin_stage(journal_path, journal, "x-link", obs_art)
            obs_art.features.set_values({"X": x_art})
            if not _assert_feature(ln, obs_art, "X", x_art):
                raise RuntimeError(f"KOLF X link readback failed for {variant.prefix}")
            _crash_after_stage("x-link", stop_after_stage)
        _complete_stage(journal_path, journal, "x-link")
    return {
        "obs_key": obs_key,
        "x_key": x_key,
        "var_key": var_key,
        "obs_uid": _artifact_uid(obs_art),
    }


def build_variant(
    variant: Variant, *, source_dir: Path, output_root: Path, dry_run: bool
) -> dict[str, Any]:
    source_path = download_verified(variant, source_dir)
    source = ad.read_h5ad(source_path, backed="r")
    try:
        obs = build_canonical_obs(source.obs, dataset_id=variant.dataset_id)
        var = source.var.copy()
        if not obs.index.is_unique or not var.index.is_unique:
            raise ValueError("KOLF source obs/var indices must be unique")
        actual_denominator = target_denominator(obs)
        if actual_denominator != variant.expected_target_denominator:
            raise RuntimeError(
                f"target denominator mismatch for {variant.dataset_id}: "
                f"{actual_denominator} != {variant.expected_target_denominator}"
            )
        report: dict[str, Any] = {
            "dataset_id": variant.dataset_id,
            "prefix": variant.prefix,
            "source": {
                "article_id": FIGSHARE_ARTICLE,
                "doi": DOI,
                "file_id": variant.file_id,
                "filename": variant.filename,
                "size_bytes": variant.size_bytes,
                "md5": variant.md5,
                "license": LICENSE,
            },
            "shape": [int(source.n_obs), int(source.n_vars)],
            "target_denominator": actual_denominator,
            "control_cells": int(obs["is_control"].sum()),
            "perturbed_cells": int(obs["is_perturbed"].sum()),
            "planned_keys": candidate_keys(variant),
            "lamin_writes": 0,
        }
        if dry_run:
            return report
        x_path = prepare_x_candidate(source_path, output_root / variant.dataset_id)
    finally:
        source.file.close()
    ln = connect_pertdata()
    ln.track(path=__file__)
    report["triplet"] = register_triplet(ln, variant, obs, var, x_path)
    report["lamin_writes"] = 3
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    require_heavy_vm()
    reports = [
        build_variant(
            v,
            source_dir=args.source_dir,
            output_root=args.output_root,
            dry_run=args.dry_run,
        )
        for v in VARIANTS
    ]
    payload = {
        "schema": "pert-gym.kolf21j-candidates.v1",
        "dry_run": args.dry_run,
        "variants": reports,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
