from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import nbformat
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tools import curate_broad_prism_obs as curate


def test_publication_contract_is_owned_by_current_task() -> None:
    assert curate.TASK_ID == "t_cf959e37"


def test_processing_decision_notebook_executes_from_repo_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).parents[1]
    notebook_path = (
        root
        / "notebooks"
        / "datasets"
        / "broad_prism_repurposing_processing_decisions.ipynb"
    )
    notebook = nbformat.read(notebook_path, as_version=4)
    nbformat.validate(notebook)
    monkeypatch.chdir(root)
    namespace: dict[str, Any] = {}
    for cell in notebook.cells:
        if cell.cell_type == "code":
            exec(compile(cell.source, str(notebook_path), "exec"), namespace)
    contract = namespace["contract"]
    assert contract["identity"]["dataset_id"] == "broad_prism_repurposing"
    assert contract["identity"]["checksums"] == {
        "Repurposing_Public_24Q2_LFC.csv": curate.LFC_SHA256,
        "Repurposing_Public_24Q2_Treatment_Meta_Data.csv": curate.TREATMENT_SHA256,
        "primary-screen-cell-line-info.csv": curate.CELL_INFO_SHA256,
    }
    assert contract["validation"]["denominator"] == (
        "Legacy OBS/X 22,316,860; immutable LFC source rows 4,463,372; "
        "canonical denominator unresolved."
    )
    assert contract["processing_decisions"]["control_mapping"] == (
        "Source trt_cp, trt_poscon, and ctl_vehicle classes remain explicit; "
        "their canonical inclusion/control semantics are pending acceptance."
    )
    assert contract["reconstruction"]["safe_to_remove_gcs"] is False


def test_full_dod_assessment_remains_fail_closed() -> None:
    root = Path(__file__).parents[1]
    assessment = json.loads(
        (
            root
            / "artifacts"
            / "schema_audit"
            / "broad_prism_full_dod_assessment_20260812.json"
        ).read_text(encoding="utf-8")
    )
    assert assessment["status"] == "blocked_no_write"
    assert assessment["execution"]["writes_attempted"] == 0
    assert assessment["write_decision"] == "refused_fail_closed"
    assert assessment["source_contract"] == {
        "json_path": "artifacts/schema_audit/broad_prism_lfc_source_row_contract_20260711.json",
        "json_sha256": "238905ddb1a40578173a35bbe00346c8b495a6fcba9d9eb34353b229e2e78a53",
        "lfc_rows": curate.EXPECTED_SOURCE_ROWS,
        "lfc_sha256": curate.LFC_SHA256,
        "treatment_metadata_sha256": curate.TREATMENT_SHA256,
        "license_status": "unknown_pending_exact_source_license_evidence",
        "dose_unit_status": "unknown_source_does_not_bind_unit",
        "row_level_disease_status": "unknown_no_exact_row_level_evidence",
    }
    assert assessment["live_jkobject"]["obs"]["uid"] == (
        curate.EXPECTED_PREDECESSOR_OBS_UID
    )
    assert [gate["gate"] for gate in assessment["gates"]] == list(range(1, 14))


def test_sealed_curation_contract_matches_committed_source_and_live_evidence() -> None:
    root = Path(__file__).parents[1]
    source_contract = json.loads(
        (
            root
            / "artifacts"
            / "schema_audit"
            / "broad_prism_lfc_source_row_contract_20260711.json"
        ).read_text(encoding="utf-8")
    )

    assert (
        curate.EXPECTED_PREDECESSOR_OBS_UID
        == source_contract["current_lamin_obs_diagnostic"]["artifact_uid"]
    )
    assert curate.EXPECTED_SOURCE_ROWS == source_contract["denominator"]["source_rows"]
    assert (
        curate.LFC_SHA256
        == source_contract["authoritative_source_files"]["lfc"]["sha256"]
    )
    assert (
        curate.TREATMENT_SHA256
        == source_contract["authoritative_source_files"]["treatment_metadata"]["sha256"]
    )
    assert curate.FIELD_DISPOSITIONS["disease"].startswith("unknown:")
    assert curate.FIELD_DISPOSITIONS["dose_unit"].startswith("unknown:")


def _source_database(path: Path) -> None:
    database = sqlite3.connect(path)
    database.execute(
        "CREATE TABLE source_rows ("
        "source_index INTEGER PRIMARY KEY, row_id TEXT NOT NULL UNIQUE, "
        "profile_id TEXT NOT NULL, lfc TEXT, lfc_cb TEXT, pass_value TEXT NOT NULL)"
    )
    database.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?)",
        [
            (0, "ACH-000001::P1::W1::R1", "P1_120H_A", "-1.5", "-1.2", "TRUE"),
            (1, "ACH-000002::P2::W2::R2", "P2_120H_B", "0.5", "0.4", "FALSE"),
        ],
    )
    database.commit()
    database.close()


def _treatment(profile_id: str, name: str, treatment_type: str) -> dict[str, str]:
    return {
        "profile_id": profile_id,
        "prism_replicate": "replicate",
        "perturbation_well": "A01",
        "culture": "adherent",
        "perturbation_type": treatment_type,
        "dose": "2.5",
        "broad_id": f"BRD-{name}",
        "name": name,
        "compound_plate": "plate",
        "rep": "1",
        "screen": "primary",
    }


def test_expected_legacy_axis_layout() -> None:
    assert [curate.expected_source_index_and_role(i, 2) for i in range(10)] == [
        (0, "legacy_synthetic_control"),
        (1, "legacy_synthetic_control"),
        (0, "profile_id"),
        (0, "LFC"),
        (0, "LFC_cb"),
        (0, "PASS"),
        (1, "profile_id"),
        (1, "LFC"),
        (1, "LFC_cb"),
        (1, "PASS"),
    ]


def test_source_rows_for_indices_handles_control_field_wrap(tmp_path: Path) -> None:
    database_path = tmp_path / "source.sqlite"
    _source_database(database_path)
    database = sqlite3.connect(database_path)
    try:
        rows = curate.source_rows_for_indices(database, [1, 0, 0, 1])
    finally:
        database.close()
    assert sorted(rows) == [0, 1]
    assert rows[0][1] == "ACH-000001::P1::W1::R1"
    assert rows[1][1] == "ACH-000002::P2::W2::R2"


def test_build_candidate_preserves_axis_and_materializes_only_lfc(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_SOURCE_ROWS", 2)
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 10)
    source_database = tmp_path / "source.sqlite"
    _source_database(source_database)

    row_ids = [
        "ACH-000001::P1::W1::R1",
        "ACH-000002::P2::W2::R2",
        *(["ACH-000001::P1::W1::R1"] * 4),
        *(["ACH-000002::P2::W2::R2"] * 4),
    ]
    broad_ids = [None, None, *curate.SOURCE_FIELDS, *curate.SOURCE_FIELDS]
    predecessor = tmp_path / "predecessor.parquet"
    pq.write_table(
        pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "depmap_id": row_ids,
                    "broad_id": broad_ids,
                    "original_obs_index": [f"row-{i}" for i in range(10)],
                    "obs_uuid": [f"uuid-{i}" for i in range(10)],
                }
            ),
            preserve_index=False,
        ),
        predecessor,
        row_group_size=3,
    )
    output = tmp_path / "candidate.parquet"
    summary = curate.build_candidate(
        predecessor_path=predecessor,
        source_database_path=source_database,
        treatments={
            "P1_120H_A": _treatment("P1_120H_A", "drug-a", "trt_cp"),
            "P2_120H_B": _treatment("P2_120H_B", "vehicle", "ctl_vehicle"),
        },
        cell_lines={
            "ACH-000001": {
                "ccle_name": "CELL_A",
                "primary_tissue": "lung",
            }
        },
        output_path=output,
    )

    candidate = pq.read_table(output).to_pandas()
    assert summary["rows"] == 10
    assert summary["legacy_order_mismatch"] == 0
    assert summary["role_counts"]["LFC"] == 2
    assert candidate["original_obs_index"].tolist() == [f"row-{i}" for i in range(10)]
    assert candidate["source_row_id"].tolist() == row_ids
    assert candidate["source_field_role"].tolist() == [
        "legacy_synthetic_control",
        "legacy_synthetic_control",
        *curate.SOURCE_FIELDS,
        *curate.SOURCE_FIELDS,
    ]
    direct = candidate[candidate["source_field_role"] == "LFC"]
    assert direct["response_value"].tolist() == [-1.5, 0.5]
    assert direct["response_metric"].tolist() == ["lfc", "lfc"]
    assert pd.isna(candidate.loc[0, "response_value"])
    assert candidate.loc[3, "loader_projectable"]
    assert not candidate.loc[7, "loader_projectable"]
    assert candidate.loc[3, "timepoint"] == "7200"
    assert candidate.loc[3, "tissue_type"] == "lung"
    assert candidate.loc[7, "tissue_type"] == "unknown"
    assert set(candidate["disease"]) == {"unknown"}
    assert set(candidate["disease_state"]) == {"missing"}
    assert set(candidate["dose_unit"]) == {"unknown"}
    assert set(candidate["dose_unit_state"]) == {"missing"}
    assert candidate.loc[7, "is_control"]
    assert candidate.loc[3, "quality_flag"] == "accepted_lfc"
    assert candidate.loc[7, "quality_flag"] == "source_qc_failed"


def test_build_source_index_rejects_unknown_pass_with_physical_csv_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_SOURCE_ROWS", 1)
    source = tmp_path / "source.csv"
    source.write_text(
        "row_id,profile_id,LFC,LFC_cb,PASS\nACH-1::P1,P1,1.0,0.9,MAYBE\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="unknown PASS value 'MAYBE' at CSV row 2"):
        curate.build_source_index(source, tmp_path / "source.sqlite")


def test_unknown_treatment_type_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="unsupported treatment type 'mystery'"):
        curate.normalize_treatment_type("mystery")


def test_candidate_uses_one_based_physical_csv_row_numbers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_SOURCE_ROWS", 1)
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 5)
    monkeypatch.setattr(curate, "MATERIALIZATION_BATCH_ROWS", 2)
    source_database = tmp_path / "source.sqlite"
    database = sqlite3.connect(source_database)
    database.execute(
        "CREATE TABLE source_rows (source_index INTEGER PRIMARY KEY, "
        "row_id TEXT NOT NULL UNIQUE, profile_id TEXT NOT NULL, lfc TEXT, "
        "lfc_cb TEXT, pass_value TEXT NOT NULL)"
    )
    database.execute(
        "INSERT INTO source_rows VALUES (0, 'ACH-1::P1', 'P1_120H', '1', '1', 'TRUE')"
    )
    database.commit()
    database.close()
    predecessor = tmp_path / "predecessor.parquet"
    pq.write_table(
        pa.table(
            {
                "depmap_id": ["ACH-1::P1"] * 5,
                "broad_id": [None, *curate.SOURCE_FIELDS],
                "original_obs_index": [f"row-{index}" for index in range(5)],
                "obs_uuid": [f"uuid-{index}" for index in range(5)],
            }
        ),
        predecessor,
        row_group_size=5,
    )
    output = tmp_path / "candidate.parquet"

    summary = curate.build_candidate(
        predecessor_path=predecessor,
        source_database_path=source_database,
        treatments={"P1_120H": _treatment("P1_120H", "drug", "trt_cp")},
        cell_lines={},
        output_path=output,
    )

    candidate = pq.read_table(output).to_pandas()
    assert candidate["source_file_row_number"].tolist() == [2] * 5
    assert summary["materialization_batch_rows"] == 2
    assert summary["maximum_materialized_batch_rows"] <= 2


class _FakeFeatureValues:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self.values = values or {}

    def get_values(self) -> dict[str, object]:
        return dict(self.values)

    def set_values(self, values: dict[str, object]) -> None:
        self.values.update(values)


class _FakeArtifact:
    def __init__(
        self,
        *,
        uid: str,
        key: str,
        path: Path,
        hash_value: str,
        n_observations: int | None = None,
        features: dict[str, object] | None = None,
        manager: "_FakeArtifactManager | None" = None,
    ) -> None:
        self.uid = uid
        self.key = key
        self.path = path
        self.hash = hash_value
        self.size = path.stat().st_size
        self.n_observations = n_observations
        self.features = _FakeFeatureValues(features)
        self.manager = manager

    def cache(self) -> Path:
        return self.path

    def load(self) -> pd.DataFrame:
        return pd.DataFrame()

    def save(self) -> "_FakeArtifact":
        assert self.manager is not None
        self.manager.save_count += 1
        self.manager.artifacts[self.uid] = self
        self.manager.current_by_key[self.key] = self
        if self.manager.fail_after_save:
            self.manager.fail_after_save = False
            raise RuntimeError("simulated crash after candidate save")
        return self


class _FakeFilter:
    def __init__(self, values: list[Any]) -> None:
        self.values = values

    def count(self) -> int:
        return len(self.values)

    def exists(self) -> bool:
        return bool(self.values)

    def all(self) -> list[object]:
        return list(self.values)


class _FakeArtifactManager:
    def __init__(self) -> None:
        self.artifacts: dict[str, _FakeArtifact] = {}
        self.current_by_key: dict[str, _FakeArtifact] = {}
        self.save_count = 0
        self.fail_after_save = True

    def add(self, artifact: _FakeArtifact) -> None:
        self.artifacts[artifact.uid] = artifact
        self.current_by_key[artifact.key] = artifact

    def get(self, *, key: str | None = None, uid: str | None = None) -> _FakeArtifact:
        if key is not None:
            return self.current_by_key[key]
        assert uid is not None
        return self.artifacts[uid]

    def filter(self, *, key: str | None = None, uid: str | None = None) -> _FakeFilter:
        values = list(self.artifacts.values())
        if key is not None:
            values = [value for value in values if value.key == key]
        if uid is not None:
            values = [value for value in values if value.uid == uid]
        return _FakeFilter(values)

    def count(self) -> int:
        return len(self.artifacts)

    def from_dataframe(
        self,
        path: Path,
        *,
        key: str,
        revises: _FakeArtifact,
        description: str,
    ) -> _FakeArtifact:
        del revises, description
        return _FakeArtifact(
            uid="candidate-uid",
            key=key,
            path=path,
            hash_value="candidate-native-hash",
            manager=self,
        )


class _FakeCollectionManager:
    def __init__(self) -> None:
        self.collections: list[object] = []

    def filter(self) -> _FakeFilter:
        return _FakeFilter(self.collections)


class _FakeCollectionArtifacts:
    def __init__(self, artifacts: list[_FakeArtifact]) -> None:
        self.values = artifacts

    def all(self) -> list[_FakeArtifact]:
        return list(self.values)

    def filter(self, *, uid: str) -> _FakeFilter:
        return _FakeFilter([value for value in self.values if value.uid == uid])


class _FakeCollection:
    def __init__(
        self,
        *,
        uid: str,
        artifacts: list[_FakeArtifact],
        hash_value: str = "collection-hash",
    ) -> None:
        self.uid = uid
        self.name = "collection"
        self.version = "1"
        self.hash = hash_value
        self.artifacts = _FakeCollectionArtifacts(artifacts)


class _FakeLamin:
    def __init__(self, *, obs: Path, candidate: Path) -> None:
        del candidate
        self.Artifact = _FakeArtifactManager()
        self.Collection = _FakeCollectionManager()
        var_path = obs.parent / "var.parquet"
        pq.write_table(pa.table({"empty": pa.array([], type=pa.string())}), var_path)
        self.var = _FakeArtifact(
            uid="var-uid",
            key=curate.VAR_KEY,
            path=var_path,
            hash_value="var-hash",
        )
        x_path = obs.parent / "X.h5ad"
        x_path.write_bytes(b"x")
        self.x = _FakeArtifact(
            uid="x-uid",
            key=curate.X_KEY,
            path=x_path,
            hash_value="x-hash",
            n_observations=2,
            features={"var": self.var},
        )
        self.obs = _FakeArtifact(
            uid="predecessor-uid",
            key=curate.OBS_KEY,
            path=obs,
            hash_value="obs-hash",
            features={"X": self.x},
        )
        for artifact in (self.obs, self.x, self.var):
            self.Artifact.add(artifact)
        self.setup = SimpleNamespace(
            settings=SimpleNamespace(
                instance=SimpleNamespace(slug="laminlabs/pertdata"),
                branch=SimpleNamespace(name="jkobject"),
            )
        )
        self.track_count = 0

    def track(self, **kwargs: object) -> None:
        del kwargs
        self.track_count += 1


def _publication_fixture(tmp_path: Path) -> tuple[_FakeLamin, dict[str, object], Path]:
    predecessor = tmp_path / "predecessor.parquet"
    candidate = tmp_path / "candidate.parquet"
    frame = pa.table(
        {
            "obs_uuid": ["uuid-1", "uuid-2"],
            "original_obs_index": ["row-1", "row-2"],
            "source_row_identifier": ["source-1", "source-2"],
            "source_field_role": ["LFC", "LFC"],
            "response_metric": ["lfc", "lfc"],
            "response_value": [1.0, 2.0],
            "quality_flag": ["accepted_lfc", "accepted_lfc"],
            "cell_line": ["ACH-1", "ACH-2"],
            "perturbation": ["drug-1", "drug-2"],
            "dose": ["1", "2"],
        }
    )
    pq.write_table(frame, predecessor)
    pq.write_table(frame, candidate)
    lamin = _FakeLamin(obs=predecessor, candidate=candidate)
    predecessor_snapshot = curate.inspect_lamin(lamin)["snapshot"]
    plan: dict[str, object] = {
        "format": "pert-gym.broad-prism-obs-plan.v1",
        "task_id": curate.TASK_ID,
        "dataset_id": curate.DATASET_ID,
        "code_commit": "code-commit",
        "code_script_sha256": "script-sha",
        "sources": curate.expected_source_provenance(),
        "predecessor": predecessor_snapshot,
        "candidate": {
            "path": str(candidate),
            "sha256": curate.sha256_file(candidate),
            "ordered_identity_sha256": curate.ordered_identity_sha256(candidate),
        },
        "var_verdict": predecessor_snapshot["var"],
    }
    plan["plan_sha256"] = curate.canonical_json_sha256(plan)
    run_root = tmp_path / "run"
    run_root.mkdir()
    (run_root / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    authorization = {
        "format": "pert-gym.broad-prism-obs-authorization.v1",
        "task_id": curate.TASK_ID,
        "dataset_id": curate.DATASET_ID,
        "approved": True,
        "plan_sha256": plan["plan_sha256"],
        "candidate_sha256": plan["candidate"]["sha256"],
        "predecessor_obs_uid": predecessor_snapshot["obs"]["uid"],
        "predecessor_x_uid": predecessor_snapshot["X"]["uid"],
        "predecessor_var_uid": predecessor_snapshot["var"]["uid"],
    }
    authorization_path = run_root / "authorization.json"
    authorization_path.write_text(json.dumps(authorization), encoding="utf-8")
    return lamin, plan, authorization_path


def test_write_recovers_crash_after_candidate_save_without_duplicate_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )

    with pytest.raises(RuntimeError, match="simulated crash after candidate save"):
        curate.run_write(run_root, authorization)

    assert (run_root / "write_journal.json").exists()
    assert not (run_root / "write_receipt.json").exists()
    assert lamin.Artifact.save_count == 1

    recovered = curate.run_write(run_root, authorization)
    assert recovered["recovery"]["candidate_was_adopted"] is True
    assert lamin.Artifact.save_count == 1
    assert (run_root / "write_receipt.json").exists()

    replay = curate.run_write(run_root, authorization)
    assert replay["replay"]["writes"] == 0
    assert replay["replay"]["status"] == "verified_no_op"
    assert lamin.Artifact.save_count == 1
    assert lamin.track_count == 0


def test_verify_rejects_tampered_or_missing_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    receipt_path = run_root / "write_receipt.json"
    sealed = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt_path.unlink()

    with pytest.raises(RuntimeError, match="write receipt is missing"):
        curate.run_verify(run_root)

    sealed["unrelated_drift"] = 99
    receipt_path.write_text(json.dumps(sealed), encoding="utf-8")
    with pytest.raises(RuntimeError, match="write receipt digest mismatch"):
        curate.run_verify(run_root)


def test_replay_and_verify_require_digest_bound_terminal_journal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    journal_path = run_root / "write_journal.json"
    sealed = json.loads(journal_path.read_text(encoding="utf-8"))
    journal_path.unlink()

    with pytest.raises(RuntimeError, match="terminal write journal is missing"):
        curate.run_verify(run_root)
    with pytest.raises(RuntimeError, match="terminal write journal is missing"):
        curate.run_write(run_root, authorization)

    sealed["receipt_sha256"] = "tampered"
    journal_path.write_text(json.dumps(sealed), encoding="utf-8")
    with pytest.raises(RuntimeError, match="write journal digest mismatch"):
        curate.run_verify(run_root)


def test_verify_rejects_coordinated_resealed_receipt_and_live_x_tamper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    rogue_path = tmp_path / "rogue-x.h5ad"
    rogue_path.write_bytes(b"rogue")
    rogue = _FakeArtifact(
        uid="rogue-x-uid",
        key=curate.X_KEY,
        path=rogue_path,
        hash_value="rogue-x-hash",
        n_observations=2,
        features={"var": lamin.var},
    )
    lamin.Artifact.add(rogue)
    current_obs = lamin.Artifact.get(key=curate.OBS_KEY)
    current_obs.features.set_values({"X": rogue})
    receipt_path = run_root / "write_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["preserved_x_uid"] = rogue.uid
    receipt["preserved_x_identity"] = curate.artifact_identity(rogue)
    receipt["post_registry"] = curate.registry_snapshot(lamin)
    receipt = curate.seal_evidence(receipt, "receipt_sha256")
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    journal_path = run_root / "write_journal.json"
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    journal["receipt_sha256"] = receipt["receipt_sha256"]
    journal = curate.seal_evidence(journal, "journal_sha256")
    journal_path.write_text(json.dumps(journal), encoding="utf-8")

    with pytest.raises(RuntimeError, match="authorized X artifact identity mismatch"):
        curate.run_verify(run_root)


def test_verify_detects_collection_or_unrelated_registry_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    unrelated_path = tmp_path / "unrelated.txt"
    unrelated_path.write_text("drift", encoding="utf-8")
    lamin.Artifact.add(
        _FakeArtifact(
            uid="unrelated-uid",
            key="unrelated/key",
            path=unrelated_path,
            hash_value="unrelated-hash",
        )
    )

    with pytest.raises(RuntimeError, match="artifact registry drift"):
        curate.run_verify(run_root)


def test_verify_detects_collection_membership_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    lamin.Collection.collections.append(
        _FakeCollection(uid="collection-uid", artifacts=[lamin.x])
    )

    with pytest.raises(RuntimeError, match="Collection registry or membership drift"):
        curate.run_verify(run_root)


def test_verify_detects_preserved_x_hash_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    lamin.x.hash = "tampered-x-hash"

    with pytest.raises(RuntimeError, match="preserved X artifact identity mismatch"):
        curate.run_verify(run_root)


def test_verify_detects_preserved_x_storage_path_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    run_root = authorization.parent
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(run_root, authorization)
    lamin.x.path = tmp_path / "moved-X.h5ad"

    with pytest.raises(RuntimeError, match="preserved X artifact identity mismatch"):
        curate.run_verify(run_root)


def test_verify_detects_collection_hash_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 2)
    lamin, _plan, authorization = _publication_fixture(tmp_path)
    collection = _FakeCollection(uid="collection-uid", artifacts=[lamin.x])
    lamin.Collection.collections.append(collection)
    plan = json.loads((authorization.parent / "plan.json").read_text(encoding="utf-8"))
    snapshot = curate.inspect_lamin(lamin)["snapshot"]
    plan["predecessor"] = snapshot
    plan["var_verdict"] = snapshot["var"]
    plan["plan_sha256"] = curate.canonical_json_sha256(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    )
    (authorization.parent / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    auth = json.loads(authorization.read_text(encoding="utf-8"))
    auth["plan_sha256"] = plan["plan_sha256"]
    authorization.write_text(json.dumps(auth), encoding="utf-8")
    lamin.Artifact.fail_after_save = False
    monkeypatch.setattr(curate, "require_eu_worker", lambda: None)
    monkeypatch.setattr(curate, "connect_pertdata", lambda: lamin)
    monkeypatch.setattr(
        curate,
        "current_code_identity",
        lambda: {"code_commit": "code-commit", "code_script_sha256": "script-sha"},
    )
    curate.run_write(authorization.parent, authorization)
    collection.hash = "drifted-collection-hash"

    with pytest.raises(RuntimeError, match="Collection registry or membership drift"):
        curate.run_verify(authorization.parent)


def test_plan_integrity_rejects_unpinned_code_or_sources() -> None:
    plan = {
        "format": "pert-gym.broad-prism-obs-plan.v1",
        "task_id": curate.TASK_ID,
        "dataset_id": curate.DATASET_ID,
        "code_commit": "wrong",
        "code_script_sha256": "script-sha",
        "sources": {},
    }
    plan["plan_sha256"] = curate.canonical_json_sha256(plan)

    with pytest.raises(RuntimeError, match="code provenance mismatch"):
        curate.validate_plan_integrity(
            plan,
            current_identity={
                "code_commit": "code-commit",
                "code_script_sha256": "script-sha",
            },
        )


@pytest.mark.parametrize(
    ("source", "field", "replacement"),
    [
        ("lfc", "uri", "gs://wrong/source.csv"),
        ("lfc", "name", "wrong/source.csv"),
        ("lfc", "bytes", 1),
        ("treatment_metadata", "uri", "gs://wrong/treatment.csv"),
        ("cell_line_metadata", "url", "https://example.invalid/wrong.csv"),
        ("cell_line_metadata", "figshare_file_id", 1),
        ("cell_line_metadata", "bytes", 1),
        ("cell_line_metadata", "md5", "bad-md5"),
    ],
)
def test_plan_integrity_rejects_any_source_tuple_drift(
    source: str, field: str, replacement: object
) -> None:
    plan: dict[str, object] = {
        "format": "pert-gym.broad-prism-obs-plan.v1",
        "task_id": curate.TASK_ID,
        "dataset_id": curate.DATASET_ID,
        "code_commit": "code-commit",
        "code_script_sha256": "script-sha",
        "sources": curate.expected_source_provenance(),
    }
    sources = plan["sources"]
    assert isinstance(sources, dict)
    source_payload = sources[source]
    assert isinstance(source_payload, dict)
    source_payload[field] = replacement
    plan["plan_sha256"] = curate.canonical_json_sha256(plan)

    with pytest.raises(RuntimeError, match="source provenance mismatch"):
        curate.validate_plan_integrity(
            plan,
            current_identity={
                "code_commit": "code-commit",
                "code_script_sha256": "script-sha",
            },
        )


def test_atomic_evidence_write_fsyncs_parent_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    opened_directories: list[int] = []
    fsynced: list[int] = []
    real_fsync = curate.os.fsync
    real_open = curate.os.open

    def recording_fsync(fd: int) -> None:
        fsynced.append(fd)
        real_fsync(fd)

    def recording_open(path: Path, flags: int, mode: int = 0o777) -> int:
        fd = real_open(path, flags, mode)
        if Path(path) == tmp_path:
            opened_directories.append(fd)
        return fd

    monkeypatch.setattr(curate.os, "open", recording_open)
    monkeypatch.setattr(curate.os, "fsync", recording_fsync)
    curate.write_json_atomic(tmp_path / "journal.json", {"state": "authorized"})

    assert opened_directories
    assert set(opened_directories) <= set(fsynced)


def test_product_execution_heartbeat_publishes_exact_payload_pid(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    heartbeat = tmp_path / "payload.heartbeat"
    monkeypatch.setenv("PERT_GYM_PAYLOAD_HEARTBEAT_PATH", str(heartbeat))
    monkeypatch.setattr(curate.time, "time", lambda: 1234.9)

    curate.emit_heartbeat("writing", 1, 2, 3)

    assert heartbeat.read_text(encoding="ascii") == f"{curate.os.getpid()} 1234\n"
    assert not heartbeat.with_suffix(".heartbeat.partial").exists()


def test_gcs_source_replaces_corrupt_completed_and_partial_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    destination = tmp_path / "source.csv"
    destination.write_bytes(b"bad")
    destination.with_suffix(".csv.partial").write_bytes(b"stale")
    expected = b"new"
    monkeypatch.setattr(
        curate,
        "describe_gcs",
        lambda uri: {"name": uri, "size": len(expected), "generation": "7"},
    )

    def copy(command: list[str]) -> str:
        Path(command[4]).write_bytes(expected)
        return ""

    monkeypatch.setattr(curate, "run_checked", copy)

    receipt = curate.materialize_gcs_source(
        "gs://bucket/source.csv",
        destination,
        expected_generation="7",
        expected_size=len(expected),
        expected_sha256=curate.hashlib.sha256(expected).hexdigest(),
    )

    assert destination.read_bytes() == expected
    assert receipt["sha256"] == curate.hashlib.sha256(expected).hexdigest()
    assert not destination.with_suffix(".csv.partial").exists()


def test_http_source_replaces_corrupt_completed_and_partial_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    destination = tmp_path / "source.csv"
    destination.write_bytes(b"bad")
    destination.with_suffix(".csv.partial").write_bytes(b"stale")
    expected = b"new"
    monkeypatch.setattr(
        curate.urllib.request,
        "urlopen",
        lambda url, timeout: io.BytesIO(expected),
    )

    receipt = curate.materialize_http_source(
        "https://example.invalid/source.csv",
        destination,
        expected_sha256=curate.hashlib.sha256(expected).hexdigest(),
        expected_md5=curate.hashlib.md5(expected, usedforsecurity=False).hexdigest(),
    )

    assert destination.read_bytes() == expected
    assert receipt["sha256"] == curate.hashlib.sha256(expected).hexdigest()
    assert not destination.with_suffix(".csv.partial").exists()


def test_initial_predecessor_identity_is_exact() -> None:
    snapshot = {
        "obs": {"uid": "wrong", "key": curate.OBS_KEY},
        "var": {"uid": curate.EXPECTED_VAR_UID, "key": curate.VAR_KEY},
    }

    with pytest.raises(
        RuntimeError, match="expected predecessor OBS identity mismatch"
    ):
        curate.validate_expected_predecessor(snapshot)
