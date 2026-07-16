from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
import sqlite3
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parents[1]
REVIEW = ROOT / "artifacts/review/temporal-v4-099-parquet-parity-v1"
CONTRACT_PATH = REVIEW / "writer_contract.py"
ROW_99_CONFIG = REVIEW / "row-99-config.json"
ROW_99_AUTHORIZATION = REVIEW / "authorization.json"
ROW_13_CONFIG = REVIEW / "row-13-prewrite-fixture.json"
ROW_7_CONFIG = REVIEW / "row-7-config.json"
ROW_7_AUTHORIZATION = REVIEW / "row-7-authorization.json"
ROW_55_CONFIG = REVIEW / "row-55-config.json"
ROW_55_AUTHORIZATION = REVIEW / "row-55-authorization.json"
WRITER = REVIEW / "write_component.py"
HELPER = REVIEW / "parquet_frame_parity.py"
LEDGER_HELPER = REVIEW / "live_ledger_control_plane.py"
ROW_7_SOURCE_VERSION_ID = "YipoWdKvKlXR9hHaku1ta0iAGqNr8w_m"
ROW_55_SOURCE_VERSION_ID = "xupxdGsz5yjNY6W.oCuNxd8V0_f1vpAA"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_contract_module():
    spec = importlib.util.spec_from_file_location("cellxgene_writer_contract", CONTRACT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_writer_module():
    sys.path.insert(0, str(REVIEW))
    try:
        spec = importlib.util.spec_from_file_location("cellxgene_writer", WRITER)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(REVIEW))


def load_ledger_helper_module():
    spec = importlib.util.spec_from_file_location(
        "cellxgene_live_ledger_control_plane", LEDGER_HELPER
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def bound_authorization(config: dict[str, object]) -> dict[str, object]:
    authorization = load_json(ROW_99_AUTHORIZATION)
    authorization["config_sha256"] = hashlib.sha256(
        (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest()
    authorization["writer_sha256"] = sha256(WRITER)
    authorization["writer_contract_sha256"] = sha256(CONTRACT_PATH)
    authorization["parquet_frame_parity_sha256"] = sha256(HELPER)
    authorization["protocol"] = config["protocol"]
    return authorization


def validate(config: dict[str, object], authorization: dict[str, object]):
    contract = load_contract_module()
    return contract.validate_bound_contract(
        config,
        authorization,
        config_sha256=hashlib.sha256(
            (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
        ).hexdigest(),
        writer_sha256=sha256(WRITER),
        helper_sha256=sha256(HELPER),
        contract_sha256=sha256(CONTRACT_PATH),
        ledger_helper_sha256=sha256(LEDGER_HELPER),
    )


def test_row_99_reviewed_fixture_validates_with_current_writer_hash_binding() -> None:
    config = load_json(ROW_99_CONFIG)
    frozen_authorization = load_json(ROW_99_AUTHORIZATION)
    authorization = bound_authorization(config)

    validated = validate(config, authorization)

    assert validated.config["shape"] == [10224, 35552]
    assert validated.config["accepted_components"] == {
        "current": 2,
        "denominator": 153,
        "credit": 0,
    }
    assert frozen_authorization["config_sha256"] == sha256(ROW_99_CONFIG)
    assert frozen_authorization["writer_sha256"] == (
        "f40409ce46393db8c713a6a4b428f2c98c0102031c1c06e70042b0f96504a723"
    )
    assert authorization["writer_sha256"] == sha256(WRITER)
    assert authorization["writer_contract_sha256"] == sha256(CONTRACT_PATH)
    assert authorization["parquet_frame_parity_sha256"] == sha256(HELPER)
    assert authorization["execution_authorized"] is False
    assert str(config["logical_key"]).endswith(
        "cell_culture_differentiation_and_proliferation_conditions_influence_the_in_vitro"
    )


def test_distinct_row_13_fixture_reaches_generic_preflight_boundary() -> None:
    contract = load_contract_module()
    config = load_json(ROW_13_CONFIG)
    authorization = bound_authorization(config)
    authorization["execution_authorized"] = False
    binding = config["authorization_binding"]
    assert isinstance(binding, dict)
    assert binding["parent_task_id"] == "t_0c090cdd"
    assert binding["parent_task_id"] != config["task_id"]
    assert binding["correction_task_id"] == config["task_id"] == "t_b8dacd9d"
    authorization.update(binding)

    validated = validate(config, authorization)
    plan = contract.preflight_plan(validated)

    assert plan["source_url"].endswith("ba404fa2-44a9-4420-b43c-03f863b12d37.h5ad")
    assert plan["shape"] == [14424, 37434]
    assert plan["logical_key"].endswith(
        "cerebellar_organoid_using_microfluidics_and_combinatorial_barcoding_based_techno"
    )
    assert plan["accepted_components"] == {"current": 3, "denominator": 153}
    assert plan["execution_authorized"] is False
    with pytest.raises(RuntimeError, match="not execution-authorized"):
        contract.require_execution_authorized(validated)


def test_row_7_exact_contract_is_intrinsically_bound_and_execution_authorized() -> None:
    contract = load_contract_module()
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    validated = contract.load_bound_contract(
        ROW_7_CONFIG,
        ROW_7_AUTHORIZATION,
        writer_path=WRITER,
        helper_path=HELPER,
        require_execution=True,
    )

    assert config["catalogue_record"] == "temporal_v4_007_a_novel_human_fetal_lung_derived_alveolar_organoid_model_reveals_mechanisms_of_s"
    assert config["revision"]["prefix"] == "temporal-v4-007"
    assert config["shape"] == [9619, 35461]
    source_head = config["source_head"]
    assert isinstance(source_head, dict)
    assert source_head["version_id"] == ROW_7_SOURCE_VERSION_ID
    assert config["accepted_components"] == {
        "metric": "accepted_components",
        "current": 3,
        "denominator": 153,
        "credit": 0,
    }
    assert config["ordered_var"]["identity_sha256"] == "runtime-computed-before-candidate-write"
    assert authorization["config_sha256"] == sha256(ROW_7_CONFIG)
    assert authorization["writer_sha256"] == sha256(WRITER)
    assert authorization["writer_contract_sha256"] == sha256(CONTRACT_PATH)
    assert authorization["parquet_frame_parity_sha256"] == sha256(HELPER)
    assert authorization["execution_authorized"] is True
    assert validated.config == config


def test_row_7_stale_snapshot_does_not_bind_live_ledger_current() -> None:
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    config["accepted_components"]["current"] = 4
    authorization["config_sha256"] = hashlib.sha256(
        (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest()
    authorization["writer_sha256"] = sha256(WRITER)
    authorization["writer_contract_sha256"] = sha256(CONTRACT_PATH)

    validated = validate(config, authorization)

    assert validated.config["accepted_components"]["current"] == 4


def test_row_55_exact_mouse_contract_is_intrinsically_bound_and_authorized() -> None:
    contract = load_contract_module()
    config = load_json(ROW_55_CONFIG)
    authorization = load_json(ROW_55_AUTHORIZATION)
    validated = contract.load_bound_contract(
        ROW_55_CONFIG,
        ROW_55_AUTHORIZATION,
        writer_path=WRITER,
        helper_path=HELPER,
        require_execution=True,
    )

    assert config["catalogue_record"] == "temporal_v4_055_type_i_interferon_responsive_microglia_shape_cortical_development_and_behavior"
    assert config["logical_key"] == "pert-gym/logical/temporal/type_i_interferon_responsive_microglia_shape_cortical_development_and_behavior"
    assert config["revision"]["prefix"] == "temporal-v4-055"
    assert config["shape"] == [12330, 22835]
    assert config["api_identity"]["organism"] == {
        "label": "Mus musculus",
        "ontology_term_id": "NCBITaxon:10090",
    }
    assert config["source_head"]["version_id"] == ROW_55_SOURCE_VERSION_ID
    assert config["accepted_components"] == {
        "metric": "accepted_components", "current": 4, "denominator": 153, "credit": 0,
    }
    assert config["storage"] == {
        "gcs_root": "scperturb/pert-gym/staging", "manifest_last": True,
        "per_block_var_count": 0, "shared_var_count": 1, "x_logical_object_count": 1,
    }
    assert authorization["live_ledger_control_plane_sha256"] == sha256(LEDGER_HELPER)
    assert validated.config == config


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda c, a: c.__setitem__("catalogue_record", "temporal_v4_055_wrong"), "catalogue_record conflicts"),
        (lambda c, a: (c.__setitem__("logical_key", "pert-gym/logical/temporal/rebound"), c["obs"]["assignments"][0].__setitem__("value", "pert-gym/logical/temporal/rebound")), "logical_key conflicts"),
        (lambda c, a: c["source"].__setitem__("collection_version_id", "00000000-0000-0000-0000-000000000000"), "source collection_version_id conflicts"),
        (lambda c, a: c["source"].__setitem__("dataset_version_id", "00000000-0000-0000-0000-000000000000"), "dataset_version_id conflicts"),
        (lambda c, a: c["source_head"].__setitem__("version_id", "rebound-version"), "source_head conflicts"),
        (lambda c, a: a.__setitem__("live_ledger_control_plane_sha256", "0" * 64), "live-ledger helper SHA-256"),
    ],
)
def test_row_55_rebinding_has_zero_side_effects(monkeypatch, tmp_path: Path, mutation, match: str) -> None:
    writer = load_writer_module()
    config = load_json(ROW_55_CONFIG)
    authorization = load_json(ROW_55_AUTHORIZATION)
    mutation(config, authorization)
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(sys, "argv", [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)])

    with pytest.raises((RuntimeError, ValueError), match=match):
        writer.main()
    assert calls == []


def test_row_55_mouse_ordered_var_uses_mouse_ensembl_namespace() -> None:
    writer = load_writer_module()
    writer.ACTIVE_CONFIG = load_json(ROW_55_CONFIG)
    writer.N_VARS = 2
    source = pd.DataFrame(
        {"feature_reference": ["NCBITaxon:10090"] * 2, "feature_name": ["Gene1", "Gene2"]},
        index=["ENSMUSG00000000001", "ENSMUSG00000000002"],
    )
    _var, identity = writer.map_var(source, "Mus musculus")
    expected = {
        "organism_ontology_id": "NCBITaxon:10090", "canonical_feature_namespace": "Ensembl Gene ID",
        "normalization_version": "source-string/v1", "n_vars": 2,
        "ordered_canonical_feature_identifiers": list(source.index),
    }
    assert identity == hashlib.sha256(json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def test_row_55_reads_live_ledger_under_writer_leases(monkeypatch, tmp_path: Path) -> None:
    writer = load_writer_module()
    config = load_json(ROW_55_CONFIG)
    authorization = load_json(ROW_55_AUTHORIZATION)
    authorization["writer_sha256"] = sha256(WRITER)
    authorization["writer_contract_sha256"] = sha256(CONTRACT_PATH)
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    events: list[object] = []

    @contextmanager
    def leases(_metadata):
        events.append("lease-enter")
        try:
            yield 123.0
        finally:
            events.append("lease-exit")

    live = {"metric": "accepted_components", "current": 4, "denominator": 153, "source": "hermes-kanban-completed-product-deltas/v1"}
    monkeypatch.setattr(writer, "acquired_writer_leases", leases)
    monkeypatch.setattr(writer, "read_live_accepted_components_ledger", lambda: events.append("live-ledger") or live)
    monkeypatch.setattr(writer, "_execute_authorized_contract", lambda *args, **kwargs: events.append(("execute", kwargs["lease_acquired"], kwargs["live_ledger"])) or 0)
    monkeypatch.setattr(writer.socket, "gethostname", lambda: "pert-gym-worker-eu")
    monkeypatch.setattr(writer, "mem_available", lambda: 4 * 1024**3)
    monkeypatch.setattr(sys, "argv", [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)])

    assert writer.main() == 0
    assert events == ["lease-enter", "live-ledger", ("execute", 123.0, live), "lease-exit"]


def immutable_manifest(revision: str = "temporal-v4-013-test") -> dict[str, str]:
    uri = (
        "gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/test/"
        f"revisions/{revision}/manifest.json"
    )
    return {"uri": uri, "generation": "1784150946597762", "sha256": "9" * 64}


def write_live_ledger_db(path: Path, records: list[object]) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL);
        CREATE TABLE task_runs (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            status TEXT NOT NULL,
            outcome TEXT,
            ended_at INTEGER,
            metadata TEXT
        );
        """
    )
    for index, record in enumerate(records, start=1):
        task_id = f"t_credit_{index}"
        metadata = {"product_delta": {"metrics": {"accepted_components": record}}}
        if index == len(records) and isinstance(record, dict):
            metadata["manifest"] = {
                **immutable_manifest(),
                "uri": record.get("live_readback"),
            }
        connection.execute("INSERT INTO tasks VALUES (?, 'done')", (task_id,))
        connection.execute(
            "INSERT INTO task_runs VALUES (?, ?, 'done', 'completed', ?, ?)",
            (index, task_id, index, json.dumps(metadata)),
        )
    connection.commit()
    connection.close()


def accepted_components_delta(before: object, after: object) -> dict[str, object]:
    manifest = immutable_manifest()
    return {
        "before": before,
        "after": after,
        "denominator": 153,
        "unit": "components",
        "mismatch": 0,
        "live_readback": manifest["uri"],
    }


def response_fixture(writer, *, current: int = 4, issued_at: float = 100.0):
    chain = [
        {"task_id": "t_zero", "run_id": 1, "ended_at": 1, "before": 0, "after": 0}
    ]
    for value in range(1, current + 1):
        chain.append(
            {
                "task_id": f"t_credit_{value}",
                "run_id": value + 1,
                "ended_at": value + 1,
                "before": value - 1,
                "after": value,
            }
        )
    manifest = immutable_manifest(f"temporal-v4-{current:03d}-test")
    return {
        "protocol": writer.LEDGER_PROTOCOL,
        "board": "pert-gym",
        "metric": "accepted_components",
        "request_nonce": "a" * 64,
        "issued_at": issued_at,
        "chain": chain,
        "latest_owner": {
            **chain[-1],
            "current": current,
            "denominator": 153,
            "unit": "components",
            "mismatch": 0,
            "live_readback": manifest["uri"],
            "manifest": manifest,
        },
    }


def test_control_plane_helper_uses_latest_completed_delta(
    monkeypatch, tmp_path: Path
) -> None:
    helper = load_ledger_helper_module()
    database = tmp_path / "kanban.db"
    write_live_ledger_db(
        database,
        [
            accepted_components_delta(0, 0),
            accepted_components_delta(0, 1),
            accepted_components_delta(1, 2),
            accepted_components_delta(2, 3),
            accepted_components_delta(3, 4),
        ],
    )
    monkeypatch.setattr(helper, "DATABASE", database)

    observed = helper.build_response("a" * 64, issued_at=100.0)

    assert observed["metric"] == "accepted_components"
    assert observed["latest_owner"]["current"] == 4
    assert observed["latest_owner"]["denominator"] == 153
    assert observed["latest_owner"]["manifest"] == immutable_manifest()


@pytest.mark.parametrize(
    "record",
    [
        None,
        {},
        accepted_components_delta(3, "4"),
        accepted_components_delta(-1, 0),
        accepted_components_delta(153, 154),
        {**accepted_components_delta(3, 4), "denominator": 152},
        {**accepted_components_delta(3, 4), "metric": "wrong"},
    ],
)
def test_control_plane_helper_rejects_malformed_records(
    monkeypatch, tmp_path: Path, record: object
) -> None:
    helper = load_ledger_helper_module()
    database = tmp_path / "kanban.db"
    write_live_ledger_db(database, [record])
    monkeypatch.setattr(helper, "DATABASE", database)

    with pytest.raises(RuntimeError, match="accepted-components"):
        helper.build_response("a" * 64)


def test_control_plane_helper_rejects_unavailable_ledger(
    monkeypatch, tmp_path: Path
) -> None:
    helper = load_ledger_helper_module()
    monkeypatch.setattr(helper, "DATABASE", tmp_path / "missing.db")

    with pytest.raises((RuntimeError, OSError)):
        helper.build_response("a" * 64)


def test_writer_accepts_fresh_immutable_live_4_and_later_live_5() -> None:
    writer = load_writer_module()
    for current in (4, 5):
        observed = writer._validate_live_ledger_response(
            response_fixture(writer, current=current),
            nonce="a" * 64,
            requested_at=99.0,
            received_at=101.0,
        )
        assert observed["current"] == current
        assert observed["denominator"] == 153
        assert observed["manifest_sha256"] == "9" * 64


def test_loopback_helper_enforces_bearer_and_writer_fetches_one_fresh_response(
    monkeypatch, tmp_path: Path
) -> None:
    helper = load_ledger_helper_module()
    writer = load_writer_module()
    database = tmp_path / "kanban.db"
    write_live_ledger_db(
        database,
        [
            accepted_components_delta(0, 0),
            accepted_components_delta(0, 1),
            accepted_components_delta(1, 2),
            accepted_components_delta(2, 3),
            accepted_components_delta(3, 4),
        ],
    )
    monkeypatch.setattr(helper, "DATABASE", database)
    token = "ephemeral-test-token-which-is-never-persisted"
    server = helper._Server(("127.0.0.1", 0), token)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}{helper.REQUEST_PATH}"
    monkeypatch.setenv("PERT_GYM_LEDGER_URL", endpoint)
    monkeypatch.setenv("PERT_GYM_LEDGER_BEARER_TOKEN", "wrong-token-with-at-least-thirty-two-bytes")

    try:
        with pytest.raises(RuntimeError, match="loopback request failed"):
            writer.read_live_accepted_components_ledger()

        monkeypatch.setenv("PERT_GYM_LEDGER_BEARER_TOKEN", token)
        observed = writer.read_live_accepted_components_ledger()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert observed["current"] == 4
    assert observed["task_id"] == "t_credit_5"
    assert observed["manifest_generation"].isdigit()
    assert not thread.is_alive()


def executable_row_7_contract() -> tuple[dict[str, object], dict[str, object]]:
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    authorization["config_sha256"] = hashlib.sha256(
        (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest()
    authorization["writer_sha256"] = sha256(WRITER)
    authorization["writer_contract_sha256"] = sha256(CONTRACT_PATH)
    return config, authorization


def configure_row_7_lease_boundary(monkeypatch, tmp_path: Path):
    writer = load_writer_module()
    config, authorization = executable_row_7_contract()
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    monkeypatch.setattr(writer.socket, "gethostname", lambda: config["execution"]["host"])
    monkeypatch.setattr(
        writer, "mem_available", lambda: config["execution"]["min_available_bytes"]
    )
    monkeypatch.setattr(writer, "vm_global_lamin_writer_lock_path", lambda: Path("global"))
    monkeypatch.setattr(writer, "legacy_lamin_writer_lock_paths", lambda: [Path("family")])
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )
    return writer


def test_row_7_live_ledger_is_observed_under_both_leases_before_external_calls(
    monkeypatch, tmp_path: Path
) -> None:
    writer = configure_row_7_lease_boundary(monkeypatch, tmp_path)
    calls: list[str] = []

    @contextmanager
    def lease(path, *args, **kwargs):
        calls.append(f"lease:{path}")
        yield

    def live_ledger():
        calls.append("live-ledger:4")
        return {
            "metric": "accepted_components",
            "current": 4,
            "denominator": 153,
            "source": "hermes-kanban-completed-product-deltas/v1",
        }

    def next_boundary(*args, **kwargs):
        calls.append("source-api")
        raise RuntimeError("next external boundary")

    monkeypatch.setattr(writer, "lamin_writer_lock", lease)
    monkeypatch.setattr(writer, "read_live_accepted_components_ledger", live_ledger)
    monkeypatch.setattr(writer, "source_api", next_boundary)
    monkeypatch.setattr(Path, "mkdir", lambda *args, **kwargs: calls.append("mkdir"))

    with pytest.raises(RuntimeError, match="next external boundary"):
        writer.main()

    assert calls == ["lease:global", "lease:family", "live-ledger:4", "source-api"]


def test_row_7_live_ledger_failure_releases_leases_before_all_external_boundaries(
    monkeypatch, tmp_path: Path
) -> None:
    writer = configure_row_7_lease_boundary(monkeypatch, tmp_path)
    lease_events: list[str] = []

    @contextmanager
    def lease(path, *args, **kwargs):
        lease_events.append(f"enter:{path}")
        try:
            yield
        finally:
            lease_events.append(f"exit:{path}")

    monkeypatch.setattr(writer, "lamin_writer_lock", lease)
    monkeypatch.setattr(
        writer,
        "read_live_accepted_components_ledger",
        lambda: (_ for _ in ()).throw(RuntimeError("accepted-components ledger malformed")),
    )
    external_calls = instrument_external_boundaries(monkeypatch, writer)

    with pytest.raises(RuntimeError, match="accepted-components ledger malformed"):
        writer.main()

    assert external_calls == []
    assert lease_events == [
        "enter:global",
        "enter:family",
        "exit:family",
        "exit:global",
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.__setitem__("board", "wrong"),
        lambda value: value.__setitem__("metric", "wrong"),
        lambda value: value.__setitem__("request_nonce", "b" * 64),
        lambda value: value.__setitem__("issued_at", 90.0),
        lambda value: value["chain"][2].__setitem__("before", 0),
        lambda value: value["chain"][2].__setitem__(
            "task_id", value["chain"][1]["task_id"]
        ),
        lambda value: value["latest_owner"].__setitem__("task_id", "t_rebound"),
        lambda value: value["latest_owner"].__setitem__("current", True),
        lambda value: value["latest_owner"].__setitem__("denominator", 152),
        lambda value: value["latest_owner"].__setitem__("mismatch", 1),
        lambda value: value["latest_owner"].__setitem__("live_readback", "mutable"),
        lambda value: value["latest_owner"]["manifest"].__setitem__(
            "uri", immutable_manifest("rebound")["uri"]
        ),
        lambda value: value["latest_owner"]["manifest"].__setitem__(
            "generation", ""
        ),
        lambda value: value["latest_owner"]["manifest"].__setitem__(
            "sha256", "not-a-sha"
        ),
    ],
)
def test_control_plane_response_rejects_freshness_replay_chain_and_immutability(
    mutation,
) -> None:
    writer = load_writer_module()
    response = response_fixture(writer)
    mutation(response)

    with pytest.raises(RuntimeError, match="accepted-components"):
        writer._validate_live_ledger_response(
            response,
            nonce="a" * 64,
            requested_at=99.0,
            received_at=101.0,
        )


def test_row_7_missing_tunnel_releases_leases_before_external_boundaries(
    monkeypatch, tmp_path: Path
) -> None:
    writer = configure_row_7_lease_boundary(monkeypatch, tmp_path)
    lease_events: list[str] = []

    @contextmanager
    def lease(path, *args, **kwargs):
        lease_events.append(f"enter:{path}")
        try:
            yield
        finally:
            lease_events.append(f"exit:{path}")

    monkeypatch.delenv("PERT_GYM_LEDGER_URL", raising=False)
    monkeypatch.delenv("PERT_GYM_LEDGER_BEARER_TOKEN", raising=False)
    monkeypatch.setattr(writer, "lamin_writer_lock", lease)
    external_calls = instrument_external_boundaries(monkeypatch, writer)

    with pytest.raises(RuntimeError, match="loopback tunnel or bearer"):
        writer.main()

    assert external_calls == []
    assert lease_events == [
        "enter:global",
        "enter:family",
        "exit:family",
        "exit:global",
    ]


def test_heartbeat_uses_the_single_lease_protected_live_ledger_observation(
    monkeypatch, tmp_path: Path
) -> None:
    writer = load_writer_module()
    writer.ACTIVE_CONFIG = load_json(ROW_7_CONFIG)
    writer.REVISION_PREFIX = "temporal-v4-007"
    writer.OUT = tmp_path
    monkeypatch.setattr(writer.socket, "gethostname", lambda: "pert-gym-worker-eu")
    monkeypatch.setattr(writer, "rss_bytes", lambda pid: 1)
    monkeypatch.setattr(writer, "mem_available", lambda: 2)
    payloads: list[dict[str, object]] = []

    def capture(_fs, key, payload):
        payloads.append(json.loads(payload))
        return {"key": key, "generation": "1", "size": len(payload), "sha256": "f" * 64}

    monkeypatch.setattr(writer, "exclusive_bytes", capture)
    rollback = tmp_path / "rollback.jsonl"
    rollback.write_text("")
    live_ledger = {
        "metric": "accepted_components",
        "current": 4,
        "denominator": 153,
        "source": "hermes-kanban-completed-product-deltas/v1",
    }
    heartbeats = writer.Heartbeats(
        object(), "prefix", "revision", "lease", [], rollback, live_ledger
    )

    heartbeats.emit()

    assert payloads[0]["product_execution"]["current"] == 4
    assert payloads[0]["product_execution"]["denominator"] == 153
    assert payloads[0]["accepted_components_ledger"] == live_ledger


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda c: c.__setitem__("catalogue_record", "wrong"), "catalogue_record"),
        (lambda c: c.__setitem__("logical_key", "pert-gym/logical/temporal/wrong"), "logical_key"),
        (lambda c: c["source"].__setitem__("collection_id", "00000000-0000-0000-0000-000000000000"), "collection_id"),
        (lambda c: c["source"].__setitem__("collection_version_id", "00000000-0000-0000-0000-000000000000"), "collection_version_id"),
        (lambda c: c["source"].__setitem__("dataset_id", "00000000-0000-0000-0000-000000000000"), "dataset_id"),
        (lambda c: c["source"].__setitem__("dataset_version_id", "00000000-0000-0000-0000-000000000000"), "dataset_version_id"),
        (lambda c: c["source"].__setitem__("asset_id", "00000000-0000-0000-0000-000000000000"), "asset_id"),
        (lambda c: c["source"].__setitem__("url", "https://datasets.cellxgene.cziscience.com/00000000-0000-0000-0000-000000000000.h5ad"), "URL|url"),
        (lambda c: c["source_head"].__setitem__("content_length", 1), "source_head"),
        (lambda c: c["source_head"].__setitem__("etag", "wrong"), "source_head"),
        (lambda c: c["source_head"].__setitem__("last_modified", "wrong"), "source_head"),
        (lambda c: c.__setitem__("shape", [1, 35461]), "shape"),
        (lambda c: c["api_identity"].__setitem__("organism", {"label": "Mus musculus", "ontology_term_id": "NCBITaxon:10090"}), "Homo sapiens"),
        (lambda c: c["api_identity"].__setitem__("assays", [{"label": "wrong", "ontology_term_id": "EFO:0000000"}]), "assays"),
        (lambda c: c["ordered_var"].__setitem__("identity_sha256", "f" * 64), "ordered[_-]var"),
    ],
)
def test_row_7_rebound_config_identity_mismatches_fail_closed(mutation, match: str) -> None:
    config = load_json(ROW_7_CONFIG)
    mutation(config)
    authorization = load_json(ROW_7_AUTHORIZATION)
    authorization["config_sha256"] = hashlib.sha256(
        (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest()

    with pytest.raises((RuntimeError, ValueError), match=match):
        validate(config, authorization)


def test_row_7_runtime_semantic_preflight_rejects_placeholder_development_stage() -> None:
    writer = load_writer_module()
    writer.ACTIVE_CONFIG = load_json(ROW_7_CONFIG)
    writer.N_OBS = 2
    source = pd.DataFrame(
        {
            "development_stage": ["unknown", "unknown"],
            "development_stage_ontology_term_id": ["unknown", "unknown"],
            "assay": ["10x 5' v1", "10x 5' v1"],
            "assay_ontology_term_id": ["EFO:0011025", "EFO:0011025"],
        },
        index=["cell-1", "cell-2"],
    )
    with pytest.raises(RuntimeError, match="required OBS predicate failed"):
        writer.map_obs(source, "Homo sapiens")


def test_row_7_runtime_ordered_var_identity_is_computed_before_candidate_write() -> None:
    writer = load_writer_module()
    writer.ACTIVE_CONFIG = load_json(ROW_7_CONFIG)
    writer.N_VARS = 2
    source = pd.DataFrame(
        {
            "feature_reference": ["NCBITaxon:9606", "NCBITaxon:9606"],
            "feature_name": ["GENE1", "GENE2"],
        },
        index=["ENSG1", "ENSG2"],
    )
    _var, identity = writer.map_var(source, "Homo sapiens")
    assert identity == writer.ordered_var_identity(["ENSG1", "ENSG2"])


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda c, a: a.__setitem__("config_sha256", "0" * 64), "config SHA-256"),
        (lambda c, a: a.__setitem__("writer_sha256", "0" * 64), "writer SHA-256"),
        (lambda c, a: a.__setitem__("parent_task_status", "running"), "not completed"),
        (lambda c, a: (c["authorization_binding"].__setitem__("parent_task_id", "t_rebound"), a.__setitem__("parent_task_id", "t_rebound")), "approved revision identity"),
        (lambda c, a: (c.__setitem__("dataset_config_status", "prewrite-fixture"), c["obs"]["semantic_evidence"].__setitem__("verdict", "pending-prewrite"), c["ordered_var"].__setitem__("identity_sha256", None)), "dataset_config_status"),
    ],
)
def test_row_7_stale_or_rebound_authorization_fails_closed(mutation, match: str) -> None:
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    mutation(config, authorization)
    if authorization["config_sha256"] != "0" * 64:
        authorization["config_sha256"] = hashlib.sha256(
            (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
        ).hexdigest()
    with pytest.raises((RuntimeError, ValueError), match=match):
        validate(config, authorization)


def test_row_7_execution_false_is_not_authorized() -> None:
    contract = load_contract_module()
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    authorization["execution_authorized"] = False
    validated = validate(config, authorization)
    with pytest.raises(RuntimeError, match="not execution-authorized"):
        contract.require_execution_authorized(validated)


@pytest.mark.parametrize(
    ("mutator", "rebind_config", "match"),
    [
        (lambda c, a: a.__setitem__("config_sha256", "0" * 64), False, "config SHA-256"),
        (lambda c, a: a.__setitem__("writer_sha256", "0" * 64), False, "writer SHA-256"),
        (
            lambda c, a: a.__setitem__("writer_contract_sha256", "0" * 64),
            False,
            "contract SHA-256",
        ),
        (
            lambda c, a: a.__setitem__("parquet_frame_parity_sha256", "0" * 64),
            False,
            "helper SHA-256",
        ),
        (lambda c, a: c.__setitem__("shape", [1, 2]), False, "config SHA-256"),
        (
            lambda c, a: c["source"].__setitem__("asset_id", "00000000-0000-0000-0000-000000000000"),
            True,
            "source URL",
        ),
        (
            lambda c, a: c.__setitem__("logical_key", "pert-gym/logical/temporal/wrong"),
            True,
            "logical_key conflicts with OBS dataset assignment",
        ),
        (
            lambda c, a: c["accepted_components"].__setitem__("current", 999),
            True,
            "accepted-components",
        ),
        (
            lambda c, a: c["obs"].__setitem__("mapper_version", "unknown/v1"),
            True,
            "mapper_version",
        ),
        (
            lambda c, a: c["ordered_var"].__setitem__("identity_sha256", "f" * 64),
            False,
            "config SHA-256",
        ),
        (lambda c, a: a.__setitem__("protocol", "wrong/v1"), False, "protocol"),
    ],
)
def test_every_bound_mismatch_aborts_before_any_write(mutator, rebind_config: bool, match: str) -> None:
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    mutator(config, authorization)
    if rebind_config:
        authorization = bound_authorization(config)

    with pytest.raises((RuntimeError, ValueError), match=match):
        validate(config, authorization)


@pytest.mark.parametrize(
    ("target", "value", "match"),
    [
        ("parent_task_id", "t_fake", "parent task"),
        ("approved_parent_protocol", "anything/v999", "parent protocol"),
        ("correction_task_id", "t_other", "correction task"),
    ],
)
def test_authorization_identity_is_exactly_bound_to_config(
    target: str, value: str, match: str
) -> None:
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    authorization[target] = value

    with pytest.raises(RuntimeError, match=match):
        validate(config, authorization)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda c: c["obs"]["assignments"][0].__setitem__(
                "value", "pert-gym/logical/temporal/conflict"
            ),
            "logical_key conflicts with OBS dataset assignment",
        ),
        (
            lambda c: c["source"].__setitem__(
                "dataset_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "dataset_version_id conflicts with asset_id",
        ),
        (
            lambda c: c["source"].__setitem__(
                "dataset_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source dataset_id conflicts with approved revision identity",
        ),
        (
            lambda c: c["source"].__setitem__(
                "collection_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source collection_version_id conflicts with approved revision identity",
        ),
        (
            lambda c: c["revision"].__setitem__("prefix", "temporal-v4-013"),
            "revision prefix conflicts with output/task identity",
        ),
    ],
)
def test_freshly_hash_bound_cross_field_conflicts_fail_intrinsically(
    mutation, match: str
) -> None:
    config = load_json(ROW_99_CONFIG)
    mutation(config)

    with pytest.raises(ValueError, match=match):
        validate(config, bound_authorization(config))


def instrument_external_boundaries(monkeypatch, writer) -> list[str]:
    calls: list[str] = []

    def reject(label: str):
        def boundary(*args, **kwargs):
            calls.append(label)
            raise AssertionError(f"unexpected boundary call: {label}")

        return boundary

    monkeypatch.setattr(Path, "mkdir", reject("mkdir"))
    monkeypatch.setattr(writer.urllib.request, "urlopen", reject("urlopen"))
    monkeypatch.setattr(writer, "source_api", reject("source_api"))
    monkeypatch.setattr(writer, "source_head", reject("source_head"))
    monkeypatch.setattr(writer, "hash_source", reject("source_body"))
    monkeypatch.setattr(writer, "connect_pertdata", reject("lamin"))
    monkeypatch.setattr(writer.fsspec, "filesystem", reject("filesystem"))
    monkeypatch.setattr(writer, "exclusive_bytes", reject("product_write"))
    return calls


def write_contract_files(
    tmp_path: Path, config: dict[str, object], authorization: dict[str, object]
) -> tuple[Path, Path]:
    config_path = tmp_path / "config.json"
    authorization_path = tmp_path / "authorization.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    authorization["config_sha256"] = sha256(config_path)
    authorization_path.write_text(json.dumps(authorization, indent=2, sort_keys=True) + "\n")
    return config_path, authorization_path


@pytest.mark.parametrize(
    "mutation",
    [
        lambda c: c["revision"].__setitem__("prefix", "temporal-v4-008"),
        lambda c: c["source"].__setitem__("url", "https://datasets.cellxgene.cziscience.com/00000000-0000-0000-0000-000000000000.h5ad"),
    ],
)
def test_row_7_unauthorized_source_or_revision_rejects_before_side_effects(
    monkeypatch, tmp_path: Path, mutation
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    mutation(config)
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )
    with pytest.raises((RuntimeError, ValueError)):
        writer.main()
    assert calls == []


@pytest.mark.parametrize(
    "mutation",
    [
        lambda head: head.pop("version_id"),
        lambda head: head.__setitem__("version_id", None),
        lambda head: head.__setitem__("version_id", "wrong-version"),
        lambda head: head.__setitem__("version_id", "rebound-version"),
    ],
)
def test_row_7_absent_null_wrong_or_rebound_config_version_rejects_before_leases_or_side_effects(
    monkeypatch, tmp_path: Path, mutation
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_7_CONFIG)
    authorization = load_json(ROW_7_AUTHORIZATION)
    source_head = config["source_head"]
    assert isinstance(source_head, dict)
    mutation(source_head)
    config_path, authorization_path = write_contract_files(
        tmp_path, config, authorization
    )
    lease_events: list[str] = []

    @contextmanager
    def lease(path, *args, **kwargs):
        lease_events.append(f"enter:{path}")
        try:
            yield
        finally:
            lease_events.append(f"exit:{path}")

    monkeypatch.setattr(writer, "lamin_writer_lock", lease)
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(WRITER),
            "--config",
            str(config_path),
            "--authorization",
            str(authorization_path),
        ],
    )

    with pytest.raises((RuntimeError, ValueError), match="source_head"):
        writer.main()

    assert calls == []
    assert lease_events == []


class SourceHeadResponse(io.BytesIO):
    def __init__(self, version_id: object, *, include_header: bool = True) -> None:
        super().__init__()
        self.status = 200
        self.url = "https://datasets.cellxgene.cziscience.com/89a17e6a-bbb0-4a9a-b2b4-3bafd89277a8.h5ad"
        self.headers: dict[str, object] = {
            "Content-Length": "181024331",
            "ETag": '"f5d444de645d745875bb1cee2113f20a-22"',
            "Last-Modified": "Wed, 10 Jun 2026 16:14:51 GMT",
        }
        if include_header:
            self.headers["x-amz-version-id"] = version_id


def configure_row_7_source_head_boundary(monkeypatch, tmp_path: Path):
    writer = configure_row_7_lease_boundary(monkeypatch, tmp_path)
    lease_events: list[str] = []

    @contextmanager
    def lease(path, *args, **kwargs):
        lease_events.append(f"enter:{path}")
        try:
            yield
        finally:
            lease_events.append(f"exit:{path}")

    monkeypatch.setattr(writer, "lamin_writer_lock", lease)
    monkeypatch.setattr(
        writer,
        "read_live_accepted_components_ledger",
        lambda: {
            "metric": "accepted_components",
            "current": 4,
            "denominator": 153,
            "source": "hermes-kanban-completed-product-deltas/v1",
        },
    )
    monkeypatch.setattr(writer, "source_api", lambda contract: {"identity": "exact"})
    return writer, lease_events


def test_row_7_exact_observed_source_version_reaches_local_output_only_after_head(
    monkeypatch, tmp_path: Path
) -> None:
    writer, lease_events = configure_row_7_source_head_boundary(monkeypatch, tmp_path)
    calls: list[str] = []
    monkeypatch.setattr(
        writer.urllib.request,
        "urlopen",
        lambda *args, **kwargs: SourceHeadResponse(ROW_7_SOURCE_VERSION_ID),
    )

    def next_boundary(*args, **kwargs):
        calls.append("mkdir")
        raise RuntimeError("next local-output boundary")

    monkeypatch.setattr(Path, "mkdir", next_boundary)
    monkeypatch.setattr(
        writer,
        "hash_source",
        lambda: (_ for _ in ()).throw(AssertionError("source body")),
    )

    with pytest.raises(RuntimeError, match="next local-output boundary"):
        writer.main()

    assert calls == ["mkdir"]
    assert lease_events == [
        "enter:global",
        "enter:family",
        "exit:family",
        "exit:global",
    ]


@pytest.mark.parametrize(
    ("version_id", "include_header"),
    [(None, False), (None, True), ("wrong-version", True)],
)
def test_row_7_absent_null_or_wrong_observed_source_version_releases_leases_before_side_effects(
    monkeypatch, tmp_path: Path, version_id: object, include_header: bool
) -> None:
    writer, lease_events = configure_row_7_source_head_boundary(monkeypatch, tmp_path)
    real_source_head = writer.source_head
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(writer, "source_head", real_source_head)
    monkeypatch.setattr(
        writer.urllib.request,
        "urlopen",
        lambda *args, **kwargs: SourceHeadResponse(
            version_id, include_header=include_header
        ),
    )
    monkeypatch.setattr(writer, "source_api", lambda contract: {"identity": "exact"})

    with pytest.raises(RuntimeError, match="source HEAD drift"):
        writer.main()

    assert calls == []
    assert lease_events == [
        "enter:global",
        "enter:family",
        "exit:family",
        "exit:global",
    ]


def executable_row_13_contract() -> tuple[dict[str, object], dict[str, object]]:
    config = load_json(ROW_13_CONFIG)
    config["dataset_config_status"] = "reviewed-executable"
    config["api_identity"]["assays"][1]["ontology_term_id"] = "EFO:0022601"
    config["obs"]["semantic_evidence"] = {
        "verdict": "accepted",
        "basis": "test boundary fixture",
    }
    config["ordered_var"]["identity_sha256"] = "f" * 64
    authorization = bound_authorization(config)
    authorization.update(config["authorization_binding"])
    authorization["execution_authorized"] = True
    return config, authorization


def cellxgene_collection_payload(
    config: dict[str, object], is_primary_data: object
) -> dict[str, object]:
    source = config["source"]
    return {
        "collection_id": source["collection_id"],
        "collection_version_id": source["collection_version_id"],
        "visibility": "PUBLIC",
        "datasets": [{
            "dataset_id": source["dataset_id"],
            "dataset_version_id": source["dataset_version_id"],
            "assets": [{"filetype": "H5AD", "url": source["url"]}],
            "cell_count": config["shape"][0],
            "feature_count": config["shape"][1],
            "organism": [config["api_identity"]["organism"]],
            "assay": config["api_identity"]["assays"],
            "tombstone": False,
            "is_primary_data": is_primary_data,
        }],
    }


def configure_row_13_main(monkeypatch, tmp_path: Path, live_value: object):
    writer = load_writer_module()
    config, authorization = executable_row_13_contract()
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    payload = cellxgene_collection_payload(config, live_value)
    monkeypatch.setattr(
        writer.urllib.request,
        "urlopen",
        lambda *args, **kwargs: io.BytesIO(json.dumps(payload).encode()),
    )
    monkeypatch.setattr(writer.socket, "gethostname", lambda: config["execution"]["host"])
    monkeypatch.setattr(
        writer,
        "mem_available",
        lambda: config["execution"]["min_available_bytes"],
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )
    return writer


@pytest.mark.parametrize("live_value", [True, [True]])
def test_row_13_exact_primary_data_representations_reach_next_preflight_boundary(
    monkeypatch, tmp_path: Path, live_value: object
) -> None:
    writer = configure_row_13_main(monkeypatch, tmp_path, live_value)
    calls: list[str] = []

    class NextPreflightBoundary(RuntimeError):
        pass

    monkeypatch.setattr(Path, "mkdir", lambda *args, **kwargs: calls.append("mkdir"))

    def next_boundary(*args, **kwargs):
        calls.append("source_head")
        raise NextPreflightBoundary("next generic preflight boundary")

    monkeypatch.setattr(writer, "source_head", next_boundary)
    with pytest.raises(NextPreflightBoundary, match="next generic preflight boundary"):
        writer.main()
    assert calls == ["mkdir", "source_head"]


@pytest.mark.parametrize(
    "live_value",
    [False, [False], [], [True, True], None, "true", 1, 0, {}, [[True]], [True, False]],
)
def test_row_13_malformed_or_non_primary_live_values_reject_before_side_effects(
    monkeypatch, tmp_path: Path, live_value: object
) -> None:
    writer = configure_row_13_main(monkeypatch, tmp_path, live_value)
    calls: list[str] = []

    def reject(label: str):
        def boundary(*args, **kwargs):
            calls.append(label)
            raise AssertionError(f"unexpected boundary call: {label}")
        return boundary

    monkeypatch.setattr(Path, "mkdir", reject("mkdir"))
    monkeypatch.setattr(writer, "source_head", reject("source_head"))
    monkeypatch.setattr(writer, "hash_source", reject("source_body"))
    monkeypatch.setattr(writer, "connect_pertdata", reject("lamin"))
    monkeypatch.setattr(writer.fsspec, "filesystem", reject("filesystem"))
    monkeypatch.setattr(writer, "exclusive_bytes", reject("product_write"))
    with pytest.raises(RuntimeError, match="primary-data"):
        writer.main()
    assert calls == []


@pytest.mark.parametrize(
    "contract_value",
    [False, [True], [False], [], [True, True], None, "true", 1, 0, {}, [[True]]],
)
def test_non_scalar_or_non_boolean_contract_primary_data_rejects_before_boundaries(
    monkeypatch, tmp_path: Path, contract_value: object
) -> None:
    writer = load_writer_module()
    config, authorization = executable_row_13_contract()
    config["api_identity"]["is_primary_data"] = contract_value
    authorization = bound_authorization(config)
    authorization.update(config["authorization_binding"])
    authorization["execution_authorized"] = True
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )
    with pytest.raises(ValueError, match="primary-data identity"):
        writer.main()
    assert calls == []


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda c, a: c["obs"]["assignments"][0].__setitem__(
                "value", "pert-gym/logical/temporal/conflict"
            ),
            "logical_key conflicts with OBS dataset assignment",
        ),
        (
            lambda c, a: c["source"].__setitem__(
                "dataset_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "dataset_version_id conflicts with asset_id",
        ),
        (
            lambda c, a: c["source"].__setitem__(
                "dataset_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source dataset_id conflicts with approved revision identity",
        ),
        (
            lambda c, a: c["source"].__setitem__(
                "collection_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source collection_version_id conflicts with approved revision identity",
        ),
        (
            lambda c, a: c["revision"].__setitem__("prefix", "temporal-v4-013"),
            "revision prefix conflicts with output/task identity",
        ),
        (
            lambda c, a: (
                c["authorization_binding"].__setitem__("parent_task_id", "t_fake"),
                a.__setitem__("parent_task_id", "t_fake"),
            ),
            "authorization parent task conflicts with approved revision identity",
        ),
        (lambda c, a: a.__setitem__("parent_task_id", "t_fake"), "parent task"),
        (
            lambda c, a: a.__setitem__("approved_parent_protocol", "anything/v999"),
            "parent protocol",
        ),
        (lambda c, a: a.__setitem__("correction_task_id", "t_other"), "correction task"),
    ],
)
def test_contract_conflicts_reject_before_instrumented_boundaries(
    monkeypatch, tmp_path: Path, mutation, match: str
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    authorization["execution_authorized"] = True
    mutation(config, authorization)
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    execution = config["execution"]
    assert isinstance(execution, dict)
    monkeypatch.setattr(
        writer.socket,
        "gethostname",
        lambda: str(execution["host"]),
    )
    monkeypatch.setattr(
        writer,
        "mem_available",
        lambda: int(execution["min_available_bytes"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )

    with pytest.raises((RuntimeError, ValueError), match=match):
        writer.main()

    assert calls == []


@pytest.mark.parametrize(
    ("parent_task_id", "parent_task_status", "match"),
    [
        ("t_0c090cdd", "running", "parent task is not completed"),
        ("t_0c090cdd", "reviewed", "parent task is not completed"),
        (
            "t_b8dacd9d",
            "completed",
            "authorization parent task conflicts with approved revision identity",
        ),
        (
            "t_arbitrary",
            "completed",
            "authorization parent task conflicts with approved revision identity",
        ),
    ],
)
def test_row_13_invalid_parent_authorization_rejects_before_instrumented_boundaries(
    monkeypatch,
    tmp_path: Path,
    parent_task_id: str,
    parent_task_status: str,
    match: str,
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_13_CONFIG)
    binding = config["authorization_binding"]
    assert isinstance(binding, dict)
    binding["parent_task_id"] = parent_task_id
    authorization = bound_authorization(config)
    authorization.update(binding)
    authorization["parent_task_status"] = parent_task_status
    authorization["execution_authorized"] = True
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    execution = config["execution"]
    assert isinstance(execution, dict)
    monkeypatch.setattr(writer.socket, "gethostname", lambda: str(execution["host"]))
    monkeypatch.setattr(
        writer,
        "mem_available",
        lambda: int(execution["min_available_bytes"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )

    with pytest.raises((RuntimeError, ValueError), match=match):
        writer.main()

    assert calls == []


def test_runtime_host_rejects_before_instrumented_boundaries(
    monkeypatch, tmp_path: Path
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    authorization["execution_authorized"] = True
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(writer.socket, "gethostname", lambda: "other-host")
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )

    with pytest.raises(RuntimeError, match="exact authorized host"):
        writer.main()

    assert calls == []


def test_heartbeat_progress_is_derived_from_configured_n_obs() -> None:
    writer = load_writer_module()

    assert list(writer.heartbeat_progress_rows(10224))[-1] == 9216
    assert list(writer.heartbeat_progress_rows(14424))[-1] == 14336
    assert "10241" not in WRITER.read_text()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda c, a: c.__setitem__("unknown", True),
        lambda c, a: a.__setitem__("unknown", True),
        lambda c, a: c.pop("shape"),
        lambda c, a: a.pop("parent_task_id"),
        lambda c, a: c["execution"].__setitem__("host", "other-host"),
        lambda c, a: c["forbidden_actions"].remove("deletion"),
    ],
)
def test_unknown_omitted_malformed_or_conflicting_values_fail_closed(mutation) -> None:
    config = copy.deepcopy(load_json(ROW_99_CONFIG))
    authorization = copy.deepcopy(load_json(ROW_99_AUTHORIZATION))
    mutation(config, authorization)

    with pytest.raises((RuntimeError, ValueError)):
        validate(config, authorization)


def test_runner_has_no_hidden_row_99_dataset_identity_and_validates_before_io() -> None:
    writer = WRITER.read_text()
    main = writer.split("def main() -> int:", maxsplit=1)[1]
    prefix = main.split("source_api(contract)", maxsplit=1)[0]

    for hidden in (
        "081fef14-662c-430d-888f-b87a701d86b3",
        "73cf6939-3caa-4105-bc57-e073ee885a28",
        "878f431e4a709fb43d0ededbcc35511b16048e369e58984bad77fcf16600db4b",
        "temporal-v4-099-20260715T135852Z-d36b0c6d",
    ):
        assert hidden not in writer
    assert "load_bound_contract(" in prefix
    assert "require_execution_authorized(contract)" in prefix
    assert "OUT.mkdir" in main
    assert main.index("gethostname") < main.index("source_api(contract)")
    assert main.index("mem_available()") < main.index("source_api(contract)")
    assert main.index("source_api(contract)") < main.index("OUT.mkdir")
    assert main.index("OUT.mkdir") < main.index("source_head(contract)")
    assert "time.monotonic() - started > EXECUTION_TIMEOUT_SECONDS" in writer
