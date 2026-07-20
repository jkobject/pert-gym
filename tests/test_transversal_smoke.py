from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "tools/run_transversal_smoke.py"


def _runner_module():
    spec = importlib.util.spec_from_file_location("transversal_smoke", RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _batch(
    runner,
    *,
    rows: int,
    model_ids: tuple[str, ...],
    targets: tuple[float, ...] | None,
    features: tuple[tuple[float, ...], ...] | None = None,
    start: int = 0,
):
    if features is None:
        features = tuple((1.0, 2.0) for _ in range(rows))
    return SimpleNamespace(
        row_ids=tuple(f"row-{start + index}" for index in range(rows)),
        features=features,
        feature_names=("feature-a", "feature-b"),
        numeric_targets=targets,
        categorical_targets=None
        if targets is not None
        else tuple(("label",) for _ in range(rows)),
        dataset_tag=runner.PRISM_DATASET_TAG
        if targets is not None
        else runner.STRAND_DATASET_TAG,
        task_tag="direct_lfc"
        if targets is not None
        else "guide_to_source_native_label_set",
        metadata={
            "model_ids": model_ids,
            "compound_ids": tuple(f"compound-{start + index}" for index in range(rows)),
        },
    )


def _runner_inputs(tmp_path: Path, runner):
    subset = tmp_path / "subset.tsv"
    subset.write_text("depmap_id\nACH-000001\n")
    baselines = tmp_path / "baselines.json"
    baselines.write_text(
        json.dumps(
            {
                "provenance": {
                    "source": {
                        "uri": "depmap-uri",
                        "generation": "depmap-gen",
                        "sha256": "depmap-source-sha",
                    }
                },
                "rows": [],
                "feature_names": [],
            }
        )
    )
    join = tmp_path / "strand.tsv"
    join.write_text("join_row_id\nstrand-1\n")
    metadata = tmp_path / "strand-metadata.json"
    manifest = tmp_path / "manifest.json"
    provenance = {
        "prism_subset": {
            "uri": "prism-uri",
            "generation": "prism-gen",
            "sha256": _sha256(subset),
        },
        "depmap_fixture": {
            "uri": "fixture-uri",
            "generation": "fixture-gen",
            "sha256": _sha256(baselines),
        },
        "depmap_source": {
            "uri": "depmap-uri",
            "generation": "depmap-gen",
            "sha256": "depmap-source-sha",
        },
        "strand_join": {
            "uri": "join-uri",
            "generation": "join-gen",
            "sha256": _sha256(join),
        },
        "strand_metadata": {"uri": "metadata-uri", "generation": "metadata-gen"},
    }
    strand_metadata = {
        "counts": {
            "unmatched_unique_perturbation_rows_before_resolution": 403,
            "resolved_unique_perturbation_rows": 401,
            "unresolved_unique_perturbation_rows_after_resolution": 2,
            "reviewed_alias_policy_keys": 157,
            "reviewed_alias_policy_per_dataset": {
                "hepg2": {"accepted_alias_rows": 53, "excluded_rows": 0},
                "jurkat": {"accepted_alias_rows": 66, "excluded_rows": 0},
                "k562": {"accepted_alias_rows": 216, "excluded_rows": 2},
                "rpe1": {"accepted_alias_rows": 66, "excluded_rows": 0},
            },
            "table_rows": 1,
        },
        "loader_exclusions": {
            "unresolved_by_file": {
                "k562-de.csv": [
                    {"perturbation": "ELOB", "classification": "ambiguous_multi_target"}
                ],
                "k562-dir.csv": [
                    {"perturbation": "ELOB", "classification": "ambiguous_multi_target"}
                ],
            }
        },
        "outputs": {"table_tsv_sha256": _sha256(join)},
    }
    metadata.write_text(json.dumps(strand_metadata))
    provenance["strand_metadata"]["sha256"] = _sha256(metadata)
    manifest.write_text(
        json.dumps(
            {
                "source": provenance["prism_subset"],
                "baseline": provenance["depmap_source"],
                "transversal_smoke_provenance": provenance,
            }
        )
    )
    out = tmp_path / "report.json"
    argv = [
        "--prism-subset",
        str(subset),
        "--prism-manifest",
        str(manifest),
        "--prism-baselines",
        str(baselines),
        "--strand-join",
        str(join),
        "--strand-metadata",
        str(metadata),
        "--prism-subset-uri",
        "prism-uri",
        "--prism-subset-generation",
        "prism-gen",
        "--depmap-fixture-uri",
        "fixture-uri",
        "--depmap-fixture-generation",
        "fixture-gen",
        "--strand-join-uri",
        "join-uri",
        "--strand-join-generation",
        "join-gen",
        "--strand-metadata-uri",
        "metadata-uri",
        "--strand-metadata-generation",
        "metadata-gen",
        "--out",
        str(out),
    ]
    return SimpleNamespace(
        subset=subset,
        baselines=baselines,
        join=join,
        metadata=metadata,
        manifest=manifest,
        out=out,
        argv=argv,
        metadata_payload=strand_metadata,
    )


def _install_passing_runner(
    monkeypatch: pytest.MonkeyPatch,
    runner,
    inputs,
    *,
    rows=126,
    model_ids=118,
    targets=None,
    features=None,
) -> None:
    manifest = json.loads(inputs.manifest.read_text())
    provenance = manifest["transversal_smoke_provenance"]
    monkeypatch.setattr(
        runner,
        "REVIEWED_INPUT_PROVENANCE",
        {
            **provenance,
            "prism_subset": {
                **provenance["prism_subset"],
                "size_bytes": inputs.subset.stat().st_size,
            },
            "depmap_fixture": {
                **provenance["depmap_fixture"],
                "size_bytes": inputs.baselines.stat().st_size,
            },
            "strand_join": {
                **provenance["strand_join"],
                "size_bytes": inputs.join.stat().st_size,
            },
            "strand_metadata": {
                **provenance["strand_metadata"],
                "size_bytes": inputs.metadata.stat().st_size,
            },
            "prism_manifest": {
                "uri": "manifest-uri",
                "generation": "manifest-gen",
                "sha256": _sha256(inputs.manifest),
                "size_bytes": inputs.manifest.stat().st_size,
            },
        },
    )
    monkeypatch.setattr(runner, "validate_fixture_for_manifest", lambda *args: None)
    monkeypatch.setattr(runner, "_commit", lambda: "test-commit")
    targets = (
        targets
        if targets is not None
        else tuple(float(index + 1) for index in range(rows))
    )
    split_rows = (rows - 44, 19, 25)
    all_targets = (
        targets
        if targets is not None
        else tuple(float(index + 1) for index in range(rows))
    )
    all_features = (
        features if features is not None else tuple((1.0, 2.0) for _ in range(rows))
    )
    prism_batches = []
    offset = 0
    for split_row_count in split_rows:
        prism_batches.append(
            _batch(
                runner,
                rows=split_row_count,
                model_ids=tuple(
                    f"model-{index % model_ids}"
                    for index in range(offset, offset + split_row_count)
                ),
                targets=all_targets[offset : offset + split_row_count],
                features=all_features[offset : offset + split_row_count],
                start=offset,
            )
        )
        offset += split_row_count
    strand = _batch(runner, rows=1, model_ids=(), targets=None)
    monkeypatch.setattr(
        runner,
        "load_transversal_batches",
        lambda **kwargs: SimpleNamespace(
            by_split={
                "train": (prism_batches[0], strand),
                "val": (prism_batches[1],),
                "test": (prism_batches[2],),
            },
            metadata={"contract": "test"},
        ),
    )


def _rejected_report(inputs) -> dict[str, object]:
    assert inputs.out.exists()
    assert inputs.out.with_suffix(".json.sha256").exists()
    report = json.loads(inputs.out.read_text())
    assert report["status"] == "rejected"
    return report


@pytest.mark.parametrize(
    ("argument", "forged"),
    [
        ("--prism-subset-uri", "forged-uri"),
        ("--prism-subset-generation", "forged-generation"),
    ],
)
def test_runner_rejects_forged_uri_or_generation_and_records_bound_provenance(
    tmp_path, monkeypatch: pytest.MonkeyPatch, argument: str, forged: str
) -> None:
    runner = _runner_module()
    inputs = _runner_inputs(tmp_path, runner)
    _install_passing_runner(monkeypatch, runner, inputs)

    with pytest.raises(ValueError, match="prism_subset provenance"):
        runner.main([*inputs.argv, argument, forged])

    report = _rejected_report(inputs)
    assert (
        report["inputs"]["prism_subset"][argument.removeprefix("--prism-subset-")]
        == forged
    )


def test_runner_records_all_bound_provenance_in_a_passed_report(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner_module()
    inputs = _runner_inputs(tmp_path, runner)
    _install_passing_runner(monkeypatch, runner, inputs)

    runner.main(inputs.argv)

    report = json.loads(inputs.out.read_text())
    assert report["status"] == "passed"
    assert set(report["inputs"]) == {
        "prism_subset",
        "prism_manifest",
        "prism_baseline_fixture",
        "depmap_source",
        "strand_join",
        "strand_metadata",
    }
    assert report["selection"]["prism_rows_selected"] == 126
    assert report["selection"]["prism_unique_model_ids"] == 118


def test_reviewed_contract_uses_the_accepted_path_b_join_not_historical_evidence() -> (
    None
):
    runner = _runner_module()

    assert runner.REVIEWED_INPUT_PROVENANCE["strand_join"]["sha256"] == (
        "df012a59fe2469e996660dfa19fd5acabc0b5e4579b7f9d717b347654a651823"
    )
    assert (
        "65f6f72de46bc5a81e31f47bc80d5a27a04206eca0e57a239cc1c9df134d8983"
        not in str(runner.REVIEWED_INPUT_PROVENANCE)
    )


def test_reviewed_contract_matches_independent_accepted_attestation() -> None:
    runner = _runner_module()

    # Independently stated from the accepted generation-pinned attestation at
    # provenance-attestation.json#1783849398326128. Do not derive this expected
    # contract from runner constants: exact durable object identity is the gate.
    accepted = {
        "prism_subset": {
            "uri": "gs://scperturb/pert-gym/staging/executions/t_fef266a1/20260712T094155Z-provenance-attestation-v1/prism_126row_subset.tsv",
            "generation": "1783849334578024",
            "sha256": "b4f9abda6162d3e8a13149f384417a26a7589acd840a32bb47abfc9deeba51a1",
            "size_bytes": 105280,
        },
        "prism_manifest": {
            "uri": "gs://scperturb/pert-gym/staging/executions/t_fef266a1/20260712T094155Z-provenance-attestation-v1/prism_126row_manifest.json",
            "generation": "1783849336667872",
            "sha256": "a7fa4054f2f6785a1f9e0abe18e05f2b534eb65b34ffb903b92543107deefa2c",
            "size_bytes": 1637,
        },
        "depmap_fixture": {
            "uri": "gs://scperturb/pert-gym/staging/executions/t_75fcfe65/20260711T041136Z-8d20d03/depmap_26q1_default_entry_baseline.json",
            "generation": "1783743276398375",
            "sha256": "2d055813bcd4e00ae7aecd86c00a772e2342a036181cf6107488444e7790d3bf",
            "size_bytes": 41343318,
        },
        "depmap_source": {
            "uri": "gs://scperturb/pert-gym/staging/data/main/depmap_ccle/OmicsExpressionTPMLogp1HumanProteinCodingGenes.csv",
            "generation": "1781122665212682",
            "sha256": "0377be80c525fde98cbd2c6e8b06bdf2a4014a9683eb70182c1f8649d711021a",
            "size_bytes": 305007605,
        },
        "strand_join": {
            "uri": "gs://scperturb/pert-gym/staging/executions/t_fef266a1/20260712T094155Z-provenance-attestation-v1/strand_path_b_join.tsv",
            "generation": "1783849338790865",
            "sha256": "df012a59fe2469e996660dfa19fd5acabc0b5e4579b7f9d717b347654a651823",
            "size_bytes": 14209636,
        },
        "strand_metadata": {
            "uri": "gs://scperturb/pert-gym/staging/executions/t_fef266a1/20260712T094155Z-provenance-attestation-v1/strand_metadata.json",
            "generation": "1783849340784769",
            "sha256": "75d34a1a0cee72e64a0811f2dd202e1ac6fb8733890cf8e9296c51e48cdcfb1f",
            "size_bytes": 9731,
        },
    }

    assert runner.REVIEWED_INPUT_PROVENANCE == accepted


def test_reviewed_contract_accepts_depmap_source_provenance_emitted_by_fixture() -> (
    None
):
    runner = _runner_module()
    reviewed = runner.REVIEWED_INPUT_PROVENANCE
    provenance_fields = ("uri", "generation", "sha256")
    manifest_provenance = {
        name: {key: record[key] for key in provenance_fields}
        for name, record in reviewed.items()
        if name != "prism_manifest"
    }
    inputs = {
        "prism_subset": dict(reviewed["prism_subset"]),
        "prism_manifest": dict(reviewed["prism_manifest"]),
        "prism_baseline_fixture": dict(reviewed["depmap_fixture"]),
        # build_fixture() records only this accepted source identity because the
        # source CSV is attested, not a local runner input whose size is observed.
        "depmap_source": manifest_provenance["depmap_source"],
        "strand_join": dict(reviewed["strand_join"]),
        "strand_metadata": dict(reviewed["strand_metadata"]),
    }
    prism_manifest = {
        "source": manifest_provenance["prism_subset"],
        "baseline": manifest_provenance["depmap_source"],
        "transversal_smoke_provenance": manifest_provenance,
    }
    strand_metadata = {
        "counts": {
            **runner._REVIEWED_STRAND_COUNTS,
            "table_rows": 16_446,
        },
        "loader_exclusions": {
            "unresolved_by_file": {
                mapping_file: [
                    {"perturbation": perturbation, "classification": classification}
                ]
                for mapping_file, perturbation, classification in runner._REVIEWED_ELOB_EXCLUSIONS
            }
        },
        "outputs": {"table_tsv_sha256": reviewed["strand_join"]["sha256"]},
    }

    counts = runner._validate_reviewed_inputs(
        inputs=inputs,
        prism_manifest=prism_manifest,
        strand_metadata=strand_metadata,
    )

    assert counts["unmatched_unique_perturbation_rows_before_resolution"] == 403


@pytest.mark.parametrize(
    "record",
    ("depmap_fixture", "depmap_source", "strand_join", "strand_metadata"),
)
def test_runner_rejects_matching_manifest_provenance_substitution(
    tmp_path, monkeypatch: pytest.MonkeyPatch, record: str
) -> None:
    runner = _runner_module()
    inputs = _runner_inputs(tmp_path, runner)
    _install_passing_runner(monkeypatch, runner, inputs)
    manifest = json.loads(inputs.manifest.read_text())
    provenance = manifest["transversal_smoke_provenance"]

    if record == "depmap_fixture":
        baseline_payload = json.loads(inputs.baselines.read_text())
        baseline_payload["forged"] = True
        inputs.baselines.write_text(json.dumps(baseline_payload))
        inputs.argv[inputs.argv.index("--depmap-fixture-uri") + 1] = (
            "forged-fixture-uri"
        )
        inputs.argv[inputs.argv.index("--depmap-fixture-generation") + 1] = (
            "forged-fixture-generation"
        )
        provenance[record].update(
            uri="forged-fixture-uri",
            generation="forged-fixture-generation",
            sha256=_sha256(inputs.baselines),
        )
    elif record == "depmap_source":
        baseline_payload = json.loads(inputs.baselines.read_text())
        baseline_payload["provenance"]["source"] = {
            "uri": "forged-source-uri",
            "generation": "forged-source-generation",
            "sha256": "f" * 64,
        }
        inputs.baselines.write_text(json.dumps(baseline_payload))
        provenance["depmap_fixture"]["sha256"] = _sha256(inputs.baselines)
        provenance[record] = baseline_payload["provenance"]["source"]
        manifest["baseline"] = provenance[record]
    elif record == "strand_join":
        inputs.join.write_text("join_row_id\nforged-strand\n")
        inputs.argv[inputs.argv.index("--strand-join-uri") + 1] = "forged-join-uri"
        inputs.argv[inputs.argv.index("--strand-join-generation") + 1] = (
            "forged-join-generation"
        )
        provenance[record].update(
            uri="forged-join-uri",
            generation="forged-join-generation",
            sha256=_sha256(inputs.join),
        )
        inputs.metadata_payload["outputs"]["table_tsv_sha256"] = _sha256(inputs.join)
        inputs.metadata.write_text(json.dumps(inputs.metadata_payload))
        provenance["strand_metadata"]["sha256"] = _sha256(inputs.metadata)
    else:
        inputs.argv[inputs.argv.index("--strand-metadata-uri") + 1] = (
            "forged-metadata-uri"
        )
        inputs.argv[inputs.argv.index("--strand-metadata-generation") + 1] = (
            "forged-metadata-generation"
        )
        inputs.metadata_payload["forged"] = True
        inputs.metadata.write_text(json.dumps(inputs.metadata_payload))
        provenance[record].update(
            uri="forged-metadata-uri",
            generation="forged-metadata-generation",
            sha256=_sha256(inputs.metadata),
        )

    inputs.manifest.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="reviewed contract"):
        runner.main(inputs.argv)

    _rejected_report(inputs)


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["counts"].update(
                {"resolved_unique_perturbation_rows": 400}
            ),
            "403/401/2",
        ),
        (
            lambda payload: payload["loader_exclusions"]["unresolved_by_file"][
                "k562-de.csv"
            ].append(
                {"perturbation": "ELOB", "classification": "ambiguous_multi_target"}
            ),
            "exactly two",
        ),
        (
            lambda payload: payload["loader_exclusions"]["unresolved_by_file"][
                "k562-de.csv"
            ].__setitem__(0, {"perturbation": "ELOB", "classification": "wrong"}),
            "exactly two",
        ),
        (
            lambda payload: payload["outputs"].update({"table_tsv_sha256": "0" * 64}),
            "joined input",
        ),
    ],
)
def test_runner_rejects_strand_policy_and_join_hash_drift(
    tmp_path, monkeypatch: pytest.MonkeyPatch, mutate, match: str
) -> None:
    runner = _runner_module()
    inputs = _runner_inputs(tmp_path, runner)
    mutate(inputs.metadata_payload)
    inputs.metadata.write_text(json.dumps(inputs.metadata_payload))
    manifest = json.loads(inputs.manifest.read_text())
    manifest["transversal_smoke_provenance"]["strand_metadata"]["sha256"] = _sha256(
        inputs.metadata
    )
    inputs.manifest.write_text(json.dumps(manifest))
    _install_passing_runner(monkeypatch, runner, inputs)

    with pytest.raises(ValueError, match=match):
        runner.main(inputs.argv)

    _rejected_report(inputs)


@pytest.mark.parametrize(
    ("rows", "model_ids", "targets", "features", "match"),
    [
        (125, 118, None, None, "126 selected PRISM rows"),
        (126, 117, None, None, "118 unique ModelIDs"),
        (126, 118, tuple(0.0 for _ in range(126)), None, "nonzero update"),
        (126, 118, None, tuple((float("nan"), 2.0) for _ in range(126)), "finite"),
    ],
)
def test_runner_rejects_bad_selection_or_optimizer_witnesses(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    rows,
    model_ids,
    targets,
    features,
    match: str,
) -> None:
    runner = _runner_module()
    inputs = _runner_inputs(tmp_path, runner)
    _install_passing_runner(
        monkeypatch,
        runner,
        inputs,
        rows=rows,
        model_ids=model_ids,
        targets=targets,
        features=features,
    )

    with pytest.raises(ValueError, match=match):
        runner.main(inputs.argv)


def test_runner_fails_closed_for_missing_input(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner_module()
    inputs = _runner_inputs(tmp_path, runner)
    _install_passing_runner(monkeypatch, runner, inputs)
    inputs.subset.unlink()

    with pytest.raises(FileNotFoundError):
        runner.main(inputs.argv)


def test_sgd_step_has_finite_losses_and_nonzero_parameter_witness() -> None:
    runner = _runner_module()

    result = runner._mse_step(((1.0, 2.0), (3.0, 4.0)), (1.0, -1.0))

    assert result["updated"] is True
    assert result["loss_before"] >= 0.0
    assert result["loss_after"] >= 0.0
    assert result["parameter_delta_l2"] > 0.0


def test_immutable_report_write_has_readback_sha_and_refuses_overwrite(
    tmp_path,
) -> None:
    runner = _runner_module()
    out = tmp_path / "report.json"
    report = {"schema_version": "test.v1", "status": "passed"}

    sidecar = runner._write_immutable_json_report(out, report)

    expected_sha = hashlib.sha256(out.read_bytes()).hexdigest()
    assert sidecar == out.with_suffix(".json.sha256")
    assert sidecar.read_text() == f"{expected_sha}  {out.name}\n"
    assert json.loads(out.read_text()) == report
    with pytest.raises(FileExistsError, match="overwrite"):
        runner._write_immutable_json_report(out, report)
