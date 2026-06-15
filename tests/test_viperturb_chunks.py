from tools.ingest_viperturb_chunks import build_chunk_plan


def test_build_chunk_plan_splits_cells_and_names_chunks():
    plan = build_chunk_plan(n_obs=12, chunk_size=5, dataset_name="genome_wide_filtered")

    assert [chunk["chunk_id"] for chunk in plan] == [
        "chunk_0000",
        "chunk_0001",
        "chunk_0002",
    ]
    assert [(chunk["start"], chunk["end"]) for chunk in plan] == [
        (0, 5),
        (5, 10),
        (10, 12),
    ]
    assert [chunk["prefix"] for chunk in plan] == [
        "viperturb/genome_wide_filtered/chunk_0000",
        "viperturb/genome_wide_filtered/chunk_0001",
        "viperturb/genome_wide_filtered/chunk_0002",
    ]


def test_build_chunk_plan_rejects_invalid_chunk_size():
    try:
        build_chunk_plan(n_obs=12, chunk_size=0, dataset_name="x")
    except ValueError as error:
        assert "chunk_size" in str(error)
    else:
        raise AssertionError("expected ValueError")
