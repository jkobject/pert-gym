# STT0000071 frozen logical component — producer handoff

## Candidate

- Logical key: `pert-gym/logical/stt0000071`
- Dataset/component: `STT0000071` / zebrafish heart regeneration
- Catalogue row: `150`
- Candidate: `gs://scperturb/pert-gym/staging/pert-gym/logical/stt0000071/revisions/stt0000071-20260716T164254Z-374d55c2`
- Completion marker: `gs://scperturb/pert-gym/staging/pert-gym/logical/stt0000071/revisions/stt0000071-20260716T164254Z-374d55c2/manifest.json`
- Manifest generation: `1784221380010648`
- Manifest SHA-256: `0054f000aace5c259c9cf4e47f67dd3efbeff0634909f5210c3f4888c5dc4f67`
- Writer SHA-256: `750f6237345d08fa06244e70662ad63f629d47129efedc5f7c69f6fd57269e0b`
- Execution host/branch: `pert-gym-worker-eu`, `laminlabs/pertdata` branch `jkobject`

## Frozen coverage and dimensions

- 46 sections, 9 samples, 9 timepoints
- 260,139 observations
- 29,747 shared source gene symbols
- 132,879,334 non-zero entries
- 525,246,132 total counts
- 138 generation-pinned non-TIFF source identities bound: 46 cell-bin GEM, 46 cell metadata, 46 raw unbinned GEM provenance objects
- 46 TIFF images explicitly excluded from this expression component
- 95 immutable payload objects plus the manifest-last completion marker (96 live objects total)
- Canonical payloads: 46 `obs.parquet`, 46 `X.zarr.zip`, one `shared-var.parquet`, source inventory, and explicit metadata/quality missingness packet

The source has two real metadata schemas. The converter preserves and identifies both: 24 Seurat spatial sections and 22 reconstructed-3D sections. Dataset age and sex are source-not-reported; quality is limited to source `n_counts`/`n_genes` because no reviewed low-quality threshold is available. Ensembl IDs are explicitly absent because the source axis is gene-symbol based.

## Safety and accounting

- Live Lamin duplicate query before mutation: 0 candidates for logical-key prefix, record description, and component description.
- Global plus all legacy-exclusive writer leases were held for the bounded writer.
- Product payloads were written on the EU worker only; no Mac-local bulk reads.
- No Lamin `main`, promotion, deletion, cleanup, or collection mutation.
- Accepted-components control plane was `4/153` before and after the producer run; producer self-credit is `0`.
- Proposed delta only if independently accepted: `4 -> 5 / 153`.
- Two failed predecessor revisions are embedded in the final manifest as immutable, no-manifest, no-credit, no-resume audit surfaces. The first contains only one shared-var object; the SSH-interrupted second contains zero GCS objects.
- Terminal state: detached writer exit `0`, no writer processes, no tmux session, and a successful non-blocking global-plus-legacy lock probe.

## Verification

Producer-side readback deleted the local section payloads, re-downloaded every generation-pinned output, verified all 95 payload SHA-256/size identities, reconstructed all 46 sparse matrices, and reported exact aggregate parity with mismatch `0`.

A separate zero-write process then:

- enumerated the exact live 96-object candidate surface;
- checked every generation and size against the manifest;
- generation-pinned and SHA-verified all 95 payloads;
- reconstructed and checked all 46 matrices and observation frames;
- validated the 29,747-row shared var and 138-object source inventory;
- proved the manifest timestamp (`2026-07-16T17:03:00.018000+00:00`) is later than the latest payload (`2026-07-16T17:00:14.170000+00:00`);
- returned `PASS`, mismatch `0`.

Commands and observed results:

- `uv run --extra dev pytest tests/test_build_stt0000071_component.py -q` — `4 passed`
- `uv run --extra dev ruff check tools/build_stt0000071_component.py tests/test_build_stt0000071_component.py` — PASS
- `make test` — `486 passed, 2 skipped`
- `uv run python independent_readback.py ...` on `pert-gym-worker-eu` — PASS, 46/46 sections, 95/95 payloads, 96/96 live objects, mismatch 0
- terminal lock probe — `PASS global+legacy locks free`

## Review evidence

- `manifest-readback.json` — exact generation-pinned completion marker bytes
- `build-result-detached.json` — compact writer result
- `independent-readback.json` — separate zero-write terminal validation
- `independent_readback.py` — validator used for that readback
- `accepted-ledger-sanitized.json` — token-free pre/post live ledger evidence
- `one-section-dry-run.json` — reconstructed-3D schema dry-run
- `one-section-dry-run-early-schema.json` — Seurat spatial schema dry-run
- `product-execution.json` — terminal product heartbeat

## Residual risks / required gate

- This is a candidate only. It has no producer acceptance credit and must not be promoted or counted until independent review/testing accepts the exact manifest generation and hash above.
- Ensembl IDs, donor resolution, age, sex, some 3D coordinates, mitochondrial/ribosomal QC, and an accepted low-quality threshold are not source-supported; these are explicit missingness, not inferred values.
- The 46 TIFFs remain source-side and intentionally outside this expression component.
