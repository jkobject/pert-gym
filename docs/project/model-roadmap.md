# Model and benchmark roadmap

_Last updated: 2026-06-22 17:14 CEST_

This page summarizes the model path without bloating README/CLAUDE. It is a
roadmap, not a claim that the current canonical collection is biologically
model-ready.

## Current modeling truth

- The canonical query surface is `pert-gym/canonical/20260621` with 1056
  triplet-integrity/query-surface members.
- The reviewed model-ready collection is separate: `pert-gym/model-ready/20260621`
  with 1 tiny loader-smoked member.
- The current model-ready v0 command can exercise loader/benchmark wiring, but
  some benchmark paths still use metadata-only synthetic fallback until bounded
  materialized matrix adapters exist.
- Do not present synthetic-fallback benchmark scores as biological model
  performance.

## Model-ready v0 criteria

A member can enter model-ready only after explicit review. Current v0 criteria:

- required obs fields: `perturbation`, `perturbation_type`, `organism`,
  `modality`, `assay`, `is_control`;
- at least one context field among `cell_line`, `cell_type`, `tissue`, `disease`;
- `is_control` includes both control and treated/non-control rows;
- promoted payloads are small enough for safe loader smoke (`n_obs <= 5000`, with
  X smoke-loaded only for tiny examples);
- no full PRISM, T-cell GWPS, XAtlas/Orion, or other huge matrices are loaded
  just to make a benchmark run.

Current v0 member:

```text
viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet
```

Artifacts:

```text
artifacts/schema_audit/define_model_ready_subset_20260621.py
artifacts/schema_audit/model_ready_subset_20260621.md
artifacts/schema_audit/model_ready_subset_20260621.json
artifacts/schema_audit/model_ready_subset_manifest_20260621.tsv
```

## Benchmarks and environments

| area | status | honest interpretation | artifact/docs pointers |
| --- | --- | --- | --- |
| Canonical benchmark loader | present | validates collection/manifest plumbing, not biology by itself | `tools/run_classical_benchmark.py`; current-status MB1 section |
| Classical baselines | ran on model-ready v0 loader contract | useful smoke of metrics/splits; synthetic fallback means no biological conclusion | `artifacts/model_benchmarks/classical_20260622.md` |
| LPM | smoke benchmark accepted | environment/adapter smoke, not a final model comparison | `docs/lpm_baseline.md`; benchmark artifacts under `artifacts/model_benchmarks/` |
| CPA | smoke benchmark accepted | model environment works on bounded data path | `docs/cpa_baseline.md`; model-env artifacts |
| chemCPA | real tiny molecular loader smoke passed | generic model-ready-v0 path is still not chemical; DRUG-seq GSE120222 now provides a 72-row expression-response smoke with PubChem/RDKit fingerprints; PRISM/GDSC remain response-screen candidates, not chemCPA expression inputs | `tools/build_chemcpa_drugseq_tiny.py`; `tools/smoke_chemcpa_drugseq_tiny.py`; `artifacts/model_benchmarks/chemcpa_drugseq_tiny_20260622.md` |
| trVAE legacy | replaced after correct blocker | TensorFlow 1.15 / Python / Mac compatibility issue remains; do not force into core env | `docs/model_environments.md`; `docs/trvae_replacement.md` |
| trVAE replacement | isolated smoke passed | in-repo conditional perturbation VAE analogue using maintained torch/anndata; synthetic fallback smoke only | `artifacts/model_benchmarks/trvae_replacement_20260622.md` |
| GEARS | dependency/API smoke and tiny real Datlinger17 GEARS-contract adapter smoke passed | official `cell-gears==0.1.2` env; bounded real CROP-seq AnnData export exists with controls, gene-symbol conditions, `var.gene_name`, and held-out perturbations; adapter metrics are smoke diagnostics only, not upstream GEARS GNN training or biological performance | `tools/run_gears_benchmark.py`; `artifacts/model_benchmarks/gears_20260622.md`; `artifacts/model_benchmarks/gears_datlinger17_tiny_20260622/report.md`; `artifacts/model_benchmarks/gears_datlinger17_real_smoke_20260622.md` |
| scPRAM | real adapter semantic smoke passed on synthetic data; real data blocked by model-ready subset shape | upstream contract now mapped correctly: binary control/stim condition for one perturbation across real cell types/contexts, not perturbation identity as cell type | `tools/run_scpram_real_adapter.py`; `artifacts/model_benchmarks/scpram_real_adapter_20260622.md` |

## Promotion roadmap

1. Promote more real model-ready members from the 1056-member canonical surface,
   starting with small, balanced, loader-safe examples.
2. Project/revise public/base alias fields where needed:
   `pert_name -> perturbation`, `pert_type -> perturbation_type`,
   `pert_dose -> dose`, `pert_time/time -> timepoint`, and related context fields.
3. Add PRISM paired-loader contracts only after chunk-level control/treated balance
   and safe matrix access are explicit.
4. Keep huge datasets in metadata/chunked plans until bounded matrix adapters can
   smoke one chunk without full local materialization.
5. For scPRAM specifically, promote or export a bounded subset with one selected
   perturbation observed as control/stimulated across at least two real
   cell-type/context values; the current one-member VIPerturb v0 chunk is not a
   semantically valid scPRAM benchmark despite being useful for generic loader
   smoke.
6. For GEARS specifically, the first real bounded subset is now a 240-cell
   Datlinger17 T-cell CROP-seq smoke panel with 40 controls and 40 cells each for
   REL, EGR1, FOS, NFKB1, and NFATC1; NFKB1/NFATC1 are held out. Next GEARS work
   should either run the upstream GEARS GNN against this tiny export or promote a
   larger reviewed panel, but must keep any results labeled by data mode and
   training mode.
7. Add model-specific adapters only when their biological assumptions are explicit
   and reviewable.
8. Re-run benchmarks against real promoted members and label results by data mode:
   synthetic fallback, metadata-only, tiny materialized smoke, or real bounded
   matrix.

## Safety rules for model work

- Do not add heavy/deep model dependencies to the default environment. Keep them
  in extras or isolated `.venv-models/*` environments.
- Do not load huge matrices blindly for benchmark convenience.
- Do not mutate Lamin or promote members from a model script unless the task is an
  explicit reviewed model-ready promotion task.
- Record benchmark artifacts under `artifacts/model_benchmarks/` and link them
  here/current-status instead of copying large tables.
