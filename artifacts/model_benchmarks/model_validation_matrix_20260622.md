# Model validation matrix — 2026-06-22

- Task: `t_7523ac9a`
- Worktree: `/Users/jkobject/.openclaw/worktrees/pert-gym/t_7523ac9a`
- Base/model commit audited: `origin/model/t_55738901-benchmark-adapters` @ `3da26818f0552d48f8b84df8ac93f280e033b993`
- PR artifact head at matrix generation: `04ce5e318e9c84b5716bc6c60569a2b9e6bdc1f8`

## Evidence class definitions

- `import/env_smoke`: dependency import or environment availability only
- `synthetic_contract_smoke`: deterministic/synthetic rows prove API/data contract only
- `real_bounded_adapter_smoke`: tiny real exported subset with metrics; validates loader/adapter semantics but too small for biological performance
- `real_benchmark_run`: reviewed real dataset benchmark protocol at meaningful scale
- `biological_performance_claim`: claim about model quality/biology; none supported here

## Fresh real run performed for this card

- Command: `PYTHONPATH=$PWD/src /Users/jkobject/.openclaw/workspace/work/pert-gym/.venv-models/scgen/bin/python tools/run_scgen_benchmark.py --real-artifact /Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/scgen_real_viperturb_tiny_20260622.json --artifact-dir artifacts/model_benchmarks --date 20260622_t7523ac9a --task-id t_7523ac9a --epochs 5`
- Exit code: `0`
- JSON: `/Users/jkobject/.openclaw/worktrees/pert-gym/t_7523ac9a/artifacts/model_benchmarks/scgen_real_20260622_t7523ac9a.json`
- MD: `/Users/jkobject/.openclaw/worktrees/pert-gym/t_7523ac9a/artifacts/model_benchmarks/scgen_real_20260622_t7523ac9a.md`
- Status: `real_subset_smoke_passed` on `VIPerturbSeq/vimentin_screen-real-scgen-tiny`; fallback `None`
- scGEN adapter metrics: MAE `3.485205`, RMSE `3.768529`
- Mean-control baseline: MAE `0.932978`, RMSE `1.305958`

## Matrix

| Model | Status | Highest evidence | Can run on current subsets? | Biological performance claim? | Key artifacts |
| --- | --- | --- | --- | --- | --- |
| classical | `pass_contract_smoke_only` | `synthetic_contract_smoke` | model_ready_v0: contract yes but current loader fallback is synthetic; VIPerturb: yes if bounded export is adapted to BenchmarkDataset; no dedicated classical real run artifact found; DRUG-seq: yes for expression prediction if loader supplies X/controls/perturbations; no chemical claim; PRISM: blocked until baseline expression X join; VIPerturb_broad: bounded only; no huge matrix loads | none | `src/pert_gym/models/baselines.py`<br>`src/pert_gym/models/classical.py`<br>`tools/run_classical_benchmark.py`<br>`tests/test_classical_baselines.py` |
| LPM | `pass_contract_smoke_only` | `synthetic_contract_smoke` | model_ready_v0: contract yes via in-memory BenchmarkBatch, but v0 loader fallback is synthetic; VIPerturb: potentially on bounded expression export; DRUG-seq: potentially expression-only; not molecular conditioning; PRISM: blocked until expression features are joined | none | `src/pert_gym/models/lpm.py`<br>`docs/lpm_baseline.md`<br>`tests/test_lpm_baseline.py` |
| CPA | `pass_contract_smoke_only_with_recent_semantic_fix` | `synthetic_contract_smoke` | model_ready_v0: contract yes but fallback synthetic; VIPerturb: potentially on bounded expression export for generic CPA, not chemical CPA; DRUG-seq: potentially expression-only; chemical features belong to chemCPA route; PRISM: blocked until expression X and fields joined | none | `src/pert_gym/models/cpa.py`<br>`tools/run_cpa_benchmark.py`<br>`docs/cpa_baseline.md`<br>`tests/test_cpa_baseline.py` |
| chemCPA | `real_loader_env_smoke_only_not_full_training` | `real_bounded_adapter_smoke` | DRUG-seq: yes for bounded real molecular loader/env smoke; PRISM: blocked: no compound structure + baseline expression join; GDSC: blocked: no compound structure + baseline expression join; model_ready_v0/VIPerturb: not appropriate for chemical conditioning | none | `tools/model_env.py`<br>`tools/smoke_chemcpa_env.py`<br>`tools/build_chemcpa_drugseq_tiny.py`<br>`tools/smoke_chemcpa_drugseq_tiny.py`<br>`src/pert_gym/benchmarks.py::load_chemcpa_drugseq_tiny` |
| GEARS | `real_bounded_adapter_smoke_available` | `real_bounded_adapter_smoke` | Datlinger17_tiny: yes for bounded real adapter smoke; model_ready_v0/VIPerturb: blocked/not appropriate until GEARS-ready gene perturbation AnnData with var.gene_name and coherent context is promoted; PRISM/DRUG-seq: not GEARS single-cell genetic perturbation contract | none | `tools/run_gears_benchmark.py`<br>`artifacts/model_benchmarks/run_gears_datlinger17_real_smoke_20260622.py (local artifact)`<br>`artifacts/model_benchmarks/gears_datlinger17_tiny_20260622/* (local untracked)` |
| scGEN | `fresh_real_subset_smoke_passed` | `real_bounded_adapter_smoke` | VIPerturb model-ready member: yes via bounded local real export; current run uses VIPerturbSeq/vimentin_screen-real-scgen-tiny; PRISM: not an scGEN expression perturbation transfer subset yet; DRUG-seq: not currently wired to scGEN runner; model_ready_v0 direct: generic loader fallback is synthetic; real export required | none | `src/pert_gym/models/scgen_adapter.py`<br>`tools/build_scgen_viperturb_tiny.py`<br>`tools/run_scgen_benchmark.py`<br>`tests/test_scgen_adapter.py` |
| scPRAM | `real_bounded_adapter_smoke_available` | `real_bounded_adapter_smoke` | McFarland20 Trametinib tiny: yes for bounded real context-transfer adapter smoke; model_ready_v0/VIPerturb: infeasible for real scPRAM: unknown context and perturbation-identity screen semantics; PRISM/DRUG-seq: not scPRAM context-transfer contract without paired control/stimulated contexts | none | `tools/run_scpram_real_adapter.py`<br>`artifacts/scripts/export_scpram_mcfarland20_20260622.py (local artifact)`<br>`artifacts/schema_audit/model_ready_scpram_20260622.* (local artifact)` |
| trVAE replacement / legacy trVAE | `replacement_synthetic_smoke_passed_legacy_blocked` | `synthetic_contract_smoke` | model_ready_v0: contract yes but fallback synthetic; VIPerturb/DRUG-seq: potentially with bounded expression BenchmarkDataset export; no real run artifact found for replacement; PRISM: blocked until expression features joined | none | `src/pert_gym/models/conditional_vae.py`<br>`tools/run_trvae_replacement_benchmark.py`<br>`docs/trvae_replacement.md`<br>`tests/test_conditional_vae.py` |

## Per-model notes

### classical

- Status: `pass_contract_smoke_only`
- Highest evidence: `synthetic_contract_smoke`
- Evidence:
  - `synthetic_contract_smoke` — `tools/run_classical_benchmark.py default load_model_ready_v0_or_synthetic` — validated in PR #7 test/make-test handoffs; runner itself emits synthetic fallback metrics
- Notes:
  - Includes mean-control, mean-perturbation, binary split, linear/ridge/elastic-net/random-forest/gradient-boosting regressors.
  - Post-fix BinarySplitBaseline respects requested controls mask.

### LPM

- Status: `pass_contract_smoke_only`
- Highest evidence: `synthetic_contract_smoke`
- Evidence:
  - `synthetic_contract_smoke` — `tests/test_lpm_baseline.py / PR #7 targeted tests` — contract/test evidence only; no real subset benchmark artifact found
- Notes:
  - Latent baseline belongs in generic expression-prediction family; not validated as a biological benchmark.

### CPA

- Status: `pass_contract_smoke_only_with_recent_semantic_fix`
- Highest evidence: `synthetic_contract_smoke`
- Evidence:
  - `synthetic_contract_smoke` — `tools/run_cpa_benchmark.py` — synthetic fallback CPA vs mean-control metrics; PR #7 tests after fix: CPA unseen perturbations use neutral unknown embedding
- Notes:
  - Audit found stale text in tools/run_cpa_benchmark.py describing old unseen->index0 behavior; code/review says neutral unknown embedding is now implemented.

### chemCPA

- Status: `real_loader_env_smoke_only_not_full_training`
- Highest evidence: `real_bounded_adapter_smoke`
- Evidence:
  - `import/env_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/chemcpa_20260622.json` — ["chemCPA env smoke passed", "torch=2.12.1", "anndata=0.11.4 scanpy=1.11.5", "rdkit_fingerprint_bits=6/128", "chemCPA_import=available", "baseline_mae=1.416667"]
  - `real_bounded_adapter_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/chemcpa_drugseq_tiny_20260622.json` — 72 DRUG-seq rows, 128 expression features, 256-bit compound fingerprints; smoke_chemcpa_drugseq_tiny evaluates MeanControlBaseline, not chemCPA training
- Notes:
  - Do not substitute CRISPRi model-ready-v0 synthetic rows for chemCPA chemical conditioning.

### GEARS

- Status: `real_bounded_adapter_smoke_available`
- Highest evidence: `real_bounded_adapter_smoke`
- Evidence:
  - `synthetic_contract_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/gears_20260622.json` — smoke_passed
  - `real_bounded_adapter_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/gears_datlinger17_real_smoke_20260622.json` — {"adapter": "tiny_gears_contract_adapter_real_datlinger17", "interpretation": "real bounded GEARS-contract adapter smoke only; not upstream GEARS GNN training and not biological performance", "mae": 0.5869261026382446, "n_features": 128, "n_test": 80, "n_train": 160, "rmse": 0.9899112994502418, "status": "passed", "test_perturbations": ["NFATC1", "NFKB1"]}
- Notes:
  - Synthetic runner validates official cell-gears dependency/API contract; real Datlinger artifact is still tiny adapter smoke, not full upstream GNN benchmark.

### scGEN

- Status: `fresh_real_subset_smoke_passed`
- Highest evidence: `real_bounded_adapter_smoke`
- Evidence:
  - `real_bounded_adapter_smoke` — `/Users/jkobject/.openclaw/worktrees/pert-gym/t_7523ac9a/artifacts/model_benchmarks/scgen_real_20260622_t7523ac9a.json` — {"baseline_metrics": {"mae": 0.9329783386501734, "rmse": 1.3059583543991486}, "fallback": null, "metrics": {"mae": 3.485204718135218, "rmse": 3.7685293361996965}, "source": "VIPerturbSeq/vimentin_screen-real-scgen-tiny", "status": "real_subset_smoke_passed"}
  - `real_bounded_adapter_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/scgen_real_20260622.json` — {"metrics": {"mae": 0.9830149456162762, "rmse": 1.4477057446517891}, "status": "real_subset_smoke_passed"}
  - `import/env_smoke` — `upstream scgen import probe inside run_scgen_benchmark.py` — {"error": "No module named 'scgen'", "error_type": "ModuleNotFoundError", "interpretation": "upstream scgen==2.1.0 was assessed but is not enabled for benchmark training under the current Python 3.11 scverse resolver", "status": "failed"}
- Notes:
  - This satisfies the card requirement for at least one real-dataset model+metric run through the canonical evaluation path. Upstream scgen package remains dependency-blocked; in-repo ScgenPerturbationAdapter is used.

### scPRAM

- Status: `real_bounded_adapter_smoke_available`
- Highest evidence: `real_bounded_adapter_smoke`
- Evidence:
  - `real_bounded_adapter_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/scpram_real_adapter_20260622_scpram_real_mcfarland20.json` — {"mae": 1.1545988321304321, "rmse": 1.4080130227879262}
  - `synthetic_contract_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/scpram_real_adapter_20260622_scpram_real_mcfarland20.json` — {"mae": 0.6712085604667664, "rmse": 0.8340365105442734}
- Notes:
  - Correct scPRAM mapping is context/cell_type transfer under one perturbation; perturbation_identity is not abused as cell_type.

### trVAE replacement / legacy trVAE

- Status: `replacement_synthetic_smoke_passed_legacy_blocked`
- Highest evidence: `synthetic_contract_smoke`
- Evidence:
  - `synthetic_contract_smoke` — `/Users/jkobject/.openclaw/workspace/work/pert-gym/artifacts/model_benchmarks/trvae_replacement_20260622.json` — {"metrics": {"mae": 0.5238715145323012, "rmse": 0.6760108925502812}, "status": "smoke_passed"}
  - `import/env_smoke` — `docs/trvae_replacement.md / runner candidate survey` — legacy TensorFlow 1.x/lowercase trvae route intentionally not installed
- Notes:
  - Maintained in-repo ConditionalPerturbationVAE replaces legacy trVAE for smoke boundary only.

## Overall verdict

Current model work has strong smoke/contract coverage and several real bounded adapter smokes, but no broad biological performance benchmark claim is supported. scGEN/VIPerturb has a fresh real tiny canonical evaluation run for this card.

## Blocking / follow-up items

- Promote at least one real, reviewed benchmark protocol beyond tiny adapter smoke before claiming biological performance.
- If modifying code later, update stale CPA runner wording that still says unseen perturbations map to index 0; the reviewed implementation now uses a neutral unknown embedding.
- PRISM/GDSC chemCPA remain blocked until compound structures and baseline expression joins are available.
- Direct model_ready_v0 loader remains metadata+synthetic fallback; real subset exporters are the honest path for bounded model smokes.
