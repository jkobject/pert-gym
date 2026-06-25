# PRISM residual inventory — t_c164fce1 / 2026-06-24

Fresh inventory was recomputed from current `origin/main` worktree plus live GCS/Lamin state before selecting the next tranche.

## Inputs

- Worktree: `/Users/jkobject/.openclaw/worktrees/pert-gym/t_c164fce1-prism-cont13`
- Base commit: `68d847624a10ea06949b2486ded298c0300a16af` (`origin/main`, after PR #34 merge)
- GCS prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
- Lamin: `laminlabs/pertdata`, branch `jkobject`

## Selection result

Selected and ingested: `GSE281048_TGFB_Perturb_seq`.

Rationale:

- It remained a live residual candidate after excluding already-ingested staged files, duplicate `(1)` copies, truncated `GSE274751`, missing-source rows, and user-excluded `GSE90063_human-004`.
- It matched the prior handoff candidate but was revalidated rather than trusted blindly.
- GCS object was visible with byte size `2,767,634,550`.
- Backed source probe: `236,606 × 33,525`, `X` layout `_CSRDataset`, obs fields include `gene`, `guide`, `condition`, `perturbation_name`, `organism`, `crispr_type`; var fields include Seurat VST fields.
- Safer than the largest remaining candidates such as `GSE210681` (`794,783 × 36,601`, 26.1 GB staged object).

## Fresh queue classification

Staged h5ads: 33.

Already represented in Lamin at inventory time: `GSE236057`, `GSE236519`, `GSE241683_carT`, `GSE243244`, `GSE246714`, `GSE247274`, `GSE247599`, `GSE250558`, `GSE252589`, `GSE254100`, `GSE255832`, `GSE263524`, `GSE267982`, `GSE269596`, `GSE280767`, `GSE283614`.

Excluded/preserved:

- duplicate copies: `GSE247274 (1)`, `GSE267982 (1)`;
- truncated/source-unsafe: `GSE274751`;
- user-excluded: `GSE90063_human-004` remains excluded (not present as staged h5ad in this prefix);
- missing-source rows remain excluded from active ingestion unless recovered: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.

Residual candidates after this tranche (do not start the next continuation until this PR is accepted/merged):

- `GSE210681`
- `GSE235325`
- `GSE241683_cropseq`
- `GSE241683_pilot`
- `GSE251715`
- `GSE261025`
- `GSE270828`
- `GSE273271`
- `GSE278572`
- `GSE281048_IFNB_Perturb_seq`
- `GSE281048_IFNG_Perturb_seq`
- `GSE281048_INS_Perturb_seq`
- `GSE281048_TNFA_Perturb_seq`

## Verification artifact

Final verification for the selected tranche is in:

- `artifacts/schema_audit/prism_gse281048_tgfb_verification_20260624.md`
- `artifacts/schema_audit/prism_gse281048_tgfb_verification_20260624.json`
- verifier script: `artifacts/scripts/verify_prism_gse281048_tgfb_20260624.py`

No X matrices were loaded during verification.
