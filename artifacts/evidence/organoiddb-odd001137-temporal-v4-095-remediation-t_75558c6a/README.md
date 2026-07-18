# Odd001137 / GSE158999 sparse-encoding remediation

Task: `t_75558c6a`

Record: `temporal_v4_095_organoiddb_odd001137_gse158999`

Logical key: `pert-gym/logical/temporal/organoiddb_odd001137_gse158999`

## Root cause

The frozen producer and verifier both returned a literal `format: csr` from `matrix_inventory()` without reading the HDF5 `X` group's `encoding-type`. AnnData physically wrote this transposed source matrix as `csc_matrix`, so producer and verifier agreed with one another while both misdescribed the immutable artifact. Independent QA correctly observed `X/indptr` length 23,962 (variables + 1), not the CSR cardinality of 30,497 (observations + 1).

## Append-only correction

The failed revision was not mutated:

- Frozen revision: `temporal-v4-095-wave10-b1dee01a49f4c78c`
- Frozen manifest remains generation `1784259356995184`, 19,245 bytes.
- Corrected revision: `temporal-v4-095-wave10-c1f63c6ec90e4c24`
- Corrected generation-pinned manifest: `gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/organoiddb_odd001137_gse158999/revisions/temporal-v4-095-wave10-c1f63c6ec90e4c24/manifest.json#1784286707609127`
- Corrected manifest SHA-256: `c5b3b786cebb426ebdc50be19ead968fa258d39241264999e910aa4ec63f4e4b`

A generation-qualified GCS listing and object description observed exactly the frozen and corrected revision prefixes; `immutability-readback.json` records the identities.

## Corrected behavior

Both `build_component.py` and `verify_component.py` now:

1. read the physical HDF5 `X.attrs["encoding-type"]`;
2. map only `csr_matrix` and `csc_matrix` to manifest formats;
3. reject unsupported encodings;
4. derive expected `indptr` cardinality from the physical major axis;
5. reject an `indptr` cardinality mismatch and an indices/data length mismatch.

The builder also recovers a crash-created partial revision without deleting or
renaming it. It accepts only the exact ordered `obs -> X -> var -> ledger ->
manifest` prefix, generation-reads and adopts an existing stage only when its
size and SHA-256 match the locally reconstructed object, and rejects identity
drift, holes, and unexpected objects before the next write.

The corrected manifest and generation-qualified verifier both report:

- format: `csc`
- physical encoding: `csc_matrix`
- shape: 30,496 observations × 23,961 variables
- `indptr` length: 23,962, equal to variables + 1
- stored nonzeros: 159,634,893
- count sum: 1,054,824,189

## Preserved controls

The EU verifier independently re-downloaded and rehashed all five GEO sources and generation-qualified all three payloads, the ledger, and the manifest. It preserved:

- exact catalogue row 95 / Odd001137 / GSE158999 scope;
- frozen control hashes and wave-10 assignment;
- 30,496 observations, 23,961 variables, eight samples;
- day counts 4=6,898, 5=7,783, 6=8,313, 7=7,502;
- ordered obs/X/var axes and all sparse component hashes;
- explicit non-excluding missingness;
- balanced 8/8 input and 5/5 output accounting;
- zero exclusions, zero dropped observations, and zero product credit.

`independent-readback.json` has verdict `PASS`. Product credit remains zero pending the fresh tester-authored independent QA requested as `t_bd4018a9` and subsequent review.

## Verification commands and results

Executed locally and on `pert-gym-worker-eu`:

```text
python test_sparse_inventory.py
SPARSE_INVENTORY_REGRESSION_PASS csr+csc+unsupported

python verify_evidence.py
REMEDIATION_EVIDENCE_PASS revision=temporal-v4-095-wave10-c1f63c6ec90e4c24 manifest=c5b3b786cebb426ebdc50be19ead968fa258d39241264999e910aa4ec63f4e4b encoding=csc_matrix indptr=23962 product_credit=0

python test_revision_recovery.py
REVISION_RECOVERY_REGRESSION_PASS every-stage+drift+hole+extra

python test_evidence_inventory.py
EVIDENCE_SEAL_REGRESSION_PASS corruption+missing+extra
```

`verify_evidence.py` validates every sealed path, byte size, and SHA-256 and
rejects missing or extra packet entries before checking semantic evidence. The
executable metadata-first reconstruction notebook is
`notebooks/datasets/temporal_v4_095_organoiddb_odd001137_gse158999_processing_decisions.ipynb`.

The exact writer/verifier stdout is preserved in `writer-execution.log` and `verifier-execution.log`; generation-pinned output metadata is under `remote-output/output/`.
