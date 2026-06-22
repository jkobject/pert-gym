# Deduplication policy

Before any new ingestion, check both the public `main` data visible in
`laminlabs/pertdata` and branch `jkobject`. The branch must avoid duplicating
datasets already present anywhere in the database.

## Required checks before ingestion

1. Source identity:
   - GEO/SRA accession;
   - DOI / Figshare / Zenodo / CZI / Drive source;
   - author dataset name;
   - raw file URI and hash where available.

2. Matrix identity:
   - n_obs, n_vars;
   - var ID set and order;
   - observation barcode overlap if accessible;
   - perturbation name/library overlap.

3. Subduplicate detection:
   - dataset A is a subset of B;
   - same cells with different filtering;
   - same experiment split across files;
   - raw counts vs normalized/log-transformed version.

4. Decision:
   - ingest new dataset;
   - skip duplicate;
   - ingest as variant with explicit relationship;
   - merge with existing logical dataset;
   - keep as typed auxiliary artifact only (`X_<name>/var_<name>` or `obsm_<name>`).

## Reporting

Write candidates to:

```text
artifacts/schema_audit/duplicate_candidates.tsv
```

and final decisions to the phase progress JSON or the schema audit repair plan.
