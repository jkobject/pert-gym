# SCP browser-auth recovery attempt — 2026-06-22

Kanban task: `t_a469ca1d`

## Verdict

No SCP files were recovered or staged in this run. The required OMX/Chrome browser-auth path failed before Chrome could be reached.

The failure is local tooling/runtime, not a renewed SCP source verdict: `omx exec` reached the Codex Chrome extension skill but every `node_repl/js` Chrome bootstrap attempt failed with:

```text
Mcp error: -32602: js: codex/sandbox-state-meta: sandboxCwd must be an absolute file URI: relative URL without a base
```

This occurred both from the pert-gym repo cwd and from `$HOME` with `--skip-git-repo-check`. Therefore no authenticated page was opened, no download section was inspected through Chrome, no files appeared in `~/Downloads`, and no GCS staging occurred.

## Datasets still requiring browser-auth retry

| SCP | current status | notes |
| --- | --- | --- |
| `SCP3301` / `GSE315712` | `not_retried_tool_blocked` | Prior docs: SCP auth-gated; GEO RAW is 33 GB and needs staged/chunked converter if used. |
| `SCP1467` | `not_retried_tool_blocked` | Prior docs: small manifest with heart expression/metadata files; headless downloads were HTTP 401. |
| `SCP211` | `not_retried_tool_blocked` | Prior docs: many kidney organoid files; expression/MTX/metadata family selection remains needed. |
| `SCP3697` | `not_retried_tool_blocked` | Prior docs: SCP record may expose no files/cells; likely still needs alternate processed source. |
| `SCP282` | `not_retried_tool_blocked` | Prior docs: seven expression matrices plus metadata/tSNE sidecars; expression+metadata selection remains needed. |
| `SCP499` | `not_retried_tool_blocked` | Prior T34 probe found the row belongs to GSE121737 early/medium bud repGene family; coordinate with that source-family pattern. |

## Evidence

- Full attempt log: `artifacts/logs/omx_scp_browser_auth_20260622_225316.log`
- Minimal smoke log: `artifacts/logs/omx_scp_browser_auth_smoke_home_skip_20260622_225457.log`
- Smoke JSON: `artifacts/schema_audit/scp_browser_auth_smoke_20260622.json`
- Machine-readable report: `artifacts/schema_audit/scp_browser_auth_recovery_20260622.json`

## Next safe action

Fix the OMX/Codex Chrome extension `node_repl/js` sandbox cwd bootstrap error, then rerun the browser-auth SCP recovery. Treat previous headless HTTP 401 as retryable after tooling is fixed; do not infer that SCP auth still blocks the datasets from this run alone.
