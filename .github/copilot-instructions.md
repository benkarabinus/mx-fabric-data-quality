# MX Prism Data Quality on Microsoft Fabric

This repository is a **metadata-driven POC for Manifest MEDEX (MX)** built on Microsoft
Fabric. It shows how HL7 v2 and CCDA messages from participating hospitals land in a
lakehouse, get parsed into queryable fields, get scored against data-quality rules, and
end up in a Power BI report — with **which fields are extracted and which rules are
applied both held in mapping tables, not in code**.

It is a teaching example. MX should be able to read it, understand the pattern, and extend
it themselves. **Simplicity is the requirement, not a nice-to-have.** If a change makes
the solution more capable but harder to follow, it is probably the wrong change.

How it works and how to extend it: [`docs/Extend.md`](../docs/Extend.md).
Deploying: [`docs/Deploy.md`](../docs/Deploy.md).
Sessions with MX: [`docs/WorkingSessionPlaybook.md`](../docs/WorkingSessionPlaybook.md).

**Check the tree before stating a repo fact.** A copy of these instructions loaded at
session start can be older than the file on disk. Before claiming something is broken,
missing or present, confirm it with a file read, `git ls-files` or a live call.

## Repository shape

Three folders, nothing else that matters:

```
fabric_workspace/   notebooks, pipelines, lakehouse defs, semantic model, report  (the solution)
infra/              Bicep, deployment scripts, sample data and seeds     (how it gets there)
docs/               Deploy.md, Extend.md, WorkingSessionPlaybook.md          (how to use it)
```

There is **no `src/`, no build step and no code generator.** The notebooks under
`fabric_workspace/notebooks/` are hand-written source and are edited directly, and the
PBIR files under `fabric_workspace/reports/mx_data_quality.Report/` are edited in Power
BI Desktop. Keep it that way; do not introduce a generator.

## Architecture

Three medallion lakehouses, one schema per concern:

| Lakehouse | Schemas | Holds |
|---|---|---|
| `mx_bronze` | `bronze` | Uploaded HL7 v2 / CCDA files, `raw_message`, `ingest_run` |
| `mx_silver` | `hl7`, `ccda`, `mapping` | Parsed field values, plus the mapping tables that drive everything |
| `mx_gold` | `dq` | `dim_participant`, `dim_period`, `dim_rule`, `fact_rule_result`, `vw_rule_results` |

Power BI reads `mx_gold` in **Direct Lake** mode, binding the four Delta tables directly.

The notebooks are split into a one-time setup pipeline and a routine processing
pipeline:

```
mx_setup_pipeline: schema_model_bronze -> schema_model_silver -> seed_mapping_tables -> schema_model_gold
mx_dq_pipeline:    ingest_raw_files -> silver_parse_hl7 -> silver_parse_ccda -> gold_score_rules -> refresh_semantic_model
```

`refresh_semantic_model` frames the Direct Lake model against the gold tables the run
just wrote. It is not optional: a model published over REST has never been framed, and
until it has been, the workspace Refresh button is rejected with *"Unable to load a
query that produces no tables"* and leaves no refresh-history entry. See
[`docs/Deploy.md`](../docs/Deploy.md#direct-lake-framing).

Both are linear, success-only pipelines. Setup creates tables with `IF NOT EXISTS`
and replaces the gold cross-check view. Seeding overwrites all four mapping tables
from uploaded CSVs, so setup can erase table-only edits. Processing requires setup
to have completed and does not set up or seed. Deployment publishes both pipelines
and files but never auto-runs them.

`data_management/{drop_all_tables,truncate_all_tables}` exist for manual cleanup and are
deliberately **not** in the pipeline.

## The split that defines the design

| Question | Answered by | Why |
|---|---|---|
| *Did this message satisfy this rule?* | `gold_score_rules` notebook, one row per message × rule | Needs the parsed message |
| *What is the rate, and does it clear the threshold?* | DAX in the semantic model | Needs the slicer context |

`fact_rule_result` stores `is_applicable` and `is_satisfied` booleans. **No percentage is
stored anywhere.** Rates are computed in DAX so they recalculate for whatever participant,
period or rule the user selects. This replaces MX's Excel macros with something they can
read in the model.

## Rules are data, not code

Four CSVs under `infra/data/samples_mx/config/` are the control surface:

| File | Seeds | Governs |
|---|---|---|
| `participant.csv` | `mapping.participant` | Who sends data |
| `hl7_field_map.csv` / `ccda_field_map.csv` | `mapping.hl7_field_map` / `ccda_field_map` | Which fields get extracted |
| `quality_rule.csv` | `mapping.quality_rule` | Which rules are scored, and at what threshold |

Adding a measured field or changing a threshold is a **row edit, not a code change**.

**If a request seems to call for hardcoding a field, participant or threshold, add a
mapping row instead.** There is never a per-provider notebook — participant is a column
value, not a code path.

The layering, from most to least often changed:

| Layer | Changes when | Type of change |
|---|---|---|
| Participants | a new provider sends data | folder on the path + row in `mapping.participant` |
| Field maps | a new field must be extracted | row in `hl7_field_map` / `ccda_field_map` |
| Quality rules | a new measure or threshold | row in `quality_rule` |
| Rule *types* | a genuinely new rule **shape** | ~10 lines in `gold_score_rules` |
| Notebooks | almost never | — |

### Rule types

The POC implements three of MX's six types, per message:

| Type | Applicable when | Satisfied when |
|---|---|---|
| `THRESHOLD` | always | `field_path` populated |
| `BIDIRECTIONAL` | always | `field_path` **or any** `companion_field_paths` populated |
| `CONDITIONAL` | `condition_field_path` populated | `field_path` populated |

`ANY_OF`, `TRIDIRECTIONAL` and `KEYWORD` are **not implemented**. Adding one is the worked
example in [`docs/Extend.md`](../docs/Extend.md).

### Two conventions that are deliberate and easy to break

- **An empty population scores `N/A`, never 0%.** Reporting 0% for a feed that sent nothing
  misrepresents the participant. The `Compliance %` measure returns `BLANK()` when the
  denominator is zero; `Status` turns that into `"N/A"`. Mirrors MX's
  `IF(denominator=0,"n/a",...)`.
- **The two-name system.** `field_path` is the technical locator (`PID-5.1` for HL7, a
  snake_case key for CCDA); `data_element` is the human label (`Patient Last Name`), shared
  across both formats. `quality_rule.field_path` joins to `*_field_map.field_path`; the
  wide views pivot on `data_element`. Do not collapse them.

## No local dependencies

Deployment needs only `azd`, `az` and `pwsh` 7. Nothing is installed from PyPI — not on the
machine, not in the dev container, not in Fabric. Every Python file in the repository uses
only the standard library; the notebooks add `pyspark` and `sempy`, both supplied by the
Fabric runtime.

Python is needed locally **only** to regenerate the sample corpus. Do not introduce a
third-party import without saying so explicitly.

For optional local checks, use the system Python interpreter. Do not create a virtual
environment or install packages; everything is standard library.

## Deployment

`azd up` provisions the capacity and runs the scripts as hooks. Everything is uploaded
from the local working tree over the Fabric REST API — nothing is downloaded from GitHub,
so the repository can stay private.

- `infra/scripts/fabric/FabricApi.psm1` — REST helpers, token caching, retries
- `infra/scripts/fabric/Install-MxSolution.ps1` — workspace and administrators
- `infra/scripts/fabric/Deploy-MxContent.ps1` — lakehouses, notebooks, both pipelines,
  semantic model, report and sample data (six steps; publishes only, runs nothing)
- `infra/scripts/fabric/Remove-MxSolution.ps1` — teardown, run by `azd down`
- `infra/scripts/utils/Test-Prerequisites.ps1` — read-only pre-flight check

Amazon S3 is **manual setup, documented in
[`docs/Deploy.md`](../docs/Deploy.md#connecting-an-amazon-s3-bucket)**. There is no script
for it and one should not be added.

The deploy script validates pipeline and notebook names, notebook bindings and placeholder
resolution before it publishes anything. `-SkipPipeline` and `-SkipNotebookRun` are
compatibility no-ops. Deployment never runs a pipeline; do not add an automatic
`Start-FabricPipelineJob` call.

## Traps — each of these cost real debugging time

**The notebook job API can hide the real exception** behind `System cancelled the Spark
session due to statement execution failures`. For pipeline-triggered notebooks, use the
pipeline activity API below: it returned the full traceback, cell number and source line.
Use interactive execution when the activity output is insufficient.

1. **Lakehouses must be created schema-enabled** — `creationPayload.enableSchemas = true`.
   Off by default via REST, and **cannot be changed after creation**. Without it the first
   `CREATE SCHEMA` fails.
2. **Cross-lakehouse reads need three-part names.** `spark.table("bronze.raw_message")`
   resolves inside the notebook's *own* default lakehouse. Use
   `mx_bronze.bronze.raw_message`, and attach every lakehouse the notebook touches.
3. **Do not orchestrate with `mssparkutils.notebook.run()`.** Children share the parent's
   Spark session, and a failing child *cancels* it rather than raising — one generic error
   for any processing notebook. Each pipeline runs its notebooks as separate activities
   so each failure is attributable.
4. **Write with `INSERT OVERWRITE TABLE … SELECT <explicit columns>`, not
   `saveAsTable(overwriteSchema=true)`.** The latter replaces the table and destroys the
   `COMMENT` metadata the schema notebooks set. Two writes are exceptions and do use
   `saveAsTable(overwriteSchema=true)`: the seed loads in `seed_mapping_tables` and
   `bronze.raw_message` in `ingest_raw_files`.
5. **TMDL is position-sensitive.** A `///` description must immediately precede what it
   describes, and multi-line DAX must begin on the line *after* the `=`.
6. **Never bind a SQL view to the Direct Lake model.** `dq.vw_rule_results` is a
   human-readable cross-check only; binding it forces DirectQuery fallback.
7. **The SQL endpoint lags the lakehouse.** `Invalid object name` is not proof the pipeline
   failed — check the lakehouse Tables view first.
8. **Placeholder GUIDs are rewritten at publish time.** Notebooks and both pipelines carry
   fixed placeholder workspace, lakehouse and notebook IDs that `Deploy-MxContent.ps1`
   substitutes. Keep the maps at the top of that script in sync if you add an item.
9. **Bronze success is `ingest_status = "INGESTED"`, not `"OK"`.** Both silver notebooks
   originally filtered on `"OK"` and succeeded with empty outputs. Reconcile row counts;
   a successful activity does not prove that data flowed.

## Useful Fabric debugging tools

Prefer the Fabric MCP tools, where available, for inventory and bounded inspection:
listing workspaces, items, schemas and tables, and previewing rows. A preview is bounded;
never present it as a full row count or exhaustive validation.

- **Pipeline run diagnostics.** With base `https://api.fabric.microsoft.com/v1`, GET
  `/workspaces/{workspaceId}/items/{pipelineId}/jobs/instances` for run status, then POST
  `/workspaces/{workspaceId}/datapipelines/pipelineruns/{runId}/queryactivityruns` with
  `lastUpdatedAfter` and `lastUpdatedBefore` UTC timestamps covering the run, plus
  `filters: []` and `orderBy: []` — an empty body returns no activities. There is **no
  pipelineId segment** in the activity URL. Each activity's
  `output.result.error.traceback` holds the real exception.
- **Notebook round-trip.** POST
  `/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition?format=ipynb` with `{}`
  and decode the InlineBase64 part. On HTTP 202, poll the `Location` header, then GET
  `{Location}/result`. `updateDefinition` persists an edit; it does not run the notebook.
  When bringing a remote edit back into the repository, restore the placeholder IDs.
- **Authentication.** Use a token for `https://api.fabric.microsoft.com`; never print or
  store it.
- **Report authoring.** `@microsoft/powerbi-report-authoring-cli` (`powerbi-report-author`)
  is an optional local aid, not a dependency. `validate <folder>` catches unknown
  formatting properties before publishing; `formatting describe-object <visualType>
  <object>` is the reliable source of property names — do not guess them. `preview`
  renders a report locally and needs a Chromium from `npx playwright install chromium`.

## Known gaps

- **The report is a starting point, not a specification.** Four pages deploy over the
  Direct Lake model; MX owns the layout and changes it in Power BI Desktop, following
  [`docs/Deploy.md`](../docs/Deploy.md#the-report). The PBIR files are the source of
  truth — no generator produces them.
- **Three of six rule types are implemented** (above).
- **A fresh-workspace deployment passed end to end on 2026-09-27.** `azd up`, then
  `mx_setup_pipeline`, then `mx_dq_pipeline` (including `refresh_semantic_model`); the
  semantic model framed and the report rendered. That outcome was observed by the user
  in Fabric. Row counts were **not** re-reconciled in that workspace.
- **Row counts come from an earlier validation in an existing workspace:** raw 150; HL7
  126 messages/1532 fields; CCDA 24 documents/312 fields; gold dimensions 3/2/33 and
  1128 facts; all 186 reporting cells matched `verify_spread.py`. This is plumbing
  evidence, not clinical validation.

## Sample data

`infra/data/samples_mx/` holds **150 synthetic messages** — 126 HL7 v2, 24 CCDA — across
three participants and two months, plus the config seeds. They are authored to produce a
known, deliberately uneven quality spread:

```
ARRMC  Arroyo Regional Medical Center        100.0%
BVCH   Bay Valley Community Hospital          65.5%
PCMC   Pinecrest Community Medical Center     25.8%
```

Matching those numbers confirms the plumbing works; it is **not** evidence the rules are
right, because the same repository generated both the data and the expectation. Validating
against MX's real messages is the job of the working sessions.

Layout is load-bearing: `hl7/<participant>/<yyyy>/<mm>/<file>` — ingestion derives
participant and period from the path. `_templates/` holds the hand-authored seeds,
`_generators/gen_samples.py` expands them, `_generators/verify_spread.py` checks the
result. Both are standard library only. Neither folder is uploaded.

`.gitattributes` marks `*.hl7` **binary**. HL7 v2 uses bare `\r` segment terminators that
git would otherwise corrupt. Read and write these files in binary or with `newline=""`.
