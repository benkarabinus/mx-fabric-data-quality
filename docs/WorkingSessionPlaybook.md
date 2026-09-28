# Working session playbook

A modular agenda for the joint sessions with Manifest MEDEX. Each session stands on its own
and leaves something usable behind, so the engagement can pause or reorder without losing
ground.

> Session 2 needs MX's AWS administrator and a Fabric administrator. Book them early —
> tenant settings and IAM keys are the most common schedule risk.

---

## Before session 1

| Owner | Task |
|---|---|
| MX | An Azure subscription, and someone who can create a resource group and a Fabric capacity |
| MX | A Fabric administrator identified and available |
| MX | A capacity size agreed — F2 is enough for the sample; F64 or larger lets free-licence users view reports |
| Both | Work through the [prerequisites](./Deploy.md#prerequisites) |

Send MX the [permissions list](./Deploy.md#permissions) and
[the AWS-side S3 requirements](./Deploy.md#1-on-the-aws-side) ahead of time.

---

## Session 1 — Deploy and walk the example

**Goal:** MX has a working Fabric workspace and has seen the whole flow run on sample data.

| Step | Detail |
|---|---|
| Verify readiness | `pwsh ./infra/scripts/utils/Test-Prerequisites.ps1` |
| Deploy | `azd up` — see [Deploy.md](./Deploy.md) |
| Run the pipelines | `mx_setup_pipeline` once, then `mx_dq_pipeline` |
| Tour the workspace | Three lakehouses, the notebooks, the pipeline run history |
| Walk one message | `hl7/BVCH/2026/01/BVCH_ADT_A01_0003.hl7`: the file in `mx_bronze`, its row in `bronze.raw_message`, its fields in `hl7.field_value`, its rule results in `dq.fact_rule_result` |
| Show the report | Overview → Scorecard → drill through to the failing messages |

**Talking points**

- Silver keeps `raw_payload` and `bronze_file_path` on every message row, so any number in
  the report traces back to the original bytes.
- Only the fields named in the mapping tables are extracted; everything else stays in the
  raw payload. The PCMC samples carry a custom `ZPD` segment that nothing maps yet — mapping
  it later is a row, not a reprocess of bronze.
- The three participants score differently by design, so the report shows a real spread.

**Exit criteria:** MX can open the workspace and re-run `mx_dq_pipeline` themselves.

---

## Session 2 — Connect MX's source data

**Goal:** MX's own HL7 and CCDA files flow into bronze.

| Step | Detail |
|---|---|
| Confirm the AWS side | Bucket, region, a read-only IAM user, KMS grants if the bucket uses SSE-KMS |
| Create the connection | Fabric portal — [Deploy.md](./Deploy.md#2-on-the-fabric-side) |
| Create the shortcut | `mx_bronze/Files/s3` |
| Verify | List the shortcut from a notebook; confirm object counts match expectations |
| Switch ingestion | Set `SOURCE_ROOT = "Files/s3"` in `ingest_raw_files`, run `mx_dq_pipeline` |

**Decisions to land**

- How much history to bring in for the POC
- Whether MX's prefixes match `hl7/<participant>/<yyyy>/<mm>/`, and if not, how to derive
  the participant and period from the real paths
- Who owns access-key rotation, and how often

> ⚠️ **PHI checkpoint.** Do not point at real data until the
> [security controls](./Deploy.md#security-considerations-for-phi) are agreed.

**Exit criteria:** `bronze.raw_message` holds MX's messages, in volumes MX recognizes.

---

## Session 3 — Load MX's field list

**Goal:** silver extracts the fields MX actually measures.

The sample maps 49 HL7 fields and 13 CCDA fields. MX brings its baseline list of mapped
fields; this session turns it into mapping rows.

| Step | Detail |
|---|---|
| Reconcile | What is on MX's list that the sample does not map, and vice versa? |
| Add rows | `hl7_field_map.csv` (message type, segment, field path) and `ccda_field_map.csv` (XPath, attribute) |
| Load and rerun | Upload the CSVs (redeploy, or upload in the portal), run `seed_mapping_tables`, then `mx_dq_pipeline` — [the round trip](./Extend.md#changing-configuration) |
| Spot check | Compare extracted values with the raw messages for a handful of fields |

Watch for participants using non-standard delimiters, and for custom segments MX wants to
measure.

Reference: [Extend.md](./Extend.md).

**Exit criteria:** every field on MX's list is in `mapping.hl7_field_map` or
`mapping.ccda_field_map`.

---

## Session 4 — Load the real rules and thresholds

**Goal:** the report reproduces MX's measures.

The sample has 33 rules. MX's existing report workbooks hold 389 measures.

| Step | Detail |
|---|---|
| Choose a report | Start with one of MX's reports rather than all of them |
| Add rows | One `quality_rule.csv` row per measure: field path, rule type, threshold, exception flag |
| Confirm denominators | Especially for conditional measures |
| Load and rescore | Same round trip: upload, `seed_mapping_tables`, `mx_dq_pipeline` |
| Compare | `dq.vw_rule_results` side by side with a recent real report |

**The comparison is the point of this session.** Where numbers differ, the cause is usually
a different denominator, a different definition of "populated", or the wrong rule type.

**Exit criteria:** our numbers reconcile with a recent MX report, or every difference is
explained and agreed.

---

## Session 5 — Measures the current rule types cannot express

**Goal:** the calculations that are not field-presence checks.

`gold_score_rules` implements three rule types: `THRESHOLD`, `BIDIRECTIONAL` and
`CONDITIONAL`. MX's workbooks also use shapes such as "any of these fields", a three-way
check, and keyword classification of note text.

| Step | Detail |
|---|---|
| Inventory | Which of MX's measures do not fit the three types? |
| Classify | Can each be restated as an existing type, or does it need a new one? |
| Implement | A new type is a new branch in `gold_score_rules`; its rules are still rows |
| Redeploy | `azd up` (or `Deploy-MxContent.ps1`), then `mx_dq_pipeline` |

**Exit criteria:** every measure in the chosen report is scored.

---

## Session 6 — Reports and ad hoc analysis

**Goal:** MX can produce and distribute the reports themselves.

| Step | Detail |
|---|---|
| Review the semantic model | Measures, relationships, Direct Lake framing |
| Change the report | Open it in Power BI Desktop, edit, commit, redeploy — [Deploy.md](./Deploy.md#the-report) |
| Distribution | Who receives what, and how; F64 or larger lets free-licence users view it |
| Ad hoc | The `mx_gold` SQL analytics endpoint, or `dq.vw_rule_results` in a notebook |
| Investigate an issue | Work a real example, e.g. a participant reports missing discharges |

**Exit criteria:** MX can answer a participant question without help.

---

## Session 7 — Operations and handover

**Goal:** MX can run this.

| Topic | Detail |
|---|---|
| Scheduling | A schedule on `mx_dq_pipeline` for the monthly cycle |
| Monitoring | Pipeline run history, parse errors, volume changes |
| Cost | Capacity pause and resume; a reservation if the POC continues |
| Extending | Add a field and a rule, live |
| Source control | How changes flow from the repository to the workspace |
| Open items | Custom-segment scope, retention, production sizing |

**Exit criteria:** MX owns the environment.

---

## Cheat sheet

```powershell
# Readiness
pwsh ./infra/scripts/utils/Test-Prerequisites.ps1

# Deploy / tear down
azd up
azd down

# Regenerate the sample corpus after editing a template or profile
python infra/data/samples_mx/_generators/gen_samples.py
python infra/data/samples_mx/_generators/verify_spread.py
```

Connecting MX's S3 bucket is a manual portal task —
[Deploy.md](./Deploy.md#connecting-an-amazon-s3-bucket).

In Fabric:

| Item | Purpose |
|---|---|
| `mx_setup_pipeline` | Create the schemas and tables and reseed the mapping tables from the CSVs |
| `mx_dq_pipeline` | Ingest, parse, score and refresh the report's model |
| `drop_all_tables` | Drop every table and start clean |
| `truncate_all_tables` | Empty the tables but keep the schemas |