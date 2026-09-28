# MX Prism Data Quality on Microsoft Fabric

A small, deliberately simple Microsoft Fabric solution accelerator for **Manifest MEDEX (MX)**.
It shows how HL7 v2 and CCDA messages from participating hospitals and providers can be landed
in a lakehouse, parsed into queryable columns, scored against data-quality rules, and reported
in Power BI — the capability MX runs today as **Prism**.

This is a **worked example, not a finished product**. It is sized so that an MX engineer can
read every notebook in an afternoon, gain a better understanding of basic Spark processing in Fabric, and work through extending the solution to work with actual MX data.

---

## The one idea worth taking away

**The interesting parts are rows in a table, not lines in a notebook.**

Which HL7 fields get extracted, which CCDA elements get extracted, which participants exist,
which measures are scored, and what threshold each measure must clear — all of it lives in four
CSV files that become four Delta tables:

| File | Table | Rows | Answers |
|---|---|---|---|
| `participant.csv` | `mapping.participant` | 3 | Who sends us data? |
| `hl7_field_map.csv` | `mapping.hl7_field_map` | 49 | Which HL7 fields do we extract? |
| `ccda_field_map.csv` | `mapping.ccda_field_map` | 13 | Which CCDA elements do we extract? |
| `quality_rule.csv` | `mapping.quality_rule` | 33 | What do we measure, and how well must it do? |

Adding a field, adding a participant, or changing a threshold is **a row edit**. The notebooks
do not change. There is **never a notebook per provider** — the participant is a column value.

The seeds live in [`infra/data/samples_mx/config/`](infra/data/samples_mx/config).
[`docs/Extend.md`](docs/Extend.md) explains why this is the shape, and how to work in it.

---

## Architecture

```
Amazon S3 (MX bucket)                     Fabric workspace: mx-fabric-dq-poc-<suffix>
  HL7 v2 / CCDA        ──shortcut──▶   ┌──────────────────────────────────────────────┐
  read-only            (or upload)     │ BRONZE · mx_bronze                           │
                                       │   Files/samples_mx/...    raw files, as sent │
                                       │   bronze.raw_message      one row per file   │
                                       │   bronze.ingest_run       one row per run    │
                                       └──────────────────┬───────────────────────────┘
                                                          │  silver_parse_hl7
                                                          │  silver_parse_ccda
                                       ┌──────────────────▼───────────────────────────┐
                                       │ SILVER · mx_silver                           │
                                       │   mapping.*     the four config tables       │
                                       │   hl7.message   hl7.field_value              │
                                       │   ccda.document ccda.field_value             │
                                       │   + vw_message_wide / vw_document_wide       │
                                       └──────────────────┬───────────────────────────┘
                                                          │  gold_score_rules
                                       ┌──────────────────▼───────────────────────────┐
                                       │ GOLD · mx_gold                               │
                                       │   dq.dim_participant  dq.dim_period          │
                                       │   dq.dim_rule         dq.fact_rule_result    │
                                       │   dq.vw_rule_results  (SQL cross-check)      │
                                       └────────┬───────────────────────┬─────────────┘
                                                │ Direct Lake           │ SQL endpoint
                                                ▼                       ▼
                                         Power BI report          Ad hoc SQL
                                         compliance by            investigate a
                                         participant / month      specific message
```

Every silver row carries the **original message text** and the **source file path** it came
from, so any number on the report can be traced back to the bytes that produced it.

---

## How a field becomes a score

This is the whole solution in six steps. Follow `PID-5.1` — the patient's last name.

1. **A mapping row names it.** `hl7_field_map.csv` has a row:

   ```csv
   map_id,message_type,segment,field_path,data_element,active
   MAP-ADT-002,ADT,PID,PID-5.1,Patient Last Name,true
   ```

   That row says: for `ADT` messages, read field `PID-5.1` and label it `Patient Last Name`.

2. **The parser obeys the row.** `silver_parse_hl7` reads every message in
   `bronze.raw_message`, walks the mapping table, and writes one row per
   *(message, mapping)* into `hl7.field_value` — carrying the extracted text **and the
   `map_id`** that produced it. The parser has no idea what `PID-5.1` means; it just follows
   rows.

3. **A view makes it a column.** `hl7.vw_message_wide` pivots those long rows into one row
   per message, with one column per mapped field named after its `data_element`
   (`Patient Last Name` becomes `patient_last_name`). This is the table MX analysts
   would query.

4. **A rule row names it as a measure.** `quality_rule.csv` has a row:

   ```csv
   rule_id,report_name,message_type,measure_group,data_element,field_path,rule_type,...,threshold,exception_allowed,active
   P4P-ADT-002,P4P Participant Report,ADT,Information,Name,PID-5.1,THRESHOLD,,,1,false,true
   ```

   That row says: on the P4P Participant Report, `PID-5.1` must be populated, and the
   target is 100%.

5. **The scoring notebook answers one question per message.**
   `gold_score_rules` asks, for each *(message, rule)* pair: **does this rule apply to this
   message, and did this message satisfy it?** It writes two booleans into
   `dq.fact_rule_result`. It stores **no percentages** — only facts about individual messages.

6. **DAX turns facts into a rate.** Five core measures do the arithmetic. `Compliance %` is
   `DIVIDE([Numerator], [Denominator])`; `Status` compares that to the rule's own threshold
   and returns Pass / Warning / Fail / N/A. Slice by participant, by month, by measure group —
   the same five measures answer all of it. Nine more summarise those results for the report's
   cards and colours; none of the fourteen names a specific rule.

The split is deliberate: **the notebook answers "did this message comply?", DAX answers "what
rate, against what threshold, in this slice?"** That is why 33 rules need no rule-specific
measures.

> **An empty population scores `N/A`, never 0%.** If a participant sent no messages a rule
> applies to, reporting 0% would misrepresent them. This mirrors MX's existing
> `IF(denominator=0,"n/a",...)` behaviour.

---

## What's in the repository

```
fabric_workspace/          everything that gets deployed into Fabric
  notebooks/               9 pipeline notebooks + 2 maintenance notebooks
  pipelines/               mx_setup_pipeline + mx_dq_pipeline
  reports/                 mx_data_quality Direct Lake semantic model + 4-page report
infra/
  data/samples_mx/         the sample corpus and the four config seeds
  scripts/fabric/          deployment scripts (PowerShell + Fabric REST)
  *.bicep                  the Fabric capacity
docs/
  Deploy.md                stand it up
  Extend.md                change it
```

The notebooks in `fabric_workspace/notebooks/` **are the source**. There is no build step,
no code generator, and no Python package to install. Edit a notebook, redeploy, done.

### The notebooks and pipeline split

| Pipeline | Notebook order | Does |
|---|---|---|
| `mx_setup_pipeline` | `schema_model_bronze -> schema_model_silver -> seed_mapping_tables -> schema_model_gold` | Creates schemas/tables/views and seeds all four mapping tables |
| `mx_dq_pipeline` | `ingest_raw_files -> silver_parse_hl7 -> silver_parse_ccda -> gold_score_rules -> refresh_semantic_model` | Processes the retained source files, scores them, and points the report at the result |

Both pipelines are linear and success-only. Deployment publishes them but does not
run either one. Run setup once in a new workspace, then run processing routinely;
an already initialized workspace normally needs only `mx_dq_pipeline`. Setup uses
`IF NOT EXISTS` for tables and replaces the gold cross-check view. Seeding
overwrites all four mapping tables from the uploaded CSVs, so a setup rerun can
erase table-only edits.

---

## Sample data

`infra/data/samples_mx/` ships **150 synthetic messages** — 126 HL7 v2 and 24 CCDA — across
three fictional participants and two months (`2026/01`, `2026/02`). The folder layout is
load-bearing: ingestion derives the participant and the period from the path.

```
hl7/ARRMC/2026/01/ARRMC_ADT_A01_0001.hl7
ccda/PCMC/2026/02/PCMC_CCD_0047.xml
```

The corpus is authored to produce a **known quality spread**, so that a correct deployment
produces a predictable result:

| Participant | Name | Pass | Warning | Fail | Overall |
|---|---|---:|---:|---:|---:|
| ARRMC | Arroyo Regional Medical Center | 66 | 0 | 0 | 100.0% |
| BVCH | Bay Valley Community Hospital | 38 | 4 | 16 | 65.5% |
| PCMC | Pinecrest Community Medical Center | 16 | 4 | 42 | 25.8% |

Matching those numbers confirms the plumbing works. It is **not** evidence that the rules are
right — the same repository produced both the data and the rules. Validating the rules against
MX's real messages is the point of the working sessions, not of this corpus.

> These are synthetic files with invented patient and facility names. No real PHI is present.

---

## Getting started

1. **[`docs/Deploy.md`](docs/Deploy.md)** — prerequisites, `azd up`, what gets created, running
   the pipeline, verifying the result, connecting Amazon S3, and teardown.
2. **[`docs/Extend.md`](docs/Extend.md)** — how the metadata-driven approach works, what each
   report measure means, and how to add a participant, a field, a rule, a threshold, or a new
   rule type.

Deployment needs only `azd`, `az` and PowerShell 7. Nothing is installed from PyPI — not
locally, not in Fabric. Python is needed only if you want to regenerate the sample corpus.

---

## Known gaps

- **The report is a starting point, not a specification.** Four pages deploy with the
  solution, over a Direct Lake model. Its layout is a design decision MX should own —
  open it in Power BI Desktop and change it. See
  [The report](docs/Deploy.md#the-report).
- **Three rule types are implemented** — `THRESHOLD`, `BIDIRECTIONAL`, `CONDITIONAL` — covering
  the 33 sample rules. MX's existing reports also use `ANY_OF`, `TRIDIRECTIONAL` and `KEYWORD`
  rules; adding a type is a new branch in `gold_score_rules`. See
  [`docs/Extend.md`](docs/Extend.md).
- **Rule semantics are evaluated per message.** The POC will not reproduce MX's macro numbers
  exactly for every rule. Reconciling the two is working-session work.
- **Matching the sample baseline is a plumbing check, not clinical validation.** See
  [Verifying the deployment](docs/Deploy.md#verifying-the-deployment).

---

## Working sessions

[`docs/WorkingSessionPlaybook.md`](docs/WorkingSessionPlaybook.md) is the agenda for sessions
with MX that validate the rules against real messages. It is not required reading to deploy or
extend the solution.

---

## Contributing / Support / Legal

See [CONTRIBUTING.md](CONTRIBUTING.md), [SUPPORT.md](SUPPORT.md),
[SECURITY.md](SECURITY.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

This project may contain trademarks or logos for projects, products, or services. Authorized
use of Microsoft trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
