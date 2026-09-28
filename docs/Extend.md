# Extending the solution

This guide is in two halves.

The **first half explains how the metadata-driven approach actually works** — what the
phrase means here, one field traced from a spreadsheet row all the way to a percentage on
a report, and an honest account of where the data stops and the code starts. Read it
before you change anything. Nearly every question of the form *"where do I add X?"*
answers itself once the shape is clear.

The **second half is the how-to**: add a participant, extract a new field, score a new
measure, move a threshold, add a genuinely new rule shape, and get files into the lake.

---

## Part 1 — How the approach works

### What "metadata-driven" means here

The conventional way to build this pipeline is to write the field list into the code:

```python
# the version we did NOT build
message = {
    "patient_id":         get_value(segments, enc, "PID", "PID-3.1"),
    "patient_last_name":  get_value(segments, enc, "PID", "PID-5.1"),
    "patient_first_name": get_value(segments, enc, "PID", "PID-5.2"),
    ...
}

if not message["patient_last_name"]:
    failures.append("Name")
```

That works, and it is how most first drafts look. The cost shows up on the second change.
Adding a field means editing the parser, editing the table definition, editing the scoring
logic, testing, and redeploying. Whoever owns the quality measures cannot make that change
themselves — it belongs to whoever owns the notebook. And when a second provider needs a
slightly different set of fields, the temptation is to copy the notebook, which is how you
end up with one pipeline per provider.

The version we built states the same thing as **data**:

```
map_id,message_type,segment,field_path,data_element,active
MAP-ADT-002,ADT,PID,PID-5.1,Patient Last Name,true
```

The notebook contains no mention of `PID-5.1`, of patient names, or of any particular
provider. It reads the mapping table, loops, and extracts whatever the rows tell it to.
Adding the twelfth field is exactly as much work as adding the second: one row.

Four tables hold everything that varies:

| Table | Rows | Answers |
|---|---|---|
| `mapping.participant` | 3 | Who submits data |
| `mapping.hl7_field_map` | 49 | Which HL7 v2 fields we extract |
| `mapping.ccda_field_map` | 13 | Which CCDA elements we extract |
| `mapping.quality_rule` | 33 | Which of those we score, how, and against what threshold |

They are seeded from four CSVs in
[`infra/data/samples_mx/config/`](../infra/data/samples_mx/config) — see
[Changing configuration](#changing-configuration) for the round trip.

### One field, end to end

Follow `PID-5.1` — the patient's last name — from configuration to a number on a report.
Every other field takes the identical path.

#### 1. A row says the field exists

`infra/data/samples_mx/config/hl7_field_map.csv`:

```
map_id,message_type,segment,field_path,data_element,active
MAP-ADT-002,ADT,PID,PID-5.1,Patient Last Name,true
```

`segment` and `field_path` tell the parser where to look. `data_element` is the human
label — it is what a person reading a report sees. `active` lets you retire a mapping
without deleting the row and losing its history.

#### 2. The silver notebook extracts it

`silver_parse_hl7` reads the mapping rows for each message's type and extracts each one.
It writes a row per (message, mapping) into `hl7.field_value`:

| message_uid | map_id | field_path | data_element | value | is_populated |
|---|---|---|---|---|---|
| `hl7/ARRMC/2026/01/ARRMC_ADT_A01_0001.hl7` | `MAP-ADT-002` | `PID-5.1` | `Patient Last Name` | `Whitfield` | `true` |

Three things about this table are deliberate:

- **`map_id` is carried on every value.** You can always trace a value back to the row
  that asked for it. If a field looks wrong, that is where you start.
- **`is_populated` is the single fact every completeness rule is scored from.** It is
  computed once, here, so scoring never has to re-decide what "present" means.
- **Long, not wide.** A new mapping row adds *rows*, not columns, so no table needs
  altering when the field list grows.

The message itself — `raw_payload`, the complete original text, and `bronze_file_path`,
where the file lives — sits beside it in `hl7.message`. Nothing is thrown away, so a
question the current field list cannot answer can still be answered by going back to
the source.

#### 3. A view assembles the wide shape

Long tables are correct but awkward to read. At the end of the run, `silver_parse_hl7`
generates `hl7.vw_message_wide`, one row per message with one column per mapped field:

**That view's SQL is generated from the mapping table, not typed.** This is the whole of
it, from `silver_parse_hl7`:

```python
def column_name(data_element):
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", data_element or "").strip("_").lower()
    return slug or "unnamed"

elements = sorted({
    row["data_element"]
    for row in field_map.select("data_element").distinct().collect()
    if row["data_element"]
})

pivot_columns = ",\n".join(
    "    MAX(CASE WHEN f.data_element = '{literal}' THEN f.value END) AS {column}".format(
        literal=element.replace("'", "''"),
        column=column_name(element),
    )
    for element in elements
)

key_select = ",\n".join(f"    m.{c}" for c in KEY_COLUMNS)
key_group = ", ".join(f"m.{c}" for c in KEY_COLUMNS)

view_sql = f"""
CREATE OR REPLACE VIEW hl7.vw_message_wide AS
SELECT
{key_select},
{pivot_columns}
FROM hl7.message m
LEFT JOIN hl7.field_value f ON f.message_uid = m.message_uid
GROUP BY {key_group}
"""
spark.sql(view_sql)
```

The notebook reads the distinct `data_element` values out of the mapping table, slugifies
each one into a column name, and emits one `MAX(CASE WHEN ...)` per element. `KEY_COLUMNS`
is the fixed spine — `message_uid`, `participant_id`, `message_type`, `message_datetime`,
`period_year`, `period_month`, `bronze_file_path`, `parse_status`. Add a mapping row and
the view gains a column on the next run, with no schema change and nobody editing SQL.

> **Two names, one field.** `field_path` is the technical locator (`PID-5.1`); it is what
> rules join on. `data_element` is the label (`Patient Last Name`); it is what the wide
> view names its columns after, lower-cased with spaces replaced by underscores. The
> snake_case form for an HL7 field is *derived at view-build time* and stored nowhere.
> CCDA is the other way round: its `field_path` **is** the slug (`patient_last_name`),
> because a raw XPath makes a poor join key.

#### 4. A rule says the field is measured

`infra/data/samples_mx/config/quality_rule.csv`:

```
rule_id,report_name,message_type,measure_group,data_element,field_path,rule_type,companion_field_paths,condition_field_path,threshold,exception_allowed,active,notes
P4P-ADT-002,P4P Participant Report,ADT,Information,Name,PID-5.1,THRESHOLD,,,1,false,true,
```

`field_path` is the join back to the mapping. `rule_type` names the *shape* of the test.
`threshold` is the bar — here 100%, because a message without a patient name is not usable.

Note the two layers are separate on purpose: **a mapping row without a rule row is
extracted but not scored.** That is how you stage work. Get a field flowing, confirm it
looks right, then decide what good looks like.

#### 5. The gold notebook answers one question per message

`gold_score_rules` writes `dq.fact_rule_result` — one row per message per rule written for
that message's type:

| message_uid | participant_id | period_key | rule_id | is_applicable | is_satisfied |
|---|---|---|---|---|---|
| `hl7/ARRMC/2026/01/ARRMC_ADT_A01_0001.hl7` | `ARRMC` | `202601` | `P4P-ADT-002` | `true` | `true` |

It answers exactly one question — *did this message satisfy this rule?* — and stores no
percentages. A percentage is a rate over a *slice*, and the slice is not known until
somebody picks a participant and a month in the report.

#### 6. DAX turns rows into a percentage

Five measures on `fact_rule_result` do all the rate and threshold arithmetic:

```dax
Numerator      = CALCULATE(COUNTROWS(fact_rule_result), fact_rule_result[is_satisfied] = TRUE)
Denominator    = CALCULATE(COUNTROWS(fact_rule_result), fact_rule_result[is_applicable] = TRUE)
Compliance %   = IF([Denominator] = 0, BLANK(), DIVIDE([Numerator], [Denominator]))
Rule Threshold = SELECTEDVALUE(dim_rule[threshold])
Status =
  IF(ISBLANK([Compliance %]), "N/A",
     IF(ISBLANK([Rule Threshold]), "Mixed",
        IF([Compliance %] >= [Rule Threshold], "Pass",
           IF(SELECTEDVALUE(dim_rule[exception_allowed]), "Warning", "Fail"))))
```

Because the fact is at message grain, the same five measures give you a rate for one rule
in one month for one participant, or for everything at once, or for any slice between —
without a single additional measure being written.

> **An empty population scores N/A, never 0%.** If a participant sent no messages carrying
> a Patient Class, the conditional rule that depends on it has an empty denominator.
> Reporting 0% would say they failed; they simply had nothing to be measured on. This
> mirrors MX's existing `IF(denominator=0,"n/a",...)` and is why `Compliance %` returns
> `BLANK()` rather than zero.

Nine more measures summarise those results for the report's cards and colours. All
fourteen are listed in [Report measures and dimensions](#report-measures-and-dimensions).

### Which layer changes for which kind of change

| Layer | Changes when | What the change is |
|---|---|---|
| **Participants** | a new provider starts sending | a folder on the ingest path and a row in `mapping.participant` |
| **Field maps** | a new field needs extracting | a row in `hl7_field_map` or `ccda_field_map` |
| **Quality rules** | a new measure, or a threshold moves | a row — or an `UPDATE` — in `quality_rule` |
| **Rule types** | a genuinely new rule *shape* | roughly ten lines in `gold_score_rules` |
| **Notebooks** | almost never | — |
| **Parsers** | a new message format entirely | a new silver notebook |

The first three rows are data. Only the fourth touches a notebook, and it is rare: the
33 rules in this sample use three shapes between them, and the 389 measures in MX's
existing report workbooks use six.

**There is never a notebook per provider.** `participant_id` is a column, not a code path.
Adding a fourth participant changes no notebook, no table definition, no rule, and no
report. Their files land under their own folder, the ingest step reads the participant
from the path, and every downstream table gains rows with a new value in one column. The
report's participant slicer gains an entry.

This is worth being emphatic about because it is the failure mode that makes these
pipelines unmaintainable. The moment there is a `gold_score_rules_arrmc` notebook, there
will eventually be nine of them, and a threshold change becomes nine edits.

### Why rules are rows rather than DAX measures

It is entirely possible to write quality rules as DAX. It would look reasonable at first:

```dax
ADT Name Compliance =
    DIVIDE(
        CALCULATE(COUNTROWS(hl7_message), NOT ISBLANK(hl7_message[patient_last_name])),
        COUNTROWS(hl7_message))
```

The problem is arithmetic. MX's report workbooks hold 389 measures. That approach needs 389
DAX measures, each one a near-identical copy, each one edited in Power BI Desktop, and each
threshold change requiring a model edit and a republish. Nobody outside the report author
can make a change, and no threshold has any history.

As rows, it is 33 rows in this sample and 389 once every existing measure is added, with the
same five DAX measures serving all of them. Moving a threshold is:

```sql
UPDATE mapping.quality_rule SET threshold = 0.95 WHERE rule_id = 'P4P-ADT-002'
```

This maps closely onto how the measures are already maintained. An MX report workbook
carries a row per measure with columns for the measure name, the segment and field it
reads, the target percentage, and whether an exception is allowed. `quality_rule` is that
same sheet, with the same one-row-per-measure grain:

| Workbook column | `quality_rule` column |
|---|---|
| Report / tab | `report_name` |
| Measure group heading | `measure_group` |
| Measure name | `data_element` |
| Segment and field | `field_path` |
| "or" alternatives in the formula | `rule_type` = `BIDIRECTIONAL`, `companion_field_paths` |
| "only when ..." in the formula | `rule_type` = `CONDITIONAL`, `condition_field_path` |
| Target % | `threshold` |
| Exception allowed | `exception_allowed` |

The macro's `COUNTIF` over a column becomes `is_satisfied` over the messages; its
`IF(denominator=0,"n/a",...)` becomes the `BLANK()` branch in `Compliance %`. The logic did
not move to a new language so much as split in two: the *per-message* half became data plus
one SQL query, and the *per-slice* half became five DAX measures.

### Where the seams are

Being honest about what is not data matters more than the sales pitch, because it tells
you which changes are cheap and which are not.

**Data — change these freely:**

- which participants exist
- which HL7 fields and CCDA elements are extracted
- which of those are scored, at what threshold, with what exception policy
- report names and measure groupings

**Code — a notebook edit, with review and a redeploy:**

- **The parsers.** `silver_parse_hl7` understands HL7 v2 encoding characters, repeats and
  escapes; `silver_parse_ccda` understands namespaced XPath. Adding a *field* is data.
  Adding a *format* — say, FHIR — is a new silver notebook.
- **The rule types.** Three shapes are implemented. A fourth is about ten lines; see
  [Add a new rule type](#add-a-new-rule-type).
- **The fact grain.** `fact_rule_result` is one row per message per rule. A measure that is
  not per-message — "were there at least 100 messages this month?" — does not fit and would
  need a second fact table.
- **The medallion shape itself.** Three lakehouses, the schemas within them, and the path
  convention `hl7/<participant>/<yyyy>/<mm>/` that ingest derives participant and period
  from.

The line, roughly: **the notebooks know how to read a format and how to test a shape;
everything about *which* fields and *which* measures is data.**

---

## Part 2 — How to

### Changing configuration

All four mapping tables are seeded from CSVs. The round trip is the same for every change
below:

1. Edit the CSV in `infra/data/samples_mx/config/`.
2. Get it into the lake — either re-run `Deploy-MxContent.ps1`, which uploads the whole
   `samples_mx` folder, or upload the single file to
   `mx_bronze/Files/samples_mx/config/` from the Fabric UI.
3. Run `mx_setup_pipeline`, or just its `seed_mapping_tables` notebook. The seed loads each
   CSV with `mode("overwrite")`, so the table is reconciled to the file rather than
   appended to. The CSV header *is* the table schema — there is no second copy to drift.
4. Run `mx_dq_pipeline`. It does not create schemas or seed mappings, so step 3 is what
   applies the change.

> You can also `UPDATE mapping.quality_rule ...` directly against the Delta table for a
> quick experiment. It works, and it is a good way to see the effect immediately — but the
> CSV is the source of truth and the next `seed_mapping_tables` run will overwrite your
> change. Put anything you want to keep in the CSV.

### Add a participant

No notebook changes. Three steps:

1. **Add a row** to `participant.csv`:

   ```
   participant_id,file_prefix,account_name,active
   NEWCO,NEWCO,New County Health,true
   ```

   `participant_id` is the value that appears in every downstream table and on the report
   slicer. `file_prefix` is the filename prefix their messages use.

2. **Create their folders** under the ingest root, following the existing convention:

   ```
   Files/samples_mx/hl7/NEWCO/2026/01/
   Files/samples_mx/ccda/NEWCO/2026/01/
   ```

   The path is load-bearing: `ingest_raw_files` reads participant, year and month from it.
   A file landed outside this shape gets `participant_id = 'UNKNOWN'` and null period
   columns, which is deliberate — it shows up rather than silently disappearing.

3. **Run `mx_dq_pipeline`.** Every table gains rows with `participant_id = 'NEWCO'`. The
   report needs no change; its slicer reads `dim_participant`, which is derived from the
   mapping table.

If the new participant sends message types nobody else does, you will also need mapping
rows for the fields in those messages — but that is the next section, not a special case
of this one.

### Add a field mapping

#### HL7 v2

Add a row to `hl7_field_map.csv`:

```
map_id,message_type,segment,field_path,data_element,active
MAP-ADT-050,ADT,PID,PID-16,Marital Status,true
```

- `map_id` — any unique string. The convention here is `MAP-<message type>-<nnn>`.
- `message_type` — must match what `MSH-9.1` carries (`ADT`, `ORU`, `MDM`, `VXU`, `RDE`).
- `segment` — the three-letter segment ID.
- `field_path` — `<segment>-<field>[.<component>]`, one-based, numbered exactly as the HL7
  standard numbers it. **Include the segment prefix**; `PID-16`, not `16`.
- `data_element` — the human label. This becomes the wide-view column name, slugified.

Re-seed, then re-run `silver_parse_hl7`. `hl7.field_value` gains a row per ADT message and
`hl7.vw_message_wide` gains a `marital_status` column.

> **Write MSH paths as the standard numbers them.** The field separator makes `MSH` count
> differently from every other segment; the parser corrects for that internally. `MSH-9.1`
> is the message type, as the standard says — no adjustment on your part.

#### CCDA

Add a row to `ccda_field_map.csv`:

```
map_id,section_name,field_path,data_element,xpath,value_attr,active
CCDA-014,Patient Role,patient_marital_status,Patient Marital Status,.//hl7:recordTarget/hl7:patientRole/hl7:patient/hl7:maritalStatusCode,code,true
```

- `field_path` — here this **is** the snake_case slug, and it is what a rule joins on.
- `xpath` — namespaced, `hl7:` prefixed, relative to the document root. Anything
  `ElementTree.findall` accepts.
- `value_attr` — leave **empty** to take the element's text; otherwise name the attribute
  (`extension`, `value`, `code`).

Re-seed, then re-run `silver_parse_ccda`.

> A bad XPath does not crash the run. `read_value` catches the syntax error and returns an
> empty string, so the field reads as unpopulated. If a new mapping comes back empty for
> every document, suspect the XPath before suspecting the data — test it against one file
> in a scratch notebook.

### Add a quality rule

Add a row to `quality_rule.csv`. The `field_path` must already exist in a field map, or
the rule can never be satisfied:

```
rule_id,report_name,message_type,measure_group,data_element,field_path,rule_type,companion_field_paths,condition_field_path,threshold,exception_allowed,active,notes
P4P-ADT-050,P4P Participant Report,ADT,Information,Marital Status,PID-16,THRESHOLD,,,0.8,false,true,
```

This scores the `PID-16` mapping added in the previous section.

Pick the shape that matches the measure:

| `rule_type` | Use when | Extra columns |
|---|---|---|
| `THRESHOLD` | one field must be populated | — |
| `BIDIRECTIONAL` | any one of several fields will do | `companion_field_paths`, `;`-separated |
| `CONDITIONAL` | only measured when a trigger field is present | `condition_field_path` |

Two of the sample rules in `quality_rule.csv`:

```
P4P-ADT-010,P4P Participant Report,ADT,Information,Race,PID-10,BIDIRECTIONAL,PID-22.1,,0.95,false,true,Race or Ethnicity satisfies the measure
P4P-ADT-033,P4P Participant Report,ADT,Admission,Patient Type,PV1-18,CONDITIONAL,,PV1-2,0.9,false,true,Scored only on messages that carry a Patient Class
```

`exception_allowed` controls the failure wording, not the arithmetic: below threshold with
`exception_allowed = true` reports **Warning**, otherwise **Fail**.

Re-seed, then re-run `gold_score_rules`. `seed_mapping_tables` runs a referential check
that lists any rule whose `field_path` is in no field map — a non-empty result there is the
cheapest place to catch a typo.

### Change a threshold

Edit the `threshold` column:

```
P4P-ADT-002,...,PID-5.1,THRESHOLD,,,0.95,false,true,
```

Re-seed. **Nothing downstream needs to re-run.** The threshold is not used in scoring; it
lives in `dim_rule` and is read by the `Rule Threshold` DAX measure at query time. Refresh
the report and the Pass/Fail bands move.

That separation is why `gold_score_rules` stores no percentages. A threshold is a
*reporting* judgement about what good looks like; whether one message carried a patient
name is a *fact*. Mixing them would mean re-scoring every message every time somebody
changed their mind about a target.

To retire a measure, set `active` to `false` rather than deleting the row — the row stays
as a record of what was once measured.

### Add a new rule type

This is the one change that touches a notebook, and it is deliberately small. Two places in
`gold_score_rules` know about rule shapes.

**Step 4** expands each rule into the field paths that would satisfy it. `THRESHOLD`
produces one row; `BIDIRECTIONAL` adds one per companion. `silver_quality_rule` is
`mapping.quality_rule`, read in Step 1:

```sql
CREATE OR REPLACE TEMP VIEW satisfying_field AS
SELECT rule_id, message_type, field_path AS satisfying_field_path
FROM silver_quality_rule
WHERE active = true

UNION

SELECT r.rule_id, r.message_type, TRIM(companion) AS satisfying_field_path
FROM silver_quality_rule r
LATERAL VIEW explode(split(r.companion_field_paths, ';')) c AS companion
WHERE r.active = true
  AND r.rule_type = 'BIDIRECTIONAL'
  AND TRIM(companion) <> ''
```

**Step 5** decides applicability and satisfaction. `MAX(...)` over the expanded paths is
the "any of these" test:

```sql
CASE
    WHEN c.rule_type <> 'CONDITIONAL' THEN true
    WHEN trigger.message_uid IS NOT NULL THEN true
    ELSE false
END AS is_applicable
```

```sql
MAX(CASE WHEN hit.message_uid IS NOT NULL THEN 1 ELSE 0 END) AS any_populated
```

**Worked example — `ALL_OF`**, a rule satisfied only when *every* listed field is populated.

1. Widen the Step 4 expansion to include the new type, so its companions expand too:

   ```sql
   AND r.rule_type IN ('BIDIRECTIONAL', 'ALL_OF')
   ```

2. Change the Step 5 aggregation from "any" to "all" for that type. `MIN` is the `AND` of
   the same expression `MAX` uses as `OR`:

   ```sql
   CASE WHEN MAX(c.rule_type) = 'ALL_OF'
        THEN MIN(CASE WHEN hit.message_uid IS NOT NULL THEN 1 ELSE 0 END)
        ELSE MAX(CASE WHEN hit.message_uid IS NOT NULL THEN 1 ELSE 0 END)
   END AS any_populated
   ```

3. Add the new type to the rule-type table in the notebook's markdown, so the next reader
   finds it documented where they expect.

Then it is data again: write `ALL_OF` in `rule_type`, list the fields in
`companion_field_paths`, re-seed, re-run. **No change is needed in gold's table
definitions, in the semantic model, or in the report** — the fact schema and the DAX
measures do not know or care how `is_satisfied` was decided.

The three shapes MX's workbooks use that this sample does not — `ANY_OF`,
`TRIDIRECTIONAL`, `KEYWORD` — follow the same pattern. `ANY_OF` and `TRIDIRECTIONAL` are variations on the expansion already
implemented; `KEYWORD` needs a value test rather than a presence test, which means
`populated_field` gains a value comparison.

### Landing files

Ingest reads from `mx_bronze/Files/samples_mx/`, and the folder layout is load-bearing:

```
Files/samples_mx/
├── hl7/<PARTICIPANT>/<yyyy>/<mm>/*.hl7
├── ccda/<PARTICIPANT>/<yyyy>/<mm>/*.xml
└── config/*.csv
```

The top-level folder decides how a file is parsed: `.hl7` under `hl7/` becomes `HL7V2`,
`.xml` under `ccda/` becomes `CCDA`. **An `.xml` misfiled under `hl7/` is silently never
ingested** — if a file does not appear in `bronze.raw_message`, check its folder first.

**From Amazon S3 (the production path).** Create a OneLake shortcut from
`mx_bronze/Files/` to the S3 bucket. Files stay in S3; Fabric reads them in place, so
there is no copy step and no sync to keep honest. The bucket needs to be laid out in the
same `hl7/<participant>/<yyyy>/<mm>/` shape. Step-by-step instructions are in
[Deploy.md](Deploy.md#connecting-an-amazon-s3-bucket).

**Manual upload (for testing).** In the Fabric portal, open `mx_bronze` → **Files** →
**Upload**, and drop files into the matching folder. Useful for trying a single real
message against the pipeline without touching S3.

Either way, run `mx_dq_pipeline`. It overwrites the raw, silver and gold POC
snapshots; ingestion audit rows append. Keep historical source files under
`SOURCE_ROOT`, and do not run the setup and processing pipelines concurrently.

---

## Checking your work

After any change, three things are worth looking at before opening the report.

**The referential check in `seed_mapping_tables`** lists rules whose `field_path` matches
no field map row. It should return nothing. A row here means a rule that can never be
satisfied, which shows up in the report as an unexplained 0%.

**The wide views** — `hl7.vw_message_wide` and `ccda.vw_document_wide` — show a new field
in context. If the column is there but empty for every message, the mapping row is wrong
(or the data genuinely lacks the field); if the column is missing, the seed did not run.

**`dq.vw_rule_results`** is a SQL projection of the same numbers the report shows, at
participant × month × rule grain, with numerator, denominator, rate, threshold and status.
It exists so a result can be checked without opening Power BI, and so a number can be
traced back to the messages behind it.

> Read the grain before comparing. To roll `vw_rule_results` up to a participant total,
> re-aggregate the numerator and denominator — never average the percentages. Averaging
> rates across rules with different denominators gives a plausible-looking wrong answer.

## Report measures and dimensions

All fourteen measures live on `fact_rule_result`, in the **Measures** display folder. None
of them names a specific rule, so adding a rule never needs a new measure.

| Measure | Answers |
|---|---|
| `Numerator` | how many messages satisfied the rule |
| `Denominator` | how many messages the rule applied to |
| `Compliance %` | numerator ÷ denominator, blank when the denominator is 0 |
| `Rule Threshold` | the threshold the selected rule must clear |
| `Status` | `Pass`, `Fail`, `Warning`, `N/A` or `Mixed` |
| `Status Color` | the colour `Status` is shown in — drives conditional formatting |
| `Rules Passing` / `Rules Warning` / `Rules Failing` / `Rules N/A` | rule counts by status |
| `Rules Scored` | passing + warning + failing: the rules that had something in scope |
| `Pass Rate` | passing ÷ scored |
| `Rule Count` | rules in context, scored or not |
| `Failed Messages` | messages a rule applied to that did not satisfy it |

`Status` reads `Mixed` when a visual is not sliced down to one rule — rules have different
thresholds, so there is no single answer. Put `rule_id` or `data_element` on the rows and
it resolves. `N/A` means the denominator was zero; see the note under
[DAX turns rows into a percentage](#6-dax-turns-rows-into-a-percentage).

Do not conditionally format `Compliance %` against a fixed number. Thresholds differ per
rule, which is why `Rule Threshold`, `Status` and `Status Color` are measures — the
comparison is already made per row.

Slice by the three dimensions:

| Table | Useful columns |
|---|---|
| `dim_participant` | `account_name`, `participant_id` |
| `dim_period` | `period_label` (sorts correctly), `year`, `month_name` |
| `dim_rule` | `measure_group`, `data_element`, `field_path`, `report_name`, `message_type`, `rule_type`, `threshold`, `exception_allowed` |

`fact_rule_result` also exposes `message_uid`, `is_applicable` and `is_satisfied`. Its key
columns are hidden on purpose — slice on the dimensions.

## Related reading

- [README.md](../README.md) — what the solution is and how it is laid out
- [Deploy.md](Deploy.md) — deploying it and connecting S3
- [Deploy.md — The report](Deploy.md#the-report) — what the four report pages show, and
  how to change them in Power BI Desktop
