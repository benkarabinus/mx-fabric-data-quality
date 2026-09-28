# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a2b3c4d-0003-4000-8000-000000000003",
# META       "default_lakehouse_name": "mx_gold",
# META       "default_lakehouse_workspace_id": "1a2b3c4d-0000-4000-8000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a2b3c4d-0002-4000-8000-000000000002"
# META         },
# META         {
# META           "id": "1a2b3c4d-0003-4000-8000-000000000003"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## gold_score_rules
#
# Answers one question, once per message, for every rule that applies to it:
#
# > **Did this message satisfy this rule?**
#
# That is all. It does not calculate a percentage, and it does not compare anything to a
# threshold. Those are rates over a *slice* of messages, and the slice is not known until
# somebody picks a participant and a month in the report. Power BI does that part with the
# five DAX measures described in the Extend guide.
#
# The split matters:
#
# | Question | Answered by | Why there |
# |---|---|---|
# | Did *this message* satisfy the rule? | this notebook | depends only on the message |
# | What is the rate for *this slice*? | DAX | depends on what the user filtered to |
#
# ### There is no per-participant and no per-rule code here
#
# Participant is a column value. A rule is a row in `mapping.quality_rule`. Adding a
# participant or a rule changes data, not this notebook. The only thing that would change
# this notebook is a genuinely new **rule shape**, and there are three:
#
# | `rule_type` | Applicable when | Satisfied when |
# |---|---|---|
# | `THRESHOLD` | always | `field_path` is populated |
# | `BIDIRECTIONAL` | always | `field_path` **or any** of `companion_field_paths` is populated |
# | `CONDITIONAL` | `condition_field_path` is populated | `field_path` is populated |
#
# `is_applicable` is the denominator flag and `is_satisfied` is the numerator flag. A
# message that is not applicable contributes to neither.

# MARKDOWN ********************

# ### Step 1 — read silver
#
# Everything this notebook needs already exists: the parsed messages, the extracted field
# values, and the rule definitions. Note the three-part names. The default lakehouse of
# this notebook is `mx_gold`, so `hl7.message` on its own would not resolve.
#
# The mapping DataFrames are registered as session temporary views for the SQL below.
# This keeps cross-lakehouse resolution in the initial reads instead of mixing those
# identifiers with temporary views inside SQL queries.

# CELL ********************

from pyspark.sql import functions as F

rules = (
    spark.table("mx_silver.mapping.quality_rule")
    .where(F.col("active") == True)  # noqa: E712 - Spark column comparison
)
participants = (
    spark.table("mx_silver.mapping.participant")
    .where(F.col("active") == True)  # noqa: E712
)

rules.createOrReplaceTempView("silver_quality_rule")
participants.createOrReplaceTempView("silver_participant")

hl7_message = spark.table("mx_silver.hl7.message")
hl7_field_value = spark.table("mx_silver.hl7.field_value")
ccda_document = spark.table("mx_silver.ccda.document")
ccda_field_value = spark.table("mx_silver.ccda.field_value")

print(f"active rules          {rules.count():>6}")
print(f"active participants   {participants.count():>6}")
print(f"hl7 messages          {hl7_message.count():>6}")
print(f"hl7 field values      {hl7_field_value.count():>6}")
print(f"ccda documents        {ccda_document.count():>6}")
print(f"ccda field values     {ccda_field_value.count():>6}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 2 — put HL7 and CCDA on one spine
#
# An HL7 message and a CCDA document are different documents, but for scoring they are the
# same shape: *a thing that arrived from a participant in a month, carrying some fields.*
# So we union them into one spine and score once.
#
# The only sleight of hand is `message_type`. HL7 messages carry a real one (`ADT`, `ORU`,
# ...). CCDA documents get the literal `'CCDA'`, which is exactly what the CCDA rows in
# `mapping.quality_rule` name in their own `message_type` column. That single convention is
# what lets one join serve both formats.
#
# Two filters are applied:
#
# - `parse_status = 'OK'` — a message we could not parse cannot be fairly scored.
# - a known period — a message whose path did not yield a year and month cannot be placed
#   in a reporting period. The count is printed so the gap is visible rather than silent.

# CELL ********************

MESSAGE_COLUMNS = [
    "message_uid",
    "participant_id",
    "message_type",
    "period_year",
    "period_month",
    "parse_status",
]

hl7_spine = hl7_message.select(*MESSAGE_COLUMNS)

ccda_spine = ccda_document.select(
    F.col("document_uid").alias("message_uid"),
    F.col("participant_id"),
    F.lit("CCDA").alias("message_type"),
    F.col("period_year"),
    F.col("period_month"),
    F.col("parse_status"),
)

all_messages = hl7_spine.unionByName(ccda_spine)

scored_message = (
    all_messages.where(F.col("parse_status") == "OK")
    .where(F.col("period_year").isNotNull() & F.col("period_month").isNotNull())
    .join(participants.select("participant_id"), on="participant_id", how="inner")
    .select(
        "message_uid",
        "participant_id",
        "message_type",
        (F.col("period_year") * 100 + F.col("period_month")).cast("int").alias("period_key"),
    )
)
scored_message.createOrReplaceTempView("scored_message")

skipped = all_messages.count() - scored_message.count()
print(f"messages on the spine   {all_messages.count():>6}")
print(f"scored                  {scored_message.count():>6}")
print(f"skipped                 {skipped:>6}   (unparsed, unknown period, or unknown participant)")

display(scored_message.groupBy("message_type").count().orderBy("message_type"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 3 — one flat set of "this field was populated" facts
#
# The two `field_value` tables have the same columns, so they union directly. We reduce them
# to `DISTINCT (message_uid, field_path)` over populated values only, which turns the
# question *"is this field populated on this message?"* into a plain lookup with no risk of
# fanning out the join — a field that was mapped twice, or a repeating segment that produced
# several values, still yields exactly one row here.

# CELL ********************

populated_field = (
    hl7_field_value.select("message_uid", "field_path", "is_populated")
    .unionByName(
        ccda_field_value.select(
            F.col("document_uid").alias("message_uid"), "field_path", "is_populated"
        )
    )
    .where(F.col("is_populated") == True)  # noqa: E712
    .select("message_uid", "field_path")
    .distinct()
)
populated_field.createOrReplaceTempView("populated_field")

print(f"populated (message, field) pairs   {populated_field.count():>6}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 4 — turn the rule rows into something a join can use
#
# A rule names its fields in two text columns: `field_path`, and a `;`-separated
# `companion_field_paths`. That is convenient to edit in a CSV but awkward to join against,
# so we expand it into one row per *satisfying* field path.
#
# ```
# P4P-ADT-010  field_path PID-10  companions PID-22.1
#
#   becomes    P4P-ADT-010 -> PID-10
#              P4P-ADT-010 -> PID-22.1
# ```
#
# A `THRESHOLD` rule produces a single row, so it needs no special case — it is a
# `BIDIRECTIONAL` rule with an empty companion list. Only `BIDIRECTIONAL` rules contribute
# extra rows, and `explode` on the split does the work.
#
# **This is the seam to widen for a new rule shape.** A rule type satisfied when *every*
# listed field is populated, for example, would keep this expansion and change the
# aggregation in Step 5 from `MAX` to `MIN`.

# CELL ********************

spark.sql(
    """
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
    """
)

expansion = spark.sql(
    """
    SELECT r.rule_id, r.rule_type, COUNT(*) AS satisfying_fields
    FROM silver_quality_rule r
    JOIN satisfying_field s ON s.rule_id = r.rule_id
    WHERE r.active = true
    GROUP BY r.rule_id, r.rule_type
    HAVING COUNT(*) > 1
    ORDER BY r.rule_id
    """
)
print(f"satisfying (rule, field) pairs   {spark.table('satisfying_field').count():>6}")
print("rules satisfied by more than one field:")
display(expansion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 5 — score every message against every rule that applies to it
#
# One query, read top to bottom:
#
# 1. **`candidate`** pairs each message with the rules declared for its `message_type`. A
#    CCDA document meets the eight `USCDI-*` rules; an `ADT` message meets the fifteen
#    `P4P-ADT-*` rules. Nothing pairs a rule with a message type it was not written for.
# 2. **`applicable`** decides the denominator. Everything is applicable except a
#    `CONDITIONAL` rule whose trigger field is absent — `P4P-ADT-033` scores Patient Type
#    only on messages that actually carry a Patient Class, so messages without one drop out
#    of the denominator rather than counting as failures.
# 3. **`satisfied`** decides the numerator. `MAX(...)` over the satisfying field paths is
#    the "any of these" test: one populated field is enough.
#
# The `LEFT JOIN` in each step is deliberate. An inner join would silently discard messages
# where the field is missing — which is precisely the population we are trying to measure.
#
# Note what is **not** here: no rule IDs, no field paths, no participant names, no
# thresholds. A threshold is a reporting attribute; it has no bearing on whether one message
# satisfied one rule.

# CELL ********************

fact = spark.sql(
    """
    WITH candidate AS (
        SELECT
            m.message_uid,
            m.participant_id,
            m.period_key,
            r.rule_id,
            r.rule_type,
            r.condition_field_path
        FROM scored_message m
        JOIN silver_quality_rule r
          ON r.message_type = m.message_type
         AND r.active = true
    ),
    applicable AS (
        SELECT
            c.message_uid,
            c.participant_id,
            c.period_key,
            c.rule_id,
            CASE
                WHEN c.rule_type <> 'CONDITIONAL' THEN true
                WHEN trigger.message_uid IS NOT NULL THEN true
                ELSE false
            END AS is_applicable
        FROM candidate c
        LEFT JOIN populated_field trigger
               ON trigger.message_uid = c.message_uid
              AND trigger.field_path  = c.condition_field_path
    ),
    satisfied AS (
        SELECT
            c.message_uid,
            c.rule_id,
            MAX(CASE WHEN hit.message_uid IS NOT NULL THEN 1 ELSE 0 END) AS any_populated
        FROM candidate c
        JOIN satisfying_field s
          ON s.rule_id = c.rule_id
        LEFT JOIN populated_field hit
               ON hit.message_uid = c.message_uid
              AND hit.field_path  = s.satisfying_field_path
        GROUP BY c.message_uid, c.rule_id
    )
    SELECT
        a.message_uid,
        a.participant_id,
        a.period_key,
        a.rule_id,
        a.is_applicable,
        a.is_applicable AND COALESCE(s.any_populated, 0) = 1 AS is_satisfied
    FROM applicable a
    LEFT JOIN satisfied s
           ON s.message_uid = a.message_uid
          AND s.rule_id     = a.rule_id
    """
)
fact.createOrReplaceTempView("fact_rule_result_new")

print(f"fact rows          {fact.count():>6}")
print(f"  applicable       {fact.where(F.col('is_applicable')).count():>6}")
print(f"  satisfied        {fact.where(F.col('is_satisfied')).count():>6}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 6 — the dimensions
#
# Three small tables that give the fact something to be sliced by. All three are derived,
# not authored: participants come from the mapping table, periods come from the months that
# actually arrived, and rules come from the rule catalog.
#
# `dim_rule` deliberately carries only the **reporting** attributes. `companion_field_paths`,
# `condition_field_path`, `active` and `notes` are scoring inputs that were consumed above —
# putting them in the semantic model would invite someone to build a visual on them and
# conclude something untrue.
#
# `period_key` is a plain integer `yyyymm`, so `202601` sorts correctly without a date
# hierarchy and joins to the fact without any date parsing in the report.

# CELL ********************

spark.sql(
    """
    INSERT OVERWRITE TABLE dq.dim_participant
    SELECT participant_id, account_name, file_prefix
    FROM silver_participant
    WHERE active = true
    """
)

spark.sql(
    """
    INSERT OVERWRITE TABLE dq.dim_period
    SELECT
        period_key,
        period_start,
        YEAR(period_start)                       AS year,
        MONTH(period_start)                      AS month,
        DATE_FORMAT(period_start, 'MMMM')        AS month_name,
        DATE_FORMAT(period_start, 'MMM yyyy')    AS period_label
    FROM (
        SELECT DISTINCT
            period_key,
            TO_DATE(
                CONCAT(
                    CAST(period_key DIV 100 AS STRING), '-',
                    LPAD(CAST(period_key % 100 AS STRING), 2, '0'), '-01'
                )
            ) AS period_start
        FROM fact_rule_result_new
    )
    """
)

spark.sql(
    """
    INSERT OVERWRITE TABLE dq.dim_rule
    SELECT
        rule_id,
        report_name,
        message_type,
        measure_group,
        data_element,
        field_path,
        rule_type,
        CAST(threshold AS DOUBLE)        AS threshold,
        CAST(exception_allowed AS BOOLEAN) AS exception_allowed
    FROM silver_quality_rule
    WHERE active = true
    """
)

spark.sql(
    """
    INSERT OVERWRITE TABLE dq.fact_rule_result
    SELECT message_uid, participant_id, period_key, rule_id, is_applicable, is_satisfied
    FROM fact_rule_result_new
    """
)

for table in ("dim_participant", "dim_period", "dim_rule", "fact_rule_result"):
    print(f"dq.{table:<20} {spark.table(f'dq.{table}').count():>6} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 7 — check the result
#
# `dq.vw_rule_results` was created by `schema_model_gold` and does in SQL what the DAX
# measures do in the report. It exists so the two can be compared: if a card in Power BI
# disagrees with this view, one of them is wrong and it is worth knowing which before the
# number reaches a participant.
#
# **Read `N/A` carefully.** It means *no applicable messages*, not *zero compliance*.
# Reporting 0% to a participant who sent nothing of that kind would be a false accusation.
# In this small sample several combinations are legitimately `N/A` — BVCH sent no VXU or RDE
# messages, PCMC sent no MDM — and they should stay `N/A` rather than being filled in.

# CELL ********************

display(
    spark.sql(
        """
        SELECT account_name, period_label, report_name, measure_group, data_element,
               field_path, numerator, denominator,
               ROUND(compliance_rate * 100, 1) AS compliance_pct,
               ROUND(threshold * 100, 1)       AS threshold_pct,
               status
        FROM dq.vw_rule_results
        ORDER BY account_name, report_name, data_element
        """
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql(
        """
        SELECT status, COUNT(*) AS rule_slices
        FROM dq.vw_rule_results
        GROUP BY status
        ORDER BY status
        """
    )
)

unscored = spark.sql(
    """
    SELECT r.rule_id, r.message_type, r.data_element
    FROM silver_quality_rule r
    LEFT JOIN dq.fact_rule_result f ON f.rule_id = r.rule_id
    WHERE r.active = true AND f.rule_id IS NULL
    ORDER BY r.rule_id
    """
)
count = unscored.count()
if count:
    print(f"{count} active rule(s) produced no fact rows - no message of that type arrived:")
    display(unscored)
else:
    print("Every active rule was scored against at least one message.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### What to change, and where
#
# | You want to | Change |
# |---|---|
# | Score a new field | add a row to `mapping.hl7_field_map` / `mapping.ccda_field_map`, then a row to `mapping.quality_rule` |
# | Move a threshold | `UPDATE mapping.quality_rule SET threshold = ... WHERE rule_id = ...` |
# | Retire a measure | `UPDATE mapping.quality_rule SET active = false WHERE rule_id = ...` |
# | Add a participant | a folder on the ingest path and a row in `mapping.participant` |
# | Add a **new rule shape** | extend Step 4 and Step 5 — the only case that touches this notebook |
#
# None of those, except the last, require editing a notebook, and none of them require a
# notebook per participant.
