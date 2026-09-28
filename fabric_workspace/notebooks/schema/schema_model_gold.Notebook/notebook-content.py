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
# META           "id": "1a2b3c4d-0001-4000-8000-000000000001"
# META         },
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

# # schema_model_gold
#
# Creates the reporting star in `mx_gold`.
#
# One fact, three dimensions, one cross-check view.
#
# The fact is **narrow on purpose**. It records a single yes/no answer per message
# per rule:
#
# | Column | Question it answers |
# |---|---|
# | `is_applicable` | Should this message be counted in this rule's denominator? |
# | `is_satisfied` | Did this message satisfy the rule? |
#
# **No percentage is stored anywhere in gold.** Percentages are not additive - a
# stored rate for ADT and a stored rate for ORU cannot be averaged to get the rate
# for both. Storing the numerator and the denominator and dividing at query time is
# what lets the report roll up by participant, by month, by measure group or by all
# messages at once and still be arithmetically correct.
#
# Thresholds live on `dim_rule` rather than in DAX, so changing a threshold is an
# update to a row in `mapping.quality_rule` and a reprocess - not a model edit.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS dq")
print("schema ready: dq")

# MARKDOWN ********************

# ## Dimensions

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS dq.dim_participant (
    participant_id STRING COMMENT 'Key, e.g. ARRMC',
    account_name   STRING COMMENT 'Display name used on the report',
    file_prefix    STRING COMMENT 'Prefix the organization names its files with'
) USING DELTA
""")

# Month grain, not day: MX reports compliance by reporting month, and every message
# is assigned to a month by the folder it arrived in. Calling this dim_period rather
# than dim_date avoids implying a daily grain that does not exist.
spark.sql("""
CREATE TABLE IF NOT EXISTS dq.dim_period (
    period_key   INT    COMMENT 'yyyymm, e.g. 202601',
    period_start DATE   COMMENT 'First day of the reporting month, for date slicers',
    year         INT,
    month        INT,
    month_name   STRING,
    period_label STRING COMMENT 'e.g. Jan 2026'
) USING DELTA
""")

# Materialised from mapping.quality_rule rather than read across lakehouses, so a
# Direct Lake semantic model finds every table it needs in one place.
spark.sql("""
CREATE TABLE IF NOT EXISTS dq.dim_rule (
    rule_id           STRING  COMMENT 'Key, e.g. P4P-ADT-005',
    report_name       STRING  COMMENT 'Which MX report this measure belongs to',
    message_type      STRING  COMMENT 'ADT, ORU, MDM, VXU, RDE, CCDA',
    measure_group     STRING  COMMENT 'Section heading on the report',
    data_element      STRING  COMMENT 'Business name of the measure',
    field_path        STRING  COMMENT 'Which field is measured, e.g. PID-11',
    rule_type         STRING  COMMENT 'THRESHOLD, BIDIRECTIONAL or CONDITIONAL',
    threshold         DOUBLE  COMMENT 'Required rate as a fraction: 1.0, 0.9, 0.25',
    exception_allowed BOOLEAN COMMENT 'TRUE where MX has granted an exception - a shortfall reports as Warning, not Fail'
) USING DELTA
""")

for t in ["dq.dim_participant", "dq.dim_period", "dq.dim_rule"]:
    print(f"ready: {t}")

# MARKDOWN ********************

# ## The fact

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS dq.fact_rule_result (
    message_uid    STRING  COMMENT 'The message scored - drill through to silver for the raw text',
    participant_id STRING  COMMENT 'Joins to dq.dim_participant',
    period_key     INT     COMMENT 'Joins to dq.dim_period',
    rule_id        STRING  COMMENT 'Joins to dq.dim_rule',
    is_applicable  BOOLEAN COMMENT 'In the denominator. FALSE when a CONDITIONAL rule did not trigger',
    is_satisfied   BOOLEAN COMMENT 'In the numerator'
) USING DELTA
""")

print("ready: dq.fact_rule_result")

# MARKDOWN ********************

# ## Cross-check view
#
# The report is the product; this view is the receipt.
#
# It answers the same question at one fixed grain - participant by month by rule -
# with the same six columns MX's Excel macro produces, so a result can be reconciled
# without opening Power BI, and so anyone who would rather work in Excel can export
# it directly from the SQL analytics endpoint.
#
# **Read the grain before comparing.** This view is fixed at participant x month x
# rule. The report can roll up further - all months, all participants, a whole
# measure group - and will legitimately show a different number. Re-aggregate the
# numerator and the denominator; never average the percentages.

# CELL ********************

spark.sql("""
CREATE OR REPLACE VIEW dq.vw_rule_results AS
SELECT
    p.account_name,
    d.period_label,
    r.report_name,
    r.measure_group,
    r.data_element,
    r.field_path,
    SUM(CASE WHEN f.is_satisfied  THEN 1 ELSE 0 END) AS numerator,
    SUM(CASE WHEN f.is_applicable THEN 1 ELSE 0 END) AS denominator,
    CASE
        WHEN SUM(CASE WHEN f.is_applicable THEN 1 ELSE 0 END) = 0 THEN NULL
        ELSE SUM(CASE WHEN f.is_satisfied THEN 1 ELSE 0 END)
             / SUM(CASE WHEN f.is_applicable THEN 1 ELSE 0 END)
    END AS compliance_rate,
    r.threshold,
    CASE
        WHEN SUM(CASE WHEN f.is_applicable THEN 1 ELSE 0 END) = 0 THEN 'N/A'
        WHEN SUM(CASE WHEN f.is_satisfied THEN 1 ELSE 0 END)
             / SUM(CASE WHEN f.is_applicable THEN 1 ELSE 0 END) >= r.threshold THEN 'Pass'
        WHEN r.exception_allowed THEN 'Warning'
        ELSE 'Fail'
    END AS status,
    f.participant_id,
    f.period_key,
    f.rule_id
FROM dq.fact_rule_result f
JOIN dq.dim_rule        r ON r.rule_id        = f.rule_id
JOIN dq.dim_participant p ON p.participant_id = f.participant_id
JOIN dq.dim_period      d ON d.period_key     = f.period_key
GROUP BY p.account_name, d.period_label, r.report_name, r.measure_group,
         r.data_element, r.field_path, r.threshold, r.exception_allowed,
         f.participant_id, f.period_key, f.rule_id
""")

print("ready: dq.vw_rule_results")

# MARKDOWN ********************

# ## Why an empty denominator is N/A and never 0%
#
# If a participant sent no MDM messages in a month, they did not fail the MDM
# measures - there was nothing to measure. Reporting 0% would tell them they have a
# problem they do not have.
#
# Both the view above and the `Status` measure in the semantic model return `N/A`
# when the denominator is zero. This mirrors the `IF(denominator=0,"n/a",...)` guard
# in MX's own workbooks, and it is the one piece of report logic worth stating twice.

# CELL ********************

spark.sql("SHOW TABLES IN dq").show(truncate=False)
