# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a2b3c4d-0002-4000-8000-000000000002",
# META       "default_lakehouse_name": "mx_silver",
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

# # schema_model_silver
#
# Creates the schemas and parsed-content tables in `mx_silver`.
#
# | Schema | Purpose |
# |---|---|
# | `mapping` | Control plane - which fields are extracted, and which are scored |
# | `hl7` | Parsed HL7 v2 content |
# | `ccda` | Parsed CCDA content |
#
# The `mapping.*` tables are **not** created here. `seed_mapping_tables` writes them
# directly from the seed CSVs, so the CSV header is the single definition of those
# tables and there is no second copy of the schema to drift from it.
#
# Both content schemas use the same shape:
#
# - a **message table** - one row per file, carrying the complete original payload
#   and the bronze file path, so any number on the report can be traced back to the
#   text that produced it;
# - a **field_value table** - one row per extracted field per message, each row
#   tagged with the `map_id` of the mapping row that produced it.
#
# One row per field rather than one column per field is what makes the extraction
# metadata-driven: adding a field is a row in the mapping table, not a schema change
# here. `nb_silver_hl7` and `nb_silver_ccda` build a column-per-field **view** on top
# by pivoting the mapping, for anyone who would rather query it that way.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS mapping")
spark.sql("CREATE SCHEMA IF NOT EXISTS hl7")
spark.sql("CREATE SCHEMA IF NOT EXISTS ccda")

print("schemas ready: mapping, hl7, ccda")

# MARKDOWN ********************

# ## HL7 v2 content

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS hl7.message (
    message_uid      STRING    COMMENT 'Stable identifier, carried through from bronze',
    participant_id   STRING    COMMENT 'Submitting organization',
    message_type     STRING    COMMENT 'ADT, ORU, MDM, VXU, RDE - from MSH-9.1',
    message_datetime TIMESTAMP COMMENT 'MSH-7, the time the sender stamped the message',
    period_year      INT       COMMENT 'Reporting period, derived from the source path',
    period_month     INT,
    bronze_file_path STRING    COMMENT 'Where the original file lives',
    raw_payload      STRING    COMMENT 'Complete original message text',
    parse_status     STRING    COMMENT 'OK, EMPTY, NO_MSH or NO_MESSAGE_TYPE'
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS hl7.field_value (
    message_uid  STRING  COMMENT 'Joins to hl7.message',
    map_id       STRING  COMMENT 'The mapping.hl7_field_map row that produced this value',
    field_path   STRING  COMMENT 'Denormalised from the mapping row for readability, e.g. PID-5.1',
    data_element STRING  COMMENT 'Denormalised business name, e.g. Patient Last Name',
    value        STRING  COMMENT 'Extracted value, NULL when the field was absent or empty',
    is_populated BOOLEAN COMMENT 'The single fact every completeness rule is scored from'
) USING DELTA
""")

for t in ["hl7.message", "hl7.field_value"]:
    print(f"ready: {t}")

# MARKDOWN ********************

# ## CCDA content
#
# Deliberately the same shape as HL7, so the silver notebooks and the scoring
# notebook differ only in how a value is located, never in what they produce.

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ccda.document (
    document_uid     STRING    COMMENT 'Stable identifier, carried through from bronze',
    participant_id   STRING,
    document_type    STRING    COMMENT 'ClinicalDocument/code@displayName',
    effective_time   TIMESTAMP COMMENT 'ClinicalDocument/effectiveTime',
    period_year      INT       COMMENT 'Reporting period, derived from the source path',
    period_month     INT,
    bronze_file_path STRING,
    raw_payload      STRING    COMMENT 'Complete original XML',
    parse_status     STRING    COMMENT 'OK, BAD_XML or NO_DOCUMENT_TYPE'
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ccda.field_value (
    document_uid STRING  COMMENT 'Joins to ccda.document',
    map_id       STRING  COMMENT 'The mapping.ccda_field_map row that produced this value',
    field_path   STRING  COMMENT 'Denormalised slug, e.g. patient_last_name',
    data_element STRING,
    value        STRING,
    is_populated BOOLEAN
) USING DELTA
""")

for t in ["ccda.document", "ccda.field_value"]:
    print(f"ready: {t}")

# MARKDOWN ********************

# ## What is deliberately absent
#
# Earlier versions of this solution also stored every HL7 segment, every OBX
# observation, every next-of-kin repeat and every CCDA section and entry. None of
# that is here, because `raw_payload` already holds the complete original message.
# Nothing has been lost - it just is not pre-shredded into tables nobody queried.
#
# If a future measure needs a field that is not extracted today, the change is a row
# in `mapping.hl7_field_map` or `mapping.ccda_field_map` and a reprocess.

# CELL ********************

spark.sql("SHOW TABLES IN hl7").show(truncate=False)
spark.sql("SHOW TABLES IN ccda").show(truncate=False)
