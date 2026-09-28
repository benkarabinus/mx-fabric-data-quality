# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a2b3c4d-0001-4000-8000-000000000001",
# META       "default_lakehouse_name": "mx_bronze",
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

# # schema_model_bronze
#
# Creates the bronze retention tables in `mx_bronze`.
#
# Bronze holds one row per ingested file with the complete original payload, plus
# an ingestion audit trail. Nothing is transformed here - this layer exists so
# that every downstream metric can be traced back to exactly what was received.
#
# The only thing the ingest notebook interprets is the **folder path**. Files arrive
# as `hl7/<participant>/<yyyy>/<mm>/<file>`, so participant and reporting period are
# read from the path rather than from the message body - which means a file that
# fails to parse is still attributed correctly and still shows up as a gap.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.raw_message (
    message_uid      STRING  COMMENT 'Stable identifier, derived from the source path',
    participant_id   STRING  COMMENT 'Submitting organization',
    message_format   STRING  COMMENT 'HL7V2 or CCDA',
    period_year      INT     COMMENT 'Reporting period, derived from the folder the file arrived in',
    period_month     INT,
    source_path      STRING  COMMENT 'Full OneLake or S3 path the file was read from',
    file_name        STRING,
    file_bytes       BIGINT,
    raw_payload      STRING  COMMENT 'Complete original message, retained for audit and drill-through',
    ingest_run_id    STRING,
    ingest_ts        TIMESTAMP,
    ingest_status    STRING  COMMENT 'INGESTED or FAILED'
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.ingest_run (
    ingest_run_id    STRING,
    started_at       TIMESTAMP,
    ended_at         TIMESTAMP,
    source_root      STRING,
    files_seen       BIGINT,
    files_ingested   BIGINT,
    files_failed     BIGINT,
    status           STRING
) USING DELTA
""")

for t in ["bronze.raw_message", "bronze.ingest_run"]:
    print(f"ready: {t}")
