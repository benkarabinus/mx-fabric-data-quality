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

# # seed_mapping_tables
#
# Loads the four configuration tables from `mx_bronze/Files/samples_mx/config` into
# `mapping.*` in `mx_silver`.
#
# **These four CSVs are the control surface of the entire solution.**
#
# | Table | Answers |
# |---|---|
# | `participant` | Who submits data, and what their files are named |
# | `hl7_field_map` | Which HL7 v2 fields are extracted |
# | `ccda_field_map` | Which CCDA elements are extracted, and the XPath that finds each |
# | `quality_rule` | Which extracted fields are scored, how, and against what threshold |
#
# Extracting a new field is a row in a field map. Scoring a new measure, or changing
# a threshold, is a row in `quality_rule`. Onboarding a provider is a row in
# `participant`. None of those is a code change.
#
# The CSV header is the only definition of these tables - they are written straight
# from the file, so there is no second copy of the schema anywhere to drift from it.
# Seeds load with `overwrite`, so a redeploy reconciles configuration rather than
# appending duplicates.

# CELL ********************

import sempy.fabric as fabric
from pyspark.sql.functions import col

WORKSPACE_ID = fabric.get_notebook_workspace_id()
BRONZE_ID = mssparkutils.lakehouse.get("mx_bronze").id
CONFIG_PATH = (f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
               f"{BRONZE_ID}/Files/samples_mx/config")

# target table -> (csv file, columns to cast to boolean)
SEEDS = {
    "mapping.participant":    ("participant.csv",    ["active"]),
    "mapping.hl7_field_map":  ("hl7_field_map.csv",  ["active"]),
    "mapping.ccda_field_map": ("ccda_field_map.csv", ["active"]),
    "mapping.quality_rule":   ("quality_rule.csv",   ["exception_allowed", "active"]),
}

for table, (filename, bool_cols) in SEEDS.items():
    df = (spark.read
          .option("header", "true")
          .option("multiLine", "true")
          .option("escape", '"')
          .csv(f"{CONFIG_PATH}/{filename}"))

    for c in bool_cols:
        df = df.withColumn(c, col(c).cast("boolean"))
    if "threshold" in df.columns:
        df = df.withColumn("threshold", col("threshold").cast("double"))

    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table:<26} {df.count():>4} rows")

# MARKDOWN ********************

# ## What got loaded
#
# The two checks below are worth reading rather than skipping.
#
# The first shows that a rule is just a row: same three rule types applied across six
# message types, with the threshold carried as data.
#
# The second is a **referential check**. Every rule names a `field_path`, and that
# path has to exist in a field map or the rule can never be scored. A non-empty
# result here means the configuration is inconsistent, and it is far cheaper to see
# it now than to find a measure silently missing from the report later.

# CELL ********************

spark.sql("""
    SELECT message_type, rule_type, count(*) AS rules
    FROM mapping.quality_rule WHERE active
    GROUP BY message_type, rule_type
    ORDER BY message_type, rule_type
""").show(50, truncate=False)

# CELL ********************

orphans = spark.sql("""
    WITH mapped AS (
        SELECT field_path FROM mapping.hl7_field_map  WHERE active
        UNION
        SELECT field_path FROM mapping.ccda_field_map WHERE active
    )
    SELECT r.rule_id, r.message_type, r.data_element, r.field_path
    FROM mapping.quality_rule r
    LEFT JOIN mapped m ON m.field_path = r.field_path
    WHERE r.active AND m.field_path IS NULL
    ORDER BY r.rule_id
""")

if orphans.count() == 0:
    print("OK - every active rule measures a field that is extracted.")
else:
    print("PROBLEM - these rules measure a field no field map extracts:")
    orphans.show(50, truncate=False)
