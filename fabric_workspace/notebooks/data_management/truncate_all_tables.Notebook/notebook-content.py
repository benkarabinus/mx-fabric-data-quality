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

# # truncate_all_tables
#
# Empties the silver and gold tables while leaving their structure in place.
# Bronze retention is untouched, so nothing that was received is lost.

# CELL ********************

# Each schema lives in exactly one lakehouse, so every name is three-part.
# A two-part name would resolve against this notebook's default lakehouse and
# silently find nothing.
SCHEMA_LAKEHOUSE = {
    "hl7":  "mx_silver",
    "ccda": "mx_silver",
    "dq":   "mx_gold",
}

for schema, lakehouse in SCHEMA_LAKEHOUSE.items():
    try:
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {lakehouse}.{schema}").collect()]
    except Exception:                                          # noqa: BLE001
        print(f"{lakehouse}.{schema}: not present")
        continue
    for t in tables:
        name = f"{lakehouse}.{schema}.{t}"
        # SHOW TABLES lists views alongside tables; a view has nothing to
        # truncate, so skip anything TRUNCATE refuses.
        try:
            spark.sql(f"TRUNCATE TABLE {name}")
            print(f"truncated {name}")
        except Exception:                                      # noqa: BLE001
            print(f"skipped {name} (not a table)")
