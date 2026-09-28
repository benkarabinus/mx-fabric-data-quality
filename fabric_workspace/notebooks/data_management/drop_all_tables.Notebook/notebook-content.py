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

# # drop_all_tables
#
# Drops every table this solution created, in every schema.
#
# > This is destructive and includes bronze retention. Re-run the pipeline
# > afterwards to rebuild from the source files.

# CELL ********************

CONFIRM = False   # set to True to actually drop

if not CONFIRM:
    raise SystemExit("Set CONFIRM = True to drop every table.")

# Each schema lives in exactly one lakehouse, so every name is three-part.
# A two-part name would resolve against this notebook's default lakehouse and
# silently find nothing.
SCHEMA_LAKEHOUSE = {
    "bronze":  "mx_bronze",
    "mapping": "mx_silver",
    "hl7":     "mx_silver",
    "ccda":    "mx_silver",
    "dq":      "mx_gold",
}

for schema, lakehouse in SCHEMA_LAKEHOUSE.items():
    try:
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {lakehouse}.{schema}").collect()]
    except Exception:                                          # noqa: BLE001
        print(f"{lakehouse}.{schema}: not present")
        continue
    for t in tables:
        name = f"{lakehouse}.{schema}.{t}"
        # SHOW TABLES lists views alongside tables, and DROP TABLE will not
        # remove a view.
        try:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
        except Exception:                                      # noqa: BLE001
            spark.sql(f"DROP VIEW IF EXISTS {name}")
        print(f"dropped {name}")
