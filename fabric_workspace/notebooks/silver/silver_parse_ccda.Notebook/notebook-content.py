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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## silver_parse_ccda
#
# The CCDA counterpart of `silver_parse_hl7`. Same shape, same contract:
#
# | Table | Grain | Holds |
# |---|---|---|
# | `ccda.document` | one row per document | the header facts, the **original XML**, and the bronze file path |
# | `ccda.field_value` | one row per document **per mapped field** | the extracted value and whether it was populated |
#
# ### This notebook contains no XPaths
#
# Search it for `birthTime` and you will not find it. The XPath for every field is read from
# `mapping.ccda_field_map` at runtime. What the notebook knows is *how to run an XPath and
# pull a value off the result* — not *which* elements matter.
#
# **To extract a new field, add a row to `mapping.ccda_field_map`. Do not edit this notebook.**
#
# ```
# map_id       CCDA-014
# section_name Patient Role
# field_path   patient_marital_status
# data_element Patient Marital Status
# xpath        .//hl7:recordTarget/hl7:patientRole/hl7:patient/hl7:maritalStatusCode
# value_attr   code
# active       true
# ```
#
# ### Why there is a `value_attr` column
#
# CCDA puts a great deal of its content in **attributes**, not element text:
#
# ```xml
# <birthTime value="19780312"/>                       <!-- the value is @value -->
# <family>Rivera</family>                             <!-- the value is the text -->
# <administrativeGenderCode code="F" .../>            <!-- the value is @code   -->
# ```
#
# Python's standard-library `ElementTree` supports only a subset of XPath and **cannot
# select an attribute** — `.../hl7:birthTime/@value` is not a legal expression for it. So the
# mapping splits the job in two: `xpath` finds the *element*, and `value_attr` names the
# attribute to read off it. Leave `value_attr` empty and the element's text is used instead.
#
# This is a deliberate trade. A full XPath engine would need a third-party library; the
# two-column split keeps the solution on the standard library and, as a bonus, makes the
# mapping easier to read — you can see at a glance whether a field is an attribute or text.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

# Bronze lives in a different lakehouse, so it needs a three-part name.
BRONZE_MESSAGES = "mx_bronze.bronze.raw_message"

field_map = (
    spark.table("mapping.ccda_field_map")
         .where(F.col("active"))
         .select("map_id", "section_name", "field_path", "data_element", "xpath", "value_attr")
)

raw = (
    spark.table(BRONZE_MESSAGES)
         .where(F.col("message_format") == "CCDA")
         .where(F.col("ingest_status") == "INGESTED")
)

print(f"active field map rows : {field_map.count()}")
print(f"CCDA documents in bronze: {raw.count()}")
field_map.groupBy("section_name").count().orderBy("section_name").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The CCDA reader
#
# Three things are worth knowing.
#
# **1. Everything is in a namespace.** A CCDA document declares
# `xmlns="urn:hl7-org:v3"` as its *default* namespace, which means every element is
# namespaced even though nothing in the file is prefixed. An XPath of `.//recordTarget`
# matches nothing. The mapping therefore writes `hl7:recordTarget` and this notebook binds
# the `hl7:` prefix to `urn:hl7-org:v3`.
#
# **2. The first match wins, but an empty match does not.** A document may carry several
# `addr` elements (home, work). Like the HL7 reader's handling of repeats, the first match
# that actually carries a value is taken — an empty element must not mask a populated one.
#
# **3. A malformed document must not stop the run.** One unparseable file should be recorded
# as `parse_status = 'BAD_XML'` and skipped, not fail the notebook. A single bad export from
# one participant should never block the other two.

# CELL ********************

import xml.etree.ElementTree as ET

NS = {"hl7": "urn:hl7-org:v3"}

# Document-level facts. These are fixed because they describe the document itself rather
# than its clinical content — the equivalent of MSH in an HL7 message.
DOC_TYPE_XPATH = "./hl7:code"
DOC_TIME_XPATH = "./hl7:effectiveTime"


def parse_document(payload):
    """Return the root element, or None if the payload is not parseable XML."""
    if not payload or not payload.strip():
        return None
    try:
        return ET.fromstring(payload)
    except ET.ParseError:
        return None


def read_value(root, xpath, value_attr):
    """Run an XPath and pull a value off the first match that carries one."""
    if root is None or not xpath:
        return ""
    try:
        matches = root.findall(xpath, NS)
    except SyntaxError:
        # A malformed XPath in the mapping is a configuration error, not a data error.
        return ""
    for element in matches:
        if value_attr:
            value = element.get(value_attr) or ""
        else:
            value = element.text or ""
        value = value.strip()
        if value:
            return value
    return ""


def read_effective_time(raw_value):
    """CCDA timestamps are YYYYMMDDHHMMSS, often truncated to YYYYMMDD."""
    digits = "".join(c for c in (raw_value or "") if c.isdigit())
    if len(digits) < 8:
        return None
    digits = (digits + "000000")[:14]
    return (
        f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]} "
        f"{digits[8:10]}:{digits[10:12]}:{digits[12:14]}"
    )


def read_header(root):
    """Document type, effective time, and how the parse went."""
    if root is None:
        return None, None, "BAD_XML"
    code = root.find(DOC_TYPE_XPATH, NS)
    document_type = ""
    if code is not None:
        document_type = (code.get("displayName") or code.get("code") or "").strip()
    time_element = root.find(DOC_TIME_XPATH, NS)
    effective_time = read_effective_time(
        time_element.get("value") if time_element is not None else None
    )
    status = "OK" if document_type else "NO_DOCUMENT_TYPE"
    return document_type or None, effective_time, status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Applying the mapping
#
# Unlike HL7, where the field list depends on the message type, every CCDA mapping row
# applies to every document — so the whole mapping is broadcast as one list.
#
# Both tables are written with `overwrite`. The notebook always reprocesses everything in
# bronze, which for a POC is both simpler and safer than incremental logic: re-running after
# a mapping change rebuilds silver to match the new mapping, with no stale rows left behind
# for a field that was de-activated.

# CELL ********************

mapping_rows = [
    (row["map_id"], row["field_path"], row["data_element"],
     row["xpath"], row["value_attr"] or "")
    for row in field_map.collect()
]
mapping_bc = spark.sparkContext.broadcast(mapping_rows)

documents_source = raw.select(
    F.col("message_uid").alias("document_uid"),
    "participant_id", "period_year", "period_month",
    "source_path", "raw_payload",
)

DOCUMENT_SCHEMA = StructType([
    StructField("document_uid", StringType()),
    StructField("participant_id", StringType()),
    StructField("document_type", StringType()),
    StructField("effective_time", StringType()),
    StructField("period_year", StringType()),
    StructField("period_month", StringType()),
    StructField("bronze_file_path", StringType()),
    StructField("raw_payload", StringType()),
    StructField("parse_status", StringType()),
])

FIELD_SCHEMA = StructType([
    StructField("document_uid", StringType()),
    StructField("map_id", StringType()),
    StructField("field_path", StringType()),
    StructField("data_element", StringType()),
    StructField("value", StringType()),
    StructField("is_populated", BooleanType()),
])


def to_document(row):
    root = parse_document(row["raw_payload"])
    document_type, effective_time, status = read_header(root)
    return (
        row["document_uid"], row["participant_id"], document_type, effective_time,
        str(row["period_year"]) if row["period_year"] is not None else None,
        str(row["period_month"]) if row["period_month"] is not None else None,
        row["source_path"], row["raw_payload"], status,
    )


def to_field_values(row):
    root = parse_document(row["raw_payload"])
    out = []
    for map_id, field_path, data_element, xpath, value_attr in mapping_bc.value:
        value = read_value(root, xpath, value_attr)
        out.append((
            row["document_uid"], map_id, field_path, data_element,
            value or None, bool(value),
        ))
    return out


documents = (
    spark.createDataFrame(documents_source.rdd.map(to_document), DOCUMENT_SCHEMA)
         .withColumn("effective_time", F.col("effective_time").cast("timestamp"))
         .withColumn("period_year", F.col("period_year").cast("int"))
         .withColumn("period_month", F.col("period_month").cast("int"))
)

field_values = spark.createDataFrame(
    documents_source.rdd.flatMap(to_field_values), FIELD_SCHEMA
)

documents.createOrReplaceTempView("tmp_ccda_document")
field_values.createOrReplaceTempView("tmp_ccda_field_value")

spark.sql("""
INSERT OVERWRITE TABLE ccda.document
SELECT
    document_uid,
    participant_id,
    document_type,
    CAST(effective_time AS TIMESTAMP),
    CAST(period_year  AS INT),
    CAST(period_month AS INT),
    bronze_file_path,
    raw_payload,
    parse_status
FROM tmp_ccda_document
""")

spark.sql("""
INSERT OVERWRITE TABLE ccda.field_value
SELECT document_uid, map_id, field_path, data_element, value, is_populated
FROM tmp_ccda_field_value
""")

print(f"ccda.document    : {spark.table('ccda.document').count()} rows")
print(f"ccda.field_value : {spark.table('ccda.field_value').count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The wide view
#
# Identical in purpose to `hl7.vw_message_wide`: the familiar column-per-field table, with
# **columns generated from the mapping**. Add a mapping row, re-run, and the column appears.
#
# `raw_payload` is left out — CCDA documents are large, and `ccda.document` is one join away
# when someone needs to see the original XML behind a failing field.

# CELL ********************

import re


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
        literal=element.replace("'", "''"), column=column_name(element)
    )
    for element in elements
)

KEY_COLUMNS = [
    "document_uid", "participant_id", "document_type", "effective_time",
    "period_year", "period_month", "bronze_file_path", "parse_status",
]
key_select = ",\n".join(f"    d.{c}" for c in KEY_COLUMNS)
key_group = ", ".join(f"d.{c}" for c in KEY_COLUMNS)

view_sql = f"""
CREATE OR REPLACE VIEW ccda.vw_document_wide AS
SELECT
{key_select},
{pivot_columns}
FROM ccda.document d
LEFT JOIN ccda.field_value f ON f.document_uid = d.document_uid
GROUP BY {key_group}
"""

spark.sql(view_sql)
print(f"ccda.vw_document_wide rebuilt with {len(elements)} mapped columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Verification
#
# **Parse status** should be `OK` for every document. Anything else means the XML did not
# parse or carried no document-level `code` element.
#
# **Population rate per field** is the number worth looking at. On the sample data the
# patient identity fields (name, DOB, gender) are populated everywhere, while address,
# phone, race, ethnicity and language are populated for only some participants — that spread
# is intentional, and it is what makes the quality report show something other than 100%.
#
# A field at **0/N is a red flag**: it usually means the XPath in `mapping.ccda_field_map`
# is wrong, not that the data is missing. Check the XPath against a real document before
# concluding a participant is at fault.

# CELL ********************

print("Parse status")
(spark.table("ccda.document")
      .groupBy("parse_status")
      .count()
      .orderBy("parse_status")
      .show(truncate=False))

print("Documents per participant")
(spark.table("ccda.document")
      .groupBy("participant_id", "document_type")
      .count()
      .orderBy("participant_id")
      .show(truncate=False))

print("Population rate per mapped field")
total = spark.table("ccda.document").count()
(spark.table("ccda.field_value")
      .groupBy("field_path", "data_element")
      .agg(F.sum(F.col("is_populated").cast("int")).alias("populated"))
      .withColumn("documents", F.lit(total))
      .withColumn("rate", F.round(F.col("populated") / F.col("documents"), 3))
      .orderBy("rate", "field_path")
      .show(50, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
