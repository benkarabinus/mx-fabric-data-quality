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

# ## silver_parse_hl7
#
# Turns the HL7 v2 messages sitting in `bronze.raw_message` into two silver tables:
#
# | Table | Grain | Holds |
# |---|---|---|
# | `hl7.message` | one row per message | the header facts, the **original message text**, and the bronze file path |
# | `hl7.field_value` | one row per message **per mapped field** | the extracted value and whether it was populated |
#
# ### This notebook contains no field names
#
# Search it for `PID-5.1` and you will not find it. The list of fields to extract is read
# from `mapping.hl7_field_map` at runtime. What the notebook knows is *how to resolve an
# HL7 field path* — `SEG-field.component` — not *which* paths matter.
#
# **To extract a new field, add a row to `mapping.hl7_field_map`. Do not edit this notebook.**
#
# ```
# map_id      MAP-ADT-020
# message_type ADT
# segment     PID
# field_path  PID-29
# data_element Patient Death Date
# active      true
# ```
#
# Re-run the notebook and `PID-29` appears in `hl7.field_value`, in the wide view, and is
# available to any quality rule that names it.
#
# ### Why one row per field instead of one column per field
#
# A column-per-field table would need an `ALTER TABLE` every time MX mapped a new field,
# which puts a schema change on the critical path of what should be a configuration change.
# The long shape absorbs new fields as new *rows*. The wide, column-per-field view that
# people actually want to read is generated at the end of this notebook by pivoting the
# mapping — so it follows the mapping automatically.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

# Bronze lives in a different lakehouse, so it needs a three-part name.
BRONZE_MESSAGES = "mx_bronze.bronze.raw_message"

field_map = (
    spark.table("mapping.hl7_field_map")
         .where(F.col("active"))
         .select("map_id", "message_type", "segment", "field_path", "data_element")
)

raw = (
    spark.table(BRONZE_MESSAGES)
         .where(F.col("message_format") == "HL7V2")
         .where(F.col("ingest_status") == "INGESTED")
)

print(f"active field map rows : {field_map.count()}")
print(f"HL7 messages in bronze: {raw.count()}")
field_map.groupBy("message_type").count().orderBy("message_type").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The HL7 v2 reader
#
# Four rules are all that is needed to resolve any `SEG-field.component` path.
#
# **1. Segments are separated by a carriage return.** Not a newline. `\r`. Tools that
# "helpfully" rewrite line endings corrupt HL7 files, which is why `*.hl7` is marked
# binary in `.gitattributes`.
#
# **2. The delimiters are declared by the message itself.** `MSH|^~\&|` says: `|` separates
# fields, `^` separates components, `~` separates repeats, `\` starts an escape sequence and
# `&` separates subcomponents. They are read from MSH rather than assumed.
#
# **3. MSH is off by one.** For every other segment, splitting on `|` puts field *N* at
# index *N*. In MSH the field separator **is** MSH-1, so MSH-*N* sits at index *N-1*.
# Getting this backwards silently returns the neighbouring field — `MSH-9` would come back
# as the message control ID, and every message would be typed as garbage.
#
# **4. A field can repeat.** `PID-13` may carry two phone numbers separated by `~`. For a
# completeness measure the question is "was anything supplied", so the first repeat that
# actually carries a value wins — a leading empty repeat must not mask a populated one.
#
# The same logic applies to repeating *segments*: a message may carry several `OBX`
# segments, and `OBX-3.1` is considered populated if any of them carries it.

# CELL ********************

import re

FIELD_PATH_RE = re.compile(r"^([A-Z][A-Z0-9]{2})-(\d+)(?:\.(\d+))?(?:\.(\d+))?$")

DEFAULT_ENCODING = ("^", "~", "\\", "&")


def split_segments(payload):
    """Split a message into segments. Bare \\r is the HL7 terminator; tolerate the rest."""
    if not payload:
        return []
    return [s for s in re.split(r"\r\n|\r|\n", payload) if s.strip()]


def read_encoding(segments):
    """Read the component/repeat/escape/subcomponent characters out of MSH-2."""
    for seg in segments:
        if seg.startswith("MSH") and len(seg) > 8:
            chars = seg[4:8]
            if len(chars) == 4:
                return (chars[0], chars[1], chars[2], chars[3])
            break
    return DEFAULT_ENCODING


def unescape(value, enc):
    """Turn HL7 escape sequences back into the characters they stand for."""
    if not value or enc[2] not in value:
        return value
    esc = enc[2]
    replacements = {
        esc + "F" + esc: "|",
        esc + "S" + esc: enc[0],
        esc + "R" + esc: enc[1],
        esc + "E" + esc: esc,
        esc + "T" + esc: enc[3],
        esc + ".br" + esc: "\n",
        esc + "X0A" + esc: "\n",
        esc + "X0D" + esc: "\r",
    }
    for token, char in replacements.items():
        value = value.replace(token, char)
    return value


def get_value(segments, enc, segment_id, field_path):
    """Resolve SEG-field[.component[.subcomponent]] across every matching segment."""
    match = FIELD_PATH_RE.match((field_path or "").strip())
    if not match:
        return ""
    _, field, component, subcomponent = match.groups()
    field = int(field)
    component = int(component) if component else None
    subcomponent = int(subcomponent) if subcomponent else None

    # MSH-1 is the field separator itself, so MSH is shifted one place left.
    index = field - 1 if segment_id == "MSH" else field

    for seg in segments:
        if seg[:3] != segment_id:
            continue
        parts = seg.split("|")
        if index >= len(parts):
            continue
        for repeat in parts[index].split(enc[1]):
            value = repeat
            if component:
                pieces = value.split(enc[0])
                value = pieces[component - 1] if component <= len(pieces) else ""
            if subcomponent:
                pieces = value.split(enc[3])
                value = pieces[subcomponent - 1] if subcomponent <= len(pieces) else ""
            value = unescape(value, enc).strip()
            if value:
                return value
    return ""


def read_header(segments, enc):
    """message_type, message_datetime and a parse status for one message."""
    if not segments:
        return ("UNKNOWN", None, "EMPTY")
    if not any(s.startswith("MSH") for s in segments):
        return ("UNKNOWN", None, "NO_MSH")

    message_type = get_value(segments, enc, "MSH", "MSH-9.1") or "UNKNOWN"

    stamp = get_value(segments, enc, "MSH", "MSH-7")
    digits = re.sub(r"\D", "", stamp or "")
    message_datetime = None
    if len(digits) >= 8:
        padded = (digits + "000000")[:14]
        message_datetime = (
            f"{padded[0:4]}-{padded[4:6]}-{padded[6:8]} "
            f"{padded[8:10]}:{padded[10:12]}:{padded[12:14]}"
        )

    status = "OK" if message_type != "UNKNOWN" else "NO_MESSAGE_TYPE"
    return (message_type, message_datetime, status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Applying the mapping
#
# The active field map is collected to the driver and broadcast to the executors as a plain
# dictionary keyed by message type. Each message then looks up only the fields mapped for
# *its own* type — an ADT message is never asked for `RXE-2.1`.
#
# Both outputs are written with `overwrite`, so the notebook can be re-run safely: silver is
# always a pure function of what is in bronze and what is in the mapping.

# CELL ********************

maps_by_type = {}
for row in field_map.collect():
    maps_by_type.setdefault(row["message_type"], []).append(
        (row["map_id"], row["segment"], row["field_path"], row["data_element"])
    )
maps_bc = spark.sparkContext.broadcast(maps_by_type)

messages_source = raw.select(
    "message_uid", "participant_id", "period_year", "period_month",
    "source_path", "raw_payload",
)

MESSAGE_SCHEMA = StructType([
    StructField("message_uid", StringType()),
    StructField("participant_id", StringType()),
    StructField("message_type", StringType()),
    StructField("message_datetime", StringType()),
    StructField("period_year", StringType()),
    StructField("period_month", StringType()),
    StructField("bronze_file_path", StringType()),
    StructField("raw_payload", StringType()),
    StructField("parse_status", StringType()),
])

FIELD_SCHEMA = StructType([
    StructField("message_uid", StringType()),
    StructField("map_id", StringType()),
    StructField("field_path", StringType()),
    StructField("data_element", StringType()),
    StructField("value", StringType()),
    StructField("is_populated", BooleanType()),
])


def to_message(row):
    segments = split_segments(row["raw_payload"])
    enc = read_encoding(segments)
    message_type, message_datetime, status = read_header(segments, enc)
    return (
        row["message_uid"], row["participant_id"], message_type, message_datetime,
        str(row["period_year"]) if row["period_year"] is not None else None,
        str(row["period_month"]) if row["period_month"] is not None else None,
        row["source_path"], row["raw_payload"], status,
    )


def to_field_values(row):
    segments = split_segments(row["raw_payload"])
    enc = read_encoding(segments)
    message_type, _, _ = read_header(segments, enc)
    out = []
    for map_id, segment_id, field_path, data_element in maps_bc.value.get(message_type, []):
        value = get_value(segments, enc, segment_id, field_path)
        out.append((
            row["message_uid"], map_id, field_path, data_element,
            value or None, bool(value),
        ))
    return out


messages = (
    spark.createDataFrame(messages_source.rdd.map(to_message), MESSAGE_SCHEMA)
         .withColumn("message_datetime", F.col("message_datetime").cast("timestamp"))
         .withColumn("period_year", F.col("period_year").cast("int"))
         .withColumn("period_month", F.col("period_month").cast("int"))
)

field_values = spark.createDataFrame(
    messages_source.rdd.flatMap(to_field_values), FIELD_SCHEMA
)

messages.createOrReplaceTempView("tmp_hl7_message")
field_values.createOrReplaceTempView("tmp_hl7_field_value")

spark.sql("""
INSERT OVERWRITE TABLE hl7.message
SELECT
    message_uid,
    participant_id,
    message_type,
    CAST(message_datetime AS TIMESTAMP),
    CAST(period_year  AS INT),
    CAST(period_month AS INT),
    bronze_file_path,
    raw_payload,
    parse_status
FROM tmp_hl7_message
""")

spark.sql("""
INSERT OVERWRITE TABLE hl7.field_value
SELECT message_uid, map_id, field_path, data_element, value, is_populated
FROM tmp_hl7_field_value
""")

print(f"hl7.message     : {spark.table('hl7.message').count()} rows")
print(f"hl7.field_value : {spark.table('hl7.field_value').count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### The wide view
#
# `hl7.field_value` is the right shape for storage and the wrong shape for reading. The view
# below gives the familiar column-per-field table, and its **columns are generated from the
# mapping** — add a mapping row, re-run, and the column appears. Nobody writes DDL.
#
# Columns are named after `data_element`, the business name, rather than the HL7 path, so
# the view reads as `patient_last_name` rather than `pid_5_1`. Where the same data element
# is mapped for several message types the values land in one column, which is what anyone
# comparing feeds would want.
#
# `raw_payload` is deliberately left out of the view — it is large, and `hl7.message` is one
# join away when someone needs to see the original text behind a failing field.

# CELL ********************

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
    "message_uid", "participant_id", "message_type", "message_datetime",
    "period_year", "period_month", "bronze_file_path", "parse_status",
]
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
print(f"hl7.vw_message_wide rebuilt with {len(elements)} mapped columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Verification
#
# Three checks worth reading before moving on.
#
# **Message types** should be `ADT`, `ORU`, `MDM`, `VXU` and `RDE`. Anything that looks like
# a facility code or a control ID means the MSH off-by-one has been reintroduced.
#
# **Mapping consistency** — `field_path` must start with the segment named in `segment`. A
# mismatch means a mapping row would silently extract nothing.
#
# **Population rates** are the raw material of every quality rule. A field at 0% is either a
# genuine gap in the feed or a mapping row pointing at the wrong place; the wide view and the
# original message text are both one query away when telling those apart matters.

# CELL ********************

print("Message types parsed")
spark.table("hl7.message").groupBy("message_type", "parse_status").count() \
     .orderBy("message_type").show(truncate=False)

print("Mapping rows whose field_path disagrees with segment")
field_map.where(~F.col("field_path").startswith(F.col("segment"))) \
         .select("map_id", "segment", "field_path").show(truncate=False)

print("Population rate per mapped field")
spark.sql("""
    SELECT m.message_type,
           f.field_path,
           f.data_element,
           SUM(CASE WHEN f.is_populated THEN 1 ELSE 0 END) AS populated,
           COUNT(*)                                        AS messages
    FROM hl7.field_value f
    JOIN hl7.message m ON m.message_uid = f.message_uid
    GROUP BY m.message_type, f.field_path, f.data_element
    ORDER BY m.message_type, f.field_path
""").show(60, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
