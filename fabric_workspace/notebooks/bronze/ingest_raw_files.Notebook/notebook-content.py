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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # ingest_raw_files
#
# Lands HL7 v2 and CCDA source files in `bronze.raw_message`, complete and
# unmodified.
#
# ## Nothing is parsed here
#
# This notebook does not know what an HL7 segment is. It copies bytes and records
# where they came from. All interpretation happens in silver, driven by the
# mapping tables - which means the mapping can change and be re-run without
# re-fetching a single file.
#
# ## The folder path is the metadata
#
# Files arrive laid out as:
#
# ```
# <SOURCE_ROOT>/hl7/<participant>/<yyyy>/<mm>/<file>.hl7
# <SOURCE_ROOT>/ccda/<participant>/<yyyy>/<mm>/<file>.xml
# ```
#
# Participant and reporting period are read from that path, not from the message
# body. That is a deliberate choice:
#
# - a file that fails to parse is still attributed to the right participant and
#   month, so the gap it represents is still visible in the report
# - it matches how the files actually arrive from MX's SFTP and S3 landing zones
# - no message needs to be opened to know who sent it
#
# **To onboard a new participant, create a folder.** There is no code change.
#
# ## Choosing a source
#
# | `SOURCE_ROOT` | Reads from |
# |---|---|
# | `Files/samples_mx` | The bundled synthetic sample data (default) |
# | `Files/s3` | MX's real data through the Amazon S3 shortcut |
#
# See `docs/Deploy.md` for how to create the S3 shortcut.

# CELL ********************

import uuid
from datetime import datetime

import sempy.fabric as fabric
from pyspark.sql import Row

# Switch to "Files/s3" once the Amazon S3 shortcut is in place.
SOURCE_ROOT = "Files/samples_mx"

# Each entry is (folder, file extension, message_format).
SOURCES = [
    ("hl7", ".hl7", "HL7V2"),
    ("ccda", ".xml", "CCDA"),
]

WORKSPACE_ID = fabric.get_notebook_workspace_id()
BRONZE_ID = mssparkutils.lakehouse.get("mx_bronze").id
BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_ID}"

ingest_run_id = str(uuid.uuid4())
started_at = datetime.utcnow()

print(f"run    {ingest_run_id}")
print(f"source {SOURCE_ROOT}")

# CELL ********************

def list_files(path):
    """Every file under an OneLake path, recursing into subfolders."""
    found = []
    try:
        entries = mssparkutils.fs.ls(path)
    except Exception as exc:                                    # noqa: BLE001
        print(f"  cannot list {path}: {exc}")
        return found
    for entry in entries:
        if entry.isDir:
            found.extend(list_files(entry.path))
        else:
            found.append(entry)
    return found


def read_path(path, kind):
    """Pull participant, year and month out of <kind>/<participant>/<yyyy>/<mm>/<file>.

    Returns (participant_id, period_year, period_month, relative_path).
    Anything that does not match the expected shape comes back as UNKNOWN /
    None so that the file is still ingested and still visible as a problem.
    """
    parts = [p for p in path.replace("\\", "/").split("/") if p]
    try:
        i = len(parts) - 1 - parts[::-1].index(kind)            # last match wins
    except ValueError:
        return "UNKNOWN", None, None, parts[-1]

    tail = parts[i + 1:]                                        # below <kind>/
    # tail[0] is only a participant folder if something follows it; a file
    # dropped directly under <kind>/ has no participant.
    participant = tail[0] if len(tail) > 1 else "UNKNOWN"
    year = int(tail[1]) if len(tail) > 2 and tail[1].isdigit() else None
    month = int(tail[2]) if len(tail) > 3 and tail[2].isdigit() else None
    return participant, year, month, "/".join([kind] + tail)

# CELL ********************

rows, seen, failed = [], 0, 0

for kind, extension, message_format in SOURCES:
    for entry in list_files(f"{BASE}/{SOURCE_ROOT}/{kind}"):
        if not entry.name.lower().endswith(extension):
            continue
        seen += 1

        participant_id, period_year, period_month, relative = read_path(entry.path, kind)

        try:
            # Read the file whole. HL7 v2 terminates segments with a bare CR,
            # which line-oriented readers mangle.
            payload = mssparkutils.fs.head(entry.path, 10 * 1024 * 1024)
            status, size = "INGESTED", int(entry.size)
        except Exception as exc:                                # noqa: BLE001
            failed += 1
            print(f"  FAILED {relative}: {exc}")
            payload, status, size = "", "FAILED", 0

        rows.append(Row(
            # The relative path is the natural key: unique, stable across runs,
            # and readable when it shows up in a drill-through.
            message_uid=relative,
            participant_id=participant_id,
            message_format=message_format,
            period_year=period_year,
            period_month=period_month,
            source_path=entry.path,
            file_name=entry.name,
            file_bytes=size,
            raw_payload=payload,
            ingest_run_id=ingest_run_id,
            ingest_ts=started_at,
            ingest_status=status,
        ))

if not rows:
    raise ValueError(
        f"No source files found under {SOURCE_ROOT}/. Check that the sample data "
        f"was uploaded, or - if you switched SOURCE_ROOT - that the S3 shortcut exists."
    )

spark.createDataFrame(rows).write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("bronze.raw_message")

spark.createDataFrame([Row(
    ingest_run_id=ingest_run_id,
    started_at=started_at,
    ended_at=datetime.utcnow(),
    source_root=SOURCE_ROOT,
    files_seen=seen,
    files_ingested=seen - failed,
    files_failed=failed,
    status="COMPLETED" if failed == 0 else "COMPLETED_WITH_ERRORS",
)]).write.format("delta").mode("append").saveAsTable("bronze.ingest_run")

print(f"\ningested {seen - failed}/{seen} file(s)")

# MARKDOWN ********************

# ## What landed
#
# A participant of `UNKNOWN`, or a missing period, means a file was found outside
# the expected folder layout. The file is still in bronze - fix the folder and
# re-run.

# CELL ********************

spark.sql("""
    SELECT participant_id,
           period_year,
           period_month,
           message_format,
           count(*)                                        AS files,
           sum(CASE WHEN ingest_status = 'FAILED' THEN 1 ELSE 0 END) AS failed
    FROM bronze.raw_message
    GROUP BY participant_id, period_year, period_month, message_format
    ORDER BY participant_id, message_format
""").show(truncate=False)
