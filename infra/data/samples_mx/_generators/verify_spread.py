"""Score the sample corpus locally and report the quality spread.

Why this exists
---------------
`gen_samples.py` shapes the sample corpus so each participant lands in a
different place against the thresholds in `config/quality_rule.csv` --
ARRMC strong, BVCH moderate, PCMC weak. That shaping is expressed as
fractions, and `round()` on a small count can silently land one message
either side of a threshold. This script checks the intent survived.

It is also the only way to see what the pipeline *should* produce without
deploying to a Fabric capacity.

Fidelity
--------
The parsing functions are not reimplemented here. They are sliced verbatim
out of the two silver notebooks at import time, so this harness cannot drift
away from what actually runs in Fabric. The rule semantics mirror Step 5 of
`gold_score_rules`:

    THRESHOLD      applicable always; satisfied when field_path populated
    BIDIRECTIONAL  applicable always; satisfied when field_path OR any
                   companion_field_paths populated
    CONDITIONAL    applicable when condition_field_path populated;
                   satisfied when field_path populated

Standard library only. Run from anywhere:

    python infra/data/samples_mx/_generators/verify_spread.py
"""

import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
NOTEBOOKS = os.path.join(REPO, "fabric_workspace", "notebooks")
CONFIG = os.path.join(ROOT, "config")


# ---------------------------------------------------------------------------
# Load the parser functions out of the notebooks rather than duplicating them.
# ---------------------------------------------------------------------------

def load_slice(notebook, start_marker, end_marker):
    """Execute the parsing section of a notebook in a private namespace."""
    path = os.path.join(NOTEBOOKS, notebook, "notebook-content.py")
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    # Notebook cell separators are comments, so they are safe to leave in.
    ns = {}
    exec(compile(text[start:end], notebook, "exec"), ns)
    return ns


hl7ns = load_slice(os.path.join("silver", "silver_parse_hl7.Notebook"),
                   "import re", "maps_by_type = {}")
ccdans = load_slice(os.path.join("silver", "silver_parse_ccda.Notebook"),
                    "import xml.etree.ElementTree as ET", "mapping_rows = [")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def read_csv(name):
    with open(os.path.join(CONFIG, name), encoding="utf-8-sig", newline="") as fh:
        return [r for r in csv.DictReader(fh)]


def is_true(value):
    return str(value).strip().lower() in ("true", "1", "yes")


participants = {r["participant_id"]: r["account_name"]
                for r in read_csv("participant.csv") if is_true(r["active"])}
hl7_map = [r for r in read_csv("hl7_field_map.csv") if is_true(r["active"])]
ccda_map = [r for r in read_csv("ccda_field_map.csv") if is_true(r["active"])]
rules = [r for r in read_csv("quality_rule.csv") if is_true(r["active"])]

maps_by_type = {}
for row in hl7_map:
    maps_by_type.setdefault(row["message_type"], []).append(row)


# ---------------------------------------------------------------------------
# Parse the corpus into the same shape as hl7.field_value / ccda.field_value
# ---------------------------------------------------------------------------

def read_path(rel):
    parts = rel.replace("\\", "/").split("/")
    # <format>/<participant>/<yyyy>/<mm>/<file>
    return parts[1], int(parts[2]), int(parts[3])


def walk(kind, suffix):
    base = os.path.join(ROOT, kind)
    for dirpath, _dirs, files in os.walk(base):
        for name in sorted(files):
            if name.endswith(suffix):
                full = os.path.join(dirpath, name)
                yield os.path.relpath(full, ROOT).replace("\\", "/"), full


# (message_uid, participant, period_key, message_type) -> set of populated paths
messages = {}
populated = {}

for rel, full in walk("hl7", ".hl7"):
    participant, year, month = read_path(rel)
    with open(full, "rb") as fh:
        payload = fh.read().decode("utf-8")
    segments = hl7ns["split_segments"](payload)
    enc = hl7ns["read_encoding"](segments)
    message_type, _dt, status = hl7ns["read_header"](segments, enc)
    if status != "OK":
        print("  !! %s parsed as %s" % (rel, status))
        continue
    messages[rel] = (participant, year * 100 + month, message_type)
    hits = set()
    for row in maps_by_type.get(message_type, []):
        value = hl7ns["get_value"](segments, enc, row["segment"],
                                   row["field_path"])
        if value:
            hits.add(row["field_path"])
    populated[rel] = hits

for rel, full in walk("ccda", ".xml"):
    participant, year, month = read_path(rel)
    with open(full, "rb") as fh:
        payload = fh.read().decode("utf-8")
    root = ccdans["parse_document"](payload)
    if root is None:
        print("  !! %s parsed as BAD_XML" % rel)
        continue
    messages[rel] = (participant, year * 100 + month, "CCDA")
    hits = set()
    for row in ccda_map:
        value = ccdans["read_value"](root, row["xpath"], row["value_attr"])
        if value:
            hits.add(row["field_path"])
    populated[rel] = hits


# ---------------------------------------------------------------------------
# Score -- mirrors gold_score_rules Step 5
# ---------------------------------------------------------------------------

def satisfying_paths(rule):
    paths = {rule["field_path"]}
    if rule["rule_type"] == "BIDIRECTIONAL" and rule["companion_field_paths"]:
        for p in rule["companion_field_paths"].split(";"):
            if p.strip():
                paths.add(p.strip())
    return paths


# (participant, period_key, rule_id) -> [applicable, satisfied]
agg = {}

for rel, (participant, period_key, message_type) in sorted(messages.items()):
    hits = populated[rel]
    for rule in rules:
        if rule["message_type"] != message_type:
            continue
        if rule["rule_type"] == "CONDITIONAL":
            trigger = rule["condition_field_path"].strip()
            if trigger and trigger not in hits:
                continue
        key = (participant, period_key, rule["rule_id"])
        slot = agg.setdefault(key, [0, 0])
        slot[0] += 1
        if satisfying_paths(rule) & hits:
            slot[1] += 1


def status_of(numerator, denominator, threshold, exception_allowed):
    if denominator == 0:
        return "N/A"
    if numerator / denominator >= threshold:
        return "Pass"
    return "Warning" if exception_allowed else "Fail"


by_rule = {r["rule_id"]: r for r in rules}

print()
print("Corpus: %d messages, %d rules, %d participant/period/rule cells"
      % (len(messages), len(rules), len(agg)))
print()

failures = []
overall = {}

for participant in sorted(participants):
    counts = {"Pass": 0, "Warning": 0, "Fail": 0, "N/A": 0}
    for (p, period_key, rule_id), (den, num) in agg.items():
        if p != participant:
            continue
        rule = by_rule[rule_id]
        st = status_of(num, den, float(rule["threshold"]),
                       is_true(rule["exception_allowed"]))
        counts[st] += 1
        if st in ("Fail", "Warning"):
            failures.append((participant, period_key, rule_id,
                             rule["data_element"], num, den,
                             float(rule["threshold"]), st))
    scored = counts["Pass"] + counts["Warning"] + counts["Fail"]
    rate = counts["Pass"] / scored if scored else 0.0
    overall[participant] = rate
    print("%-6s %-28s  Pass %3d  Warning %2d  Fail %3d   -> %5.1f%% of rules pass"
          % (participant, participants[participant], counts["Pass"],
             counts["Warning"], counts["Fail"], rate * 100))

print()
print("Rules not meeting threshold (participant x period):")
print("  %-6s %-7s %-14s %-26s %7s %10s %7s"
      % ("part", "period", "rule", "data_element", "rate", "threshold", "status"))
for row in sorted(failures):
    participant, period_key, rule_id, element, num, den, threshold, st = row
    print("  %-6s %-7d %-14s %-26s %6.1f%% %9.0f%% %7s"
          % (participant, period_key, rule_id, element[:26],
             100.0 * num / den, 100.0 * threshold, st))

# Rules that produce no population at all for a participant -- the "N/A, never
# 0%" teaching point. These are message types the participant does not send.
print()
print("Message types absent per participant (score N/A, never 0%):")
for participant in sorted(participants):
    sent = {t for rel, (p, _k, t) in messages.items() if p == participant}
    expected = {r["message_type"] for r in rules}
    missing = sorted(expected - sent)
    print("  %-6s %s" % (participant, ", ".join(missing) if missing else "(none)"))

ordered = sorted(overall, key=lambda p: -overall[p])
print()
print("Ranking (strongest first): %s" % " > ".join(ordered))
if ordered != ["ARRMC", "BVCH", "PCMC"]:
    print("WARNING: expected ARRMC > BVCH > PCMC")
    sys.exit(1)
print("Spread is as designed.")
