"""Generate the MX sample corpus.

This script rebuilds every file under ``infra/data/samples_mx/hl7`` and
``infra/data/samples_mx/ccda`` from a small set of hand-authored templates.

Why a generator at all?  The quality report is a set of percentages, and a
percentage over three messages is noise.  The corpus therefore needs enough
volume per participant / message type that a compliance rate is meaningful,
and it needs a *designed* spread so the Power BI report tells a story:

    ARRMC   strong  - passes nearly every rule
    BVCH    moderate - fails a handful
    PCMC    weak    - fails many

How it works:

1.  One template per (participant, message type) is read from the existing
    corpus - the richest file available, so every mapped field is present.
2.  Each output message is a clone of that template with a fresh identity
    (control id, MRN, patient name, date of birth, account number, timestamp).
3.  A per-participant QUALITY_PROFILE then sets or clears individual fields.
    ``"PID-13": 0.5`` means "populate this field in half the messages".
    Anything not listed is populated in full.

Everything is deterministic - re-running produces byte-identical output.

Standard library only.  Run from anywhere:

    python infra/data/samples_mx/_generators/gen_samples.py
"""

import csv
import os
import re
import shutil
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CONFIG = os.path.join(ROOT, "config")
# The hand-authored source messages.  Kept apart from the generated corpus so
# that regeneration is idempotent - the generator only ever reads from here
# and only ever writes to hl7/ and ccda/.
TEMPLATES = os.path.join(ROOT, "_templates")

CR = "\r"
CCDA_NS = "urn:hl7-org:v3"
NS = {"hl7": CCDA_NS}

# ---------------------------------------------------------------------------
# Corpus shape
# ---------------------------------------------------------------------------

# Two periods so the report has a time axis that is not degenerate.
MONTHS = [(2026, 1), (2026, 2)]

# Messages per participant per month.  Chosen so the thresholds in
# config/quality_rule.csv land on clean fractions: 8 -> 12.5% steps,
# 5 -> 20%, 4 -> 25%.
COUNTS = {"ADT": 8, "ORU": 5, "MDM": 4, "VXU": 4, "RDE": 4, "CCDA": 4}

# Which message types each participant sends.  The gaps are deliberate:
# a participant that sends no VXU has no immunisation population, so those
# rules must report N/A rather than 0%.
PARTICIPANT_TYPES = {
    "ARRMC": ["ADT", "ORU", "MDM", "VXU", "RDE", "CCDA"],
    "BVCH": ["ADT", "ORU", "MDM", "CCDA"],
    "PCMC": ["ADT", "ORU", "VXU", "RDE", "CCDA"],
}

# ---------------------------------------------------------------------------
# Quality profile - the designed spread
# ---------------------------------------------------------------------------
# field path -> fraction of messages in which the field is populated.
# A field absent from a participant's map is populated in every message.
# Comments name the rule and threshold the fraction is aimed at.

QUALITY_PROFILE = {
    # Strong sender.  Optional demographics are thin, which is realistic,
    # but every threshold is cleared.
    "ARRMC": {
        "PID-13": 0.875,   # P4P-ADT-011 >= 0.5
        "PID-15.1": 0.5,   # P4P-ADT-013 >= 0.25
        "PID-19.1": 0.375,  # P4P-ADT-015 >= 0.25
        "PD1-3.1": 0.75,   # P4P-ADT-020 >= 0.5
        "PD1-4": 0.625,    # P4P-ADT-021 >= 0.5
        "OBX-6": 0.8,      # P4P-ORU-020 >= 0.5
        # P4P-ADT-032 is BIDIRECTIONAL over PV1-10 and PV1-18.  Hospital
        # service alone would miss the 0.9 threshold; the PV1-18 companion
        # (absent here, so populated everywhere) carries it to 100%.
        "PV1-10": 0.5,     # P4P-ADT-032 >= 0.9 - passes via companion
    },
    # Moderate sender.  Clears the core identity rules, misses several
    # optional-demographic and result-detail thresholds.
    "BVCH": {
        "PID-11": 0.875,   # P4P-ADT-005 >= 0.9    FAIL
        "PID-13": 0.5,     # P4P-ADT-011 >= 0.5    pass, exactly at threshold
        "PID-15.1": 0.125,  # P4P-ADT-013 >= 0.25   FAIL
        "PID-19.1": 0.375,  # P4P-ADT-015 >= 0.25
        "PD1-3.1": 0.25,   # P4P-ADT-020 >= 0.5    WARNING (exception allowed)
        "PD1-4": 0.375,    # P4P-ADT-021 >= 0.5    WARNING (exception allowed)
        "PID-10": 0.875,   # P4P-ADT-010 bidirectional with PID-22.1
        "PID-22.1": 0.5,
        "PV1-10": 0.75,    # P4P-ADT-032 bidirectional with PV1-18
        "PV1-18": 0.625,
        "OBX-3.1": 0.6,    # P4P-ORU-017 >= 0.8    FAIL
        "OBX-6": 0.4,      # P4P-ORU-020 >= 0.5    FAIL
        "TXA-12.1": 0.75,  # P4P-MDM-014 >= 1.0    FAIL
        "patient_address_city": 0.75,   # USCDI-005 >= 0.8   FAIL
        "patient_phone_number": 0.75,   # USCDI-008 >= 0.8   FAIL
    },
    # Weak sender.  Fails widely, including one core identity rule, which is
    # what a real onboarding participant usually looks like.
    "PCMC": {
        "PID-7.1": 0.875,  # P4P-ADT-003 >= 1.0    FAIL on a core field
        "PID-11": 0.5,     # P4P-ADT-005 >= 0.9    FAIL
        "PID-13": 0.25,    # P4P-ADT-011 >= 0.5    FAIL
        "PID-15.1": 0.125,  # P4P-ADT-013 >= 0.25   FAIL
        "PID-19.1": 0.125,  # P4P-ADT-015 >= 0.25   FAIL
        "PD1-3.1": 0.125,  # P4P-ADT-020 >= 0.5    WARNING
        "PD1-4": 0.125,    # P4P-ADT-021 >= 0.5    WARNING
        "PID-10": 0.375,   # P4P-ADT-010 bidirectional  FAIL
        "PID-22.1": 0.25,
        "PV1-2": 0.75,     # P4P-ADT-033 condition - shrinks that denominator
        "PV1-10": 0.5,     # P4P-ADT-032 bidirectional  FAIL
        "PV1-18": 0.375,
        "PV2-3": 0.5,      # P4P-ADT-043 bidirectional  FAIL
        "DG1-3.1": 0.375,
        "OBR-3.1": 0.8,    # P4P-ORU-008 >= 1.0    FAIL
        "OBX-3.1": 0.4,    # P4P-ORU-017 >= 0.8    FAIL
        "OBX-6": 0.2,      # P4P-ORU-020 >= 0.5    FAIL
        "RXA-3": 0.75,     # P4P-VXU-018 >= 0.95   FAIL
        "RXA-5.1": 0.75,   # P4P-VXU-019 >= 0.95   FAIL
        "RXE-2.1": 0.75,   # P4P-RDE-016 >= 0.95   FAIL
        "RXE-3": 0.5,      # P4P-RDE-018 bidirectional with RXE-21.2
        "RXE-21.2": 0.25,
        "patient_address_city": 0.5,     # USCDI-005 >= 0.8  FAIL
        "patient_address_state": 0.75,   # USCDI-006 >= 0.8  FAIL
        "patient_address_zipcode": 0.5,  # USCDI-007 >= 0.8  FAIL
        "patient_phone_number": 0.5,     # USCDI-008 >= 0.8  FAIL
    },
}

# Companion halves of BIDIRECTIONAL rules fill from the back of the run so
# that the union of the two sides covers more messages than either alone -
# which is the whole point of a bidirectional rule.
FROM_END = {"PID-22.1", "PV1-18", "DG1-3.1", "RXE-21.2"}

# Values used when a template does not already carry a controlled field.
FILLER = {
    "PID-10": "2106-3^White^HL70005",
    "PID-11": "100 Main St^^San Bernardino^CA^92401^US^H",
    "PID-13": "^PRN^PH^^^909^5550100",
    "PID-15.1": "en",
    "PID-19.1": "999-00-0000",
    "PID-22.1": "N",
    "PD1-3.1": "Community Clinic",
    "PD1-4": "1234567890^Alvarez^Rita^^^^^^NPI",
    "PV1-2": "I",
    "PV1-10": "MED",
    "PV1-18": "IP",
    "PV2-3": "R69^Illness unspecified^I10",
    "DG1-3.1": "R69",
    "OBR-3.1": "FILL0000001",
    "OBX-3.1": "8867-4",
    "OBX-6": "/min",
    "RXA-3": "20260115",
    "RXA-5.1": "08",
    "RXE-2.1": "00093",
    "RXE-3": "1",
    "TXA-12.1": "DOC0000001",
    "RXE-21.2": "Take 1 tablet by mouth daily",
}

# The CCDA equivalent of FILLER.  Some templates carry nullFlavor placeholders
# instead of real elements - PCMC sends <addr nullFlavor="NI"/> with no child
# elements at all.  There is nothing to strip in that case, so a quality
# fraction aimed at such a field would silently flat-line at 0%.  These values
# let the generator create the element when the profile says it should be
# populated, so the declared fraction is what actually lands in the corpus.
CCDA_FILLER = {
    "patient_address_streetline": "742 Cedar Ridge Rd",
    "patient_address_city": "Pinecrest",
    "patient_address_state": "CA",
    "patient_address_zipcode": "93644",
    "patient_phone_number": "tel:+1-559-555-0142",
}

# Identity pools.  Small and deterministic - the point is variety, not realism.
SURNAMES = ["Rivera", "Nguyen", "Okafor", "Hartley", "Delgado", "Brennan",
            "Castellanos", "Whitfield", "Ahmadi", "Sorenson", "Peralta",
            "Kowalski", "Mbeki", "Ferrara", "Lindqvist", "Toussaint"]
GIVEN = ["Marisol", "Daniel", "Adaeze", "Colin", "Yolanda", "Priya",
         "Martin", "Elise", "Omar", "Rosalind", "Tobias", "Ingrid",
         "Hector", "Naomi", "Curtis", "Delphine"]
DOBS = ["19780312", "19650901", "19920224", "20010717", "19551130",
        "19871005", "19740419", "19990628", "19620213", "20101122",
        "19830906", "19701215", "19950403", "19590820", "20050107",
        "19890514"]


# ---------------------------------------------------------------------------
# HL7 v2 helpers
# ---------------------------------------------------------------------------

PATH_RE = re.compile(r"^([A-Z0-9]{3})-(\d+)(?:\.(\d+))?$")


def parse_path(path):
    """'PID-5.1' -> ('PID', 5, 1).  'PID-13' -> ('PID', 13, None)."""
    m = PATH_RE.match(path)
    if not m:
        raise ValueError("unrecognised field path: %r" % path)
    seg_id, field, comp = m.group(1), int(m.group(2)), m.group(3)
    return seg_id, field, int(comp) if comp else None


def split_segments(text):
    return [s for s in re.split(r"\r\n|\r|\n", text) if s.strip()]


def join_segments(segments):
    return CR.join(segments) + CR


def _field_index(seg_id, field):
    # MSH-1 is the field separator itself, so MSH is offset by one.
    return field - 1 if seg_id == "MSH" else field


def _apply(seg, field, comp, value):
    """Return ``seg`` with the given field (or component) set to ``value``."""
    seg_id = seg[:3]
    idx = _field_index(seg_id, field)
    parts = seg.split("|")
    while len(parts) <= idx:
        parts.append("")
    if comp is None:
        parts[idx] = value
    else:
        comps = parts[idx].split("^")
        while len(comps) < comp:
            comps.append("")
        comps[comp - 1] = value
        parts[idx] = "^".join(comps)
    return "|".join(parts)


def get_field(segments, path):
    """First non-empty value for ``path`` across all matching segments."""
    seg_id, field, comp = parse_path(path)
    idx = _field_index(seg_id, field)
    for seg in segments:
        if seg[:3] != seg_id:
            continue
        parts = seg.split("|")
        if idx >= len(parts):
            continue
        raw = parts[idx]
        if comp is not None:
            comps = raw.split("^")
            raw = comps[comp - 1] if comp <= len(comps) else ""
        if raw:
            return raw
    return ""


def set_field(segments, path, value):
    """Set ``path`` on the first matching segment.  No-op if absent."""
    seg_id, field, comp = parse_path(path)
    for i, seg in enumerate(segments):
        if seg[:3] == seg_id:
            segments[i] = _apply(seg, field, comp, value)
            return True
    return False


def clear_field(segments, path):
    """Blank ``path`` on *every* matching segment.

    Every segment matters: the silver parser scans all repeats of a segment
    and takes the first non-empty value, so leaving one OBX populated would
    leave the field populated.
    """
    seg_id, field, comp = parse_path(path)
    found = False
    for i, seg in enumerate(segments):
        if seg[:3] == seg_id:
            segments[i] = _apply(seg, field, comp, "")
            found = True
    return found


# ---------------------------------------------------------------------------
# Population schedule
# ---------------------------------------------------------------------------

def populated(i, n, fraction, from_end):
    """Is message ``i`` of ``n`` one of the populated ones?"""
    k = int(round(fraction * n))
    if k <= 0:
        return False
    if k >= n:
        return True
    return (n - 1 - i) < k if from_end else i < k


def profile_for(participant, message_type):
    """Field paths this participant controls that apply to this message type."""
    out = {}
    for path, fraction in QUALITY_PROFILE.get(participant, {}).items():
        if PATH_RE.match(path):
            out[path] = fraction
    return out


# ---------------------------------------------------------------------------
# Template discovery
# ---------------------------------------------------------------------------

def read_text(path):
    with open(path, "rb") as fh:
        return fh.read().decode("utf-8")


def find_hl7_templates():
    """(participant, type) -> segment list, choosing the richest example."""
    templates = {}
    base = os.path.join(TEMPLATES, "hl7")
    for dirpath, _dirs, files in os.walk(base):
        for name in sorted(files):
            if not name.endswith(".hl7"):
                continue
            segments = split_segments(read_text(os.path.join(dirpath, name)))
            if not segments:
                continue
            msg_type = get_field(segments, "MSH-9.1")
            participant = get_field(segments, "MSH-4") or name.split("_")[0]
            if not msg_type:
                continue
            key = (participant, msg_type)
            score = sum(len(s) for s in segments)
            if key not in templates or score > templates[key][0]:
                templates[key] = (score, segments)
    return {k: v[1] for k, v in templates.items()}


def find_ccda_templates():
    """participant -> list of (label, template) in filename order.

    Every template is kept rather than only the richest, so a participant that
    supplies more than one document type (BVCH sends both a CCD and a referral
    note) produces both in the generated corpus.  Documents are dealt round
    robin across this list.  The label is the middle token of the template
    filename (CCD, REF) and becomes the middle token of the generated name.
    """
    templates = {}
    base = os.path.join(TEMPLATES, "ccda")
    for dirpath, _dirs, files in os.walk(base):
        for name in sorted(files):
            if not name.endswith(".xml"):
                continue
            full = os.path.join(dirpath, name)
            participant = os.path.relpath(full, base).split(os.sep)[0]
            parts = os.path.splitext(name)[0].split("_")
            label = parts[1] if len(parts) > 2 else "CCD"
            templates.setdefault(participant, []).append(
                (label, read_text(full)))
    return templates


def load_ccda_field_map():
    """field_path -> (xpath, value_attr), straight from the mapping CSV."""
    out = {}
    with open(os.path.join(CONFIG, "ccda_field_map.csv"), newline="",
             encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("active", "").strip().lower() != "true":
                continue
            out[row["field_path"]] = (row["xpath"], row.get("value_attr", ""))
    return out


# ---------------------------------------------------------------------------
# Message construction
# ---------------------------------------------------------------------------

def build_hl7(template, participant, msg_type, seq, index, count,
              year, month, filler_used):
    segments = list(template)
    day = 1 + (index * 3) % 28
    stamp = "%04d%02d%02d%02d%02d%02d" % (
        year, month, day, 8 + (index % 10), (index * 7) % 60, (index * 11) % 60)
    person = (seq * 7) % len(SURNAMES)

    set_field(segments, "MSH-3", participant + "_EHR")
    set_field(segments, "MSH-4", participant)
    set_field(segments, "MSH-7", stamp)
    set_field(segments, "MSH-10", "%s%s%04d" % (participant, msg_type, seq))
    if get_field(segments, "EVN-2"):
        set_field(segments, "EVN-2", stamp)

    set_field(segments, "PID-3.1", "MRN%07d" % (1000000 + seq * 13))
    set_field(segments, "PID-5.1", SURNAMES[person])
    set_field(segments, "PID-5.2", GIVEN[person])
    set_field(segments, "PID-7.1", DOBS[person])
    if get_field(segments, "PID-18"):
        set_field(segments, "PID-18", "ACC%07d" % (2000000 + seq * 13))
    if get_field(segments, "PV1-19"):
        set_field(segments, "PV1-19", "ACC%07d" % (2000000 + seq * 13))

    for path, fraction in profile_for(participant, msg_type).items():
        seg_id = parse_path(path)[0]
        if not any(s[:3] == seg_id for s in segments):
            continue  # segment not in this message type - rule cannot apply
        if populated(index, count, fraction, path in FROM_END):
            if not get_field(segments, path):
                value = FILLER.get(path)
                if value is None:
                    raise KeyError("no FILLER value for %s" % path)
                set_field(segments, path, value)
                filler_used.add(path)
        else:
            clear_field(segments, path)

    return join_segments(segments)


def ccda_ensure(root, field_map, field_path, filler_used):
    """Guarantee that `field_path` resolves to an element carrying a value.

    Some templates use nullFlavor placeholders rather than real elements -
    PCMC sends <addr nullFlavor="NI"/> with no children at all - so there is
    nothing for the profile loop to strip and nothing for the parser to read.
    Create the element in that case, and drop the nullFlavor marker so the
    document does not claim the value is absent while carrying it.
    """
    xpath, value_attr = field_map[field_path]
    value = CCDA_FILLER.get(field_path)
    targets = root.findall(xpath, NS)
    if not targets:
        if value is None:
            return
        parent_xpath, _, leaf = xpath.rpartition("/")
        parents = root.findall(parent_xpath, NS)
        if not parents:
            return
        tag = "{%s}%s" % (CCDA_NS, leaf.split(":")[-1])
        for parent in parents:
            parent.attrib.pop("nullFlavor", None)
            targets.append(ET.SubElement(parent, tag))
        filler_used.add(field_path)
    for el in targets:
        el.attrib.pop("nullFlavor", None)
        if value_attr:
            if not el.get(value_attr) and value is not None:
               el.set(value_attr, value)
               filler_used.add(field_path)
        elif not (el.text or "").strip() and value is not None:
            el.text = value
            filler_used.add(field_path)


def build_ccda(template, participant, seq, index, count, year, month,
               field_map, filler_used):
    root = ET.fromstring(template)
    day = 1 + (index * 3) % 28
    stamp = "%04d%02d%02d%02d%02d%02d" % (
        year, month, day, 9 + (index % 8), (index * 5) % 60, 0)
    person = (seq * 7) % len(SURNAMES)

    doc_id = root.find("./hl7:id", NS)
    if doc_id is not None:
        doc_id.set("extension", "CCD-%s-%04d" % (participant, seq))
    eff = root.find("./hl7:effectiveTime", NS)
    if eff is not None:
        eff.set("value", stamp)

    def matches(field_path):
        xpath = field_map[field_path][0]
        return root.findall(xpath, NS)

    for el in matches("patientrole_id_extension"):
        el.set("extension", "MRN%07d" % (1000000 + seq * 13))
    for el in matches("patient_last_name"):
        el.text = SURNAMES[person]
    for el in matches("patient_first_name"):
        el.text = GIVEN[person]
    for el in matches("patient_dob"):
        el.set("value", DOBS[person])

    for field_path, fraction in QUALITY_PROFILE.get(participant, {}).items():
        if field_path not in field_map:
            continue
        if populated(index, count, fraction, field_path in FROM_END):
            ccda_ensure(root, field_map, field_path, filler_used)
            continue
        _xpath, value_attr = field_map[field_path]
        for el in matches(field_path):
            if value_attr:
                el.attrib.pop(value_attr, None)
            else:
                el.text = None

    ET.register_namespace("", CCDA_NS)
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    body = ET.tostring(root, encoding="unicode")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + body + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    hl7_templates = find_hl7_templates()
    ccda_templates = find_ccda_templates()
    ccda_field_map = load_ccda_field_map()

    triggers = {}
    for (participant, msg_type), segments in hl7_templates.items():
        triggers.setdefault(msg_type, get_field(segments, "MSH-9.2"))

    for kind in ("hl7", "ccda"):
        target = os.path.join(ROOT, kind)
        if os.path.isdir(target):
            shutil.rmtree(target)

    filler_used = set()
    written = {}

    for participant in sorted(PARTICIPANT_TYPES):
        seq = 0
        for msg_type in PARTICIPANT_TYPES[participant]:
            count = COUNTS[msg_type]
            for year, month in MONTHS:
                for index in range(count):
                    seq += 1
                    if msg_type == "CCDA":
                        variants = ccda_templates[participant]
                        label, template = variants[index % len(variants)]
                        text = build_ccda(template, participant, seq, index,
                                          count, year, month, ccda_field_map,
                                          filler_used)
                        name = "%s_%s_%04d.xml" % (participant, label, seq)
                        kind = "ccda"
                        data = text.encode("utf-8")
                    else:
                        template = hl7_templates.get((participant, msg_type))
                        if template is None:
                            # Fall back to another participant's template; the
                            # sending facility is rewritten below.
                            template = next(
                                v for (p, t), v in sorted(hl7_templates.items())
                                if t == msg_type)
                        text = build_hl7(template, participant, msg_type, seq,
                                         index, count, year, month, filler_used)
                        name = "%s_%s_%s_%04d.hl7" % (
                            participant, msg_type, triggers.get(msg_type, "X"),
                            seq)
                        kind = "hl7"
                        data = text.encode("utf-8")

                    out_dir = os.path.join(ROOT, kind, participant,
                                           "%04d" % year, "%02d" % month)
                    os.makedirs(out_dir, exist_ok=True)
                    with open(os.path.join(out_dir, name), "wb") as fh:
                        fh.write(data)
                    key = (participant, msg_type)
                    written[key] = written.get(key, 0) + 1

    print("Wrote %d files" % sum(written.values()))
    for participant in sorted(PARTICIPANT_TYPES):
        row = ["%s %d" % (t, written.get((participant, t), 0))
               for t in PARTICIPANT_TYPES[participant]]
        print("  %-6s %s" % (participant, "  ".join(row)))
    if filler_used:
        print("Filler values used for: %s" % ", ".join(sorted(filler_used)))


if __name__ == "__main__":
    main()
