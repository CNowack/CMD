#!/usr/bin/env python3
"""
load_derosa_data.py — Derosa et al. (Cell 2024) database loader
================================================================
Reads the three processed files from data/processed/ and inserts
their contents into Patient, Sample, and GenusAbundance.

Run AFTER:  schema.sql
            load_bmc_data.py   (populates Taxonomy + SIG annotation)
Run BEFORE: nothing — this is the last loading step.

Input files (data/processed/):
    derosa_patients.csv        — 188 NSCLC validation patients
    derosa_genus_abundance.csv — genus-level relative abundance for those patients

No microbiome processing logic here — all transformations were done
in process_derosa.py. This script only reads and inserts.

patient_id (FK to Patient.patient_id) is derived at load time from the sid,
not stored in the processed files. For Derosa samples:
    sid format:  "{patient_id}_stool_baseline"
    patient_id:  strip "_stool_baseline" suffix from sid
"""

import os
import mariadb
import pandas as pd
import getpass

# =============================================================================
# Connection
# =============================================================================

user = getpass.getpass("Enter BU username: ")
pswd = getpass.getpass("Enter BU password: ")
db   = getpass.getpass("Enter database name: ")

conn = mariadb.connect(
    host="bioed-new.bu.edu",
    user=user,
    password=pswd,
    db=db,
    port=4253
)
cursor = conn.cursor()

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(SCRIPT_DIR, "../data/processed")


# =============================================================================
# Helper
# =============================================================================

def clean(val):
    """Convert NaN/None to Python None so MariaDB receives SQL NULL."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def require_processed_file(filename):
    path = os.path.join(PROCESSED_DIR, filename)
    if not os.path.exists(path):
        print(f"ERROR: processed file not found: {path}")
        print(f"       Run process_derosa.py first to generate processed files.")
        raise SystemExit(1)
    return path


# =============================================================================
# Section 1 — Patient
# =============================================================================

print("\n── 1. Derosa patients ────────────────────────────────────")

path = require_processed_file("derosa_patients.csv")
patients_df = pd.read_csv(path)
print(f"  Loaded {len(patients_df)} patients from processed file")

sql = """
    INSERT IGNORE INTO Patient (
        patient_id, source_id, data_source,
        cancer_type,
        response_status, response_status_raw,
        os_months, os_event, os12,
        treatment_line, treatment_line_raw, cohort,
        score, sig_category, pred_topo,
        n_sig1, n_sig2, akk_abundance, akk_tricho,
        atb, ecog_ps, age, bmi, immunotherapy
    ) VALUES (
        %s,%s,%s,
        %s,
        %s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,%s
    )
"""

rows = [
    (
        clean(r.patient_id),
        clean(r.source_id),
        clean(r.data_source),
        clean(r.cancer_type),
        clean(r.response_status),
        clean(r.response_status_raw),
        clean(r.os_months),
        clean(r.os_event),
        clean(r.os12),
        clean(r.treatment_line),
        clean(r.treatment_line_raw),
        clean(r.cohort),
        clean(r.score),
        clean(r.sig_category),
        clean(r.pred_topo),
        clean(r.n_sig1),
        clean(r.n_sig2),
        clean(r.akk_abundance),
        clean(r.akk_tricho),
        clean(r.atb),
        clean(r.ecog_ps),
        clean(r.age),
        clean(r.bmi),
        clean(r.immunotherapy),
    )
    for r in patients_df.itertuples(index=False)
]

cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} patient rows inserted")


# =============================================================================
# Section 2 — Sample
# =============================================================================
# One synthesized stool baseline row per Derosa patient.
# sid = "{patient_id}_stool_baseline" — constructed from patient_id.
# sequencing_batch and days_from_treatment are NULL (not collected in Derosa).

print("\n── 2. Derosa sample rows ─────────────────────────────────")

# Fetch the patient_ids just inserted to ensure FK integrity
cursor.execute(
    "SELECT patient_id FROM Patient WHERE data_source = 'Derosa_NSCLC'"
)
derosa_patient_ids = [row[0] for row in cursor.fetchall()]

if not derosa_patient_ids:
    print("  ERROR: no Derosa_NSCLC patients found — Patient insert may have failed")
    raise SystemExit(1)

sql = """
    INSERT IGNORE INTO Sample
        (sid, patient_id, sample_type, timepoint)
    VALUES (%s, %s, %s, %s)
"""
sample_rows = [
    (f"{pid}_stool_baseline", pid, "stool", "Baseline")
    for pid in derosa_patient_ids
]
cursor.executemany(sql, sample_rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} sample rows inserted")


# =============================================================================
# Section 3 — GenusAbundance
# =============================================================================
# Reads derosa_genus_abundance.csv.
# bbid is derived from sid by stripping "_stool_baseline" suffix.
# Verifies all sids have a corresponding Sample row before inserting.

print("\n── 3. Derosa genus abundance ─────────────────────────────")

path = require_processed_file("derosa_genus_abundance.csv")
genus_df = pd.read_csv(path)
print(f"  Loaded {len(genus_df):,} genus abundance rows from processed file")

# Build set of valid sids from Sample table for FK verification
cursor.execute(
    "SELECT sid FROM Sample WHERE patient_id IN "
    "(SELECT patient_id FROM Patient WHERE data_source = 'Derosa_NSCLC')"
)
valid_sids = {row[0] for row in cursor.fetchall()}

# Verify coverage before inserting
genus_sids    = set(genus_df["sid"])
missing_sids  = genus_sids - valid_sids
if missing_sids:
    print(f"  WARNING: {len(missing_sids)} sids in genus file have no Sample row "
          f"— these rows will be skipped")
    genus_df = genus_df[genus_df["sid"].isin(valid_sids)]

# Derive patient_id from sid: strip "_stool_baseline" suffix
genus_df["patient_id"] = genus_df["sid"].str.replace("_stool_baseline", "", regex=False)

sql = """
    INSERT IGNORE INTO GenusAbundance
        (sid, patient_id, genus, relative_abundance, data_source)
    VALUES (%s, %s, %s, %s, %s)
"""
rows = [
    (
        r.sid,
        r.patient_id,
        r.genus,
        round(float(r.relative_abundance), 6),
        r.data_source,
    )
    for r in genus_df.itertuples(index=False)
]
cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount:,} genus abundance rows inserted")


# =============================================================================
# Sanity check
# =============================================================================

print("\n── Sanity check ──────────────────────────────────────────")

cursor.execute(
    "SELECT data_source, COUNT(*) FROM Patient GROUP BY data_source"
)
print("  Patient rows by cohort:")
for source, count in cursor.fetchall():
    print(f"    {source}: {count}")

cursor.execute(
    "SELECT data_source, COUNT(*), COUNT(DISTINCT sid) "
    "FROM GenusAbundance GROUP BY data_source"
)
print("\n  GenusAbundance by cohort:")
for source, rows, sids in cursor.fetchall():
    print(f"    {source}: {rows:,} rows across {sids} samples")

cursor.execute(
    "SELECT COUNT(*) FROM GenusAbundance ga "
    "JOIN Patient p ON ga.patient_id = p.patient_id "
    "WHERE p.data_source = 'Derosa_NSCLC' "
    "AND ga.genus IN (SELECT DISTINCT genus FROM Taxonomy WHERE sig_group != 'none')"
)
sig_matched = cursor.fetchone()[0]
print(f"\n  Derosa genus abundance rows matching SIG genera: {sig_matched:,}")
print(f"  (Non-zero = SIG annotation is working correctly)")

cursor.close()
conn.close()
print("\nDerosa loading complete.")