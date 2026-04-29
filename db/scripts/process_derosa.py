#!/usr/bin/env python3
"""
process_derosa.py — Derosa et al. (Cell 2024) data processor
=============================================================
Phase 1 scope: NSCLC validation cohort only.

Reads raw files from data/derosa/ and writes two clean CSVs
to data/processed/ ready for load_derosa_data.py.

Why only validation cohort?
  - Discovery cohort (n=245) microbiome data not deposited in the repo.
  - met4_valid_complete.csv contains validation cohort microbiome only.
  - Only patients WITH microbiome data are loaded — every patient in the
    database has a corresponding GenusAbundance row.
  - 499 DS1 clinical rows exist but 311 discovery patients have no
    microbiome data; loading them would create misleading empty rows.

Input files (data/derosa/):
    DS1_oncology_clinical_data_DiscValid.csv  — 499 NSCLC patients (disc + valid)
    met4_valid_complete.csv                   — 236 validation microbiome samples
    sig1.txt                                  — SIG1 species list (37 species)
    sig2.txt                                  — SIG2 species list (45 species)

Note: sig*.txt are read by load_bmc_data.py to annotate Taxonomy.sig_group.
They are not processed here — no output file for SIG annotation.

Output files (data/processed/):
    derosa_patients.csv        — one row per patient  → INSERT INTO Patient
    derosa_genus_abundance.csv — one row per (sid, genus) → INSERT INTO GenusAbundance

Output schemas:
    derosa_patients.csv
        patient_id, source_id, data_source, cancer_type,
        response_status, response_status_raw,
        os_months, os_event, os12,
        treatment_line, treatment_line_raw, cohort,
        score, sig_category, pred_topo, n_sig1, n_sig2,
        akk_abundance, akk_tricho,
        atb, ecog_ps, age, bmi, immunotherapy

    derosa_genus_abundance.csv
        sid, genus, relative_abundance, data_source
        (bbid and sample rows derived at load time — not stored here)

Join chain (met4 → Patient):
    met4 Sample_id  →  DS1 Group column  →  DS1 Patient_ID  →  patient_id "NSCLC_#"
    The Group column (not Patient_ID) is the microbiome sample identifier in DS1.
"""

import os
import re
import sys
import pandas as pd

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DEROSA_DIR    = os.path.join(SCRIPT_DIR, "../data/derosa")
PROCESSED_DIR = os.path.join(SCRIPT_DIR, "../data/processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)


# =============================================================================
# Helpers
# =============================================================================

def clean(val):
    """Return None for NaN/empty/NA, otherwise return value as-is."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if val is None:
        return None
    v = str(val).strip()
    return None if v in {"", "nan", "NA", "N/A"} else val


def require_file(path):
    if not os.path.exists(path):
        print(f"ERROR: required file not found: {path}")
        print(f"       Place Derosa repo files in data/derosa/")
        sys.exit(1)


# =============================================================================
# Harmonization maps
# =============================================================================

# Response status: harmonized controlled vocabulary across both cohorts.
# Raw original value always preserved in response_status_raw.
RESPONSE_MAP = {
    "CR":                       "R",
    "PR":                       "R",
    "PR/dissociated response":  "R",
    "SD":                       "SD",
    "SD/dissociated response":  "SD",
    "SD/PD":                    "SD",   # best response = SD before progression
    "PD":                       "NR",
    "PD/death":                 "NR",
    "PD/Death":                 "NR",
    "Death":                    "NR",
    "NaN":                      None,
}

def harmonize_response(raw_val):
    """Return (harmonized_value, raw_string). Warns on unrecognized values."""
    if pd.isna(raw_val):
        return None, None
    raw_str = str(raw_val).strip()
    if not raw_str or raw_str.lower() in {"nan", "na", "n/a"}:
        return None, None
    harmonized = RESPONSE_MAP.get(raw_str)
    if raw_str not in RESPONSE_MAP:
        print(f"  WARNING: unmapped response_status: '{raw_str}'")
    return harmonized, raw_str


def harmonize_treatment_line(ligne_val, treatment_line_val):
    """
    Return (harmonized_value, raw_combined_string).
    raw_combined = "LIGNE_value|Treatment_line_value" for auditability.
    Primary signal: LIGNE column.
      "1"               → '1L'
      starts with ">"
      or is "2","3","4" → '2L+'
      "maintenance"     → 'Maintenance'
    """
    ligne_str  = str(clean(ligne_val) or "").strip()
    txline_str = str(clean(treatment_line_val) or "").strip()
    raw = f"{ligne_str}|{txline_str}" if (ligne_str or txline_str) else None

    if not ligne_str:
        return None, raw
    if ligne_str == "1":
        return "1L", raw
    if ligne_str.startswith(">") or ligne_str in {"2", "3", "4"}:
        return "2L+", raw
    if "maintenance" in ligne_str.lower():
        return "Maintenance", raw
    return None, raw


# =============================================================================
# Genus parsing (MetaPhlAn column names)
# =============================================================================

SGB_PATTERN = re.compile(r'^(GGB|SGB)\d+')
NON_GENUS   = {"sample", "unknown", "unclassified", "nan", ""}

def parse_genus(col_name):
    """
    Extract genus from MetaPhlAn species column name.
    'Faecalibacterium_prausnitzii'       → 'Faecalibacterium'
    'Faecalibacterium_prausnitzii_SGB15' → 'Faecalibacterium'
    'GGB260_SGB362'                      → None  (genome bin — no genus)
    """
    col   = str(col_name).strip()
    genus = col.split("_")[0]
    if SGB_PATTERN.match(genus):
        return None
    if not genus or not genus[0].isupper():
        return None
    if genus.lower() in NON_GENUS:
        return None
    return genus


# =============================================================================
# Section 1 — DS1 NSCLC patients
# =============================================================================
# Load all 499 patients but retain only the 188 that have met4 microbiome data.
# The filtering happens at the end of this section via inner join with met4.
#
# Key structural note:
#   DS1 Group column = microbiome sample identifier (matches met4 Sample_id)
#   DS1 Patient_ID   = integer patient identifier (used to build patient_id PK)
#   These are two separate identifiers — do not conflate them.

print("\n── 1. DS1 NSCLC patients ─────────────────────────────────")

fpath = os.path.join(DEROSA_DIR, "DS1_oncology_clinical_data_DiscValid.csv")
require_file(fpath)

ds1 = pd.read_csv(fpath)
ds1.columns = ds1.columns.str.strip()

cohort_counts = ds1["Cohort"].value_counts().to_dict()
print(f"  Loaded {len(ds1)} rows: {cohort_counts}")

# Load met4 to get the set of sample IDs that have microbiome data
fpath_met4 = os.path.join(DEROSA_DIR, "met4_valid_complete.csv")
require_file(fpath_met4)

# met4 Sample_id values are quoted in the CSV ("PD1p_042").
# pandas read_csv leaves quotes as part of the string value unless
# quotechar is set. Strip explicitly to match DS1 Group format.
met4_ids = set(
    pd.read_csv(fpath_met4, usecols=[0], quotechar='"')
    .iloc[:, 0]
    .astype(str).str.strip().str.strip('"')
)
print(f"  met4 sample IDs available: {len(met4_ids)}")

# Filter DS1 to only patients whose Group ID is in met4
ds1_with_mb = ds1[ds1["Group"].astype(str).str.strip().isin(met4_ids)].copy()
print(f"  DS1 patients with met4 microbiome data: {len(ds1_with_mb)}")

# Build patient records and Group → patient_id lookup for met4 join
all_patients = []
group_to_patient_id = {}   # met4 Sample_id → patient_id (used in Section 2)

for _, row in ds1_with_mb.iterrows():
    group_id = str(row.get("Group", "")).strip()
    if not group_id:
        continue

    # Patient_ID is only populated for the discovery cohort.
    # Validation cohort rows have Patient_ID = NaN — use Group as identifier.
    pid = clean(row.get("Patient_ID"))
    if pid is not None:
        # Discovery cohort: use integer Patient_ID
        patient_id = f"NSCLC_{int(float(str(pid).strip()))}"
        source_id  = str(int(float(str(pid).strip())))
    else:
        # Validation cohort: use Group value as identifier (URL-safe string)
        patient_id = f"NSCLC_{group_id}"
        source_id  = group_id

    group_to_patient_id[group_id] = patient_id

    resp_h, resp_raw     = harmonize_response(row.get("Best_response"))
    tline_h, tline_raw   = harmonize_treatment_line(
        row.get("LIGNE"), row.get("Treatment_line")
    )

    all_patients.append({
        "patient_id":           patient_id,
        "source_id":            source_id,
        "data_source":          "Derosa_NSCLC",
        "cancer_type":          "NSCLC",
        "response_status":      resp_h,
        "response_status_raw":  resp_raw,
        "os_months":            clean(row.get("OS")),
        "os_event":             clean(row.get("Death")),
        "os12":                 clean(row.get("OS12")),
        "treatment_line":       tline_h,
        "treatment_line_raw":   tline_raw,
        "cohort":               clean(row.get("Cohort")),
        "score":                clean(row.get("SCORE")),
        "sig_category":         clean(row.get("SIGCAT")),
        "pred_topo":            clean(row.get("PRED_TOPO")),
        "n_sig1":               clean(row.get("N_SIGB1")),
        "n_sig2":               clean(row.get("N_SIGB2")),
        "akk_abundance":        clean(row.get("Akkermansia_muciniphila")),
        "akk_tricho":           clean(row.get("AKK_TRICHO")),
        "atb":                  clean(row.get("ATB")),
        "ecog_ps":              clean(row.get("ECOGPS")),
        "age":                  clean(row.get("Age")),
        "bmi":                  clean(row.get("BMI")),
        "immunotherapy":        clean(row.get("ICB_monotherapyORcombination")),
    })

print(f"  ✓ {len(all_patients)} patients processed")
print(f"  Group → patient_id lookup: {len(group_to_patient_id)} entries")


# =============================================================================
# Section 2 — GenusAbundance from met4_valid_complete.csv
# =============================================================================
# Wide format: rows = samples (Sample_id), columns = MetaPhlAn species names.
# Values are MetaPhlAn 4.0 relative abundances (0.0–1.0).
#
# Steps:
#   1. Strip quotes from Sample_id values
#   2. Map Sample_id → patient_id using group_to_patient_id lookup
#   3. Derive sid = "{patient_id}_stool_baseline" (synthesized in load script)
#   4. Parse genus from each species column name
#   5. Sum species that share the same genus within each sample
#      (same operation as QIIME2 taxa collapse for BMC data)
#   6. Drop rows with no parseable genus or zero abundance

print("\n── 2. Genus abundance — met4 validation cohort ───────────")

met4 = pd.read_csv(fpath_met4, quotechar='"')
id_col = met4.columns[0]   # "Sample_id"
met4[id_col] = met4[id_col].astype(str).str.strip().str.strip('"')

# met4 contains metadata columns mixed with species columns.
# Detected: 'Cohort' (str), 'OS12' (R/NR), 'AKK_TRICHO' (Low/Zero/High).
# Exclude any column whose values are non-numeric (metadata, not abundance).
def _is_numeric_col(series):
    sample = series.dropna().head(20)
    if len(sample) == 0:
        return True  # empty column — treat as species (will produce no rows)
    def _is_num(v):
        try: float(v); return True
        except: return False
    return all(_is_num(str(v)) for v in sample)

all_non_id_cols = [c for c in met4.columns if c != id_col]
metadata_cols   = [c for c in all_non_id_cols if not _is_numeric_col(met4[c])]
species_cols    = [c for c in all_non_id_cols if c not in metadata_cols]

if metadata_cols:
    print(f"  Metadata columns excluded from melt: {metadata_cols}")

# Pre-compute genus for every species column — done once, not per-row
genus_map = {col: parse_genus(col) for col in species_cols}
n_skipped  = sum(1 for g in genus_map.values() if g is None)
print(f"  Species columns: {len(species_cols)} total, {n_skipped} SGB-only skipped")

all_genus_rows = []
unmatched_samples = []

for _, row in met4.iterrows():
    raw_sid    = row[id_col]
    patient_id = group_to_patient_id.get(raw_sid)

    if patient_id is None:
        unmatched_samples.append(raw_sid)
        continue

    # sid is synthesized — the load script creates the Sample row using this value
    sid = f"{patient_id}_stool_baseline"

    # Accumulate genus totals within this sample
    genus_totals = {}
    for col in species_cols:
        genus = genus_map.get(col)
        if genus is None:
            continue
        val = row[col]
        if pd.notna(val) and float(val) > 0:
            genus_totals[genus] = genus_totals.get(genus, 0.0) + float(val)

    for genus, rel_abund in genus_totals.items():
        all_genus_rows.append({
            "sid":               sid,
            "genus":             genus,
            "relative_abundance": round(rel_abund, 6),
            "data_source":       "Derosa_NSCLC",
        })

if unmatched_samples:
    print(f"  WARNING: {len(unmatched_samples)} met4 rows had no matching patient "
          f"(not in filtered DS1 set)")

genus_df = pd.DataFrame(all_genus_rows,
                         columns=["sid", "genus", "relative_abundance", "data_source"])

# Resolve any (sid, genus) duplicates by summing — should not occur but defensive
dupes = genus_df.duplicated(subset=["sid", "genus"]).sum()
if dupes:
    print(f"  WARNING: {dupes} duplicate (sid, genus) pairs — summing")
    genus_df = (
        genus_df.groupby(["sid", "genus", "data_source"], as_index=False)
        ["relative_abundance"].sum()
    )
    genus_df["relative_abundance"] = genus_df["relative_abundance"].round(6)

print(f"  ✓ {len(genus_df):,} genus abundance rows")
print(f"    Unique samples: {genus_df.sid.nunique()}")
print(f"    Unique genera:  {genus_df.genus.nunique()}")


# =============================================================================
# Write processed files
# =============================================================================

print("\n── Writing processed files ───────────────────────────────")

# ── derosa_patients.csv ───────────────────────────────────────────────────────
patients_df = pd.DataFrame(all_patients, columns=[
    "patient_id", "source_id", "data_source", "cancer_type",
    "response_status", "response_status_raw",
    "os_months", "os_event", "os12",
    "treatment_line", "treatment_line_raw", "cohort",
    "score", "sig_category", "pred_topo", "n_sig1", "n_sig2",
    "akk_abundance", "akk_tricho",
    "atb", "ecog_ps", "age", "bmi", "immunotherapy",
])

dupes = patients_df["patient_id"].duplicated().sum()
if dupes:
    print(f"  WARNING: {dupes} duplicate patient_id values — investigate")

out = os.path.join(PROCESSED_DIR, "derosa_patients.csv")
patients_df.to_csv(out, index=False)
print(f"  ✓ derosa_patients.csv        — {len(patients_df):,} rows")

# Audit response and treatment distributions
print("\n  Response status:")
for val, n in patients_df["response_status"].value_counts(dropna=False).items():
    print(f"    {str(val):6}: {n}")
print("\n  Treatment line:")
for val, n in patients_df["treatment_line"].value_counts(dropna=False).items():
    print(f"    {str(val):15}: {n}")
print("\n  Cohort split:")
for val, n in patients_df["cohort"].value_counts(dropna=False).items():
    print(f"    {str(val):12}: {n}")

# ── derosa_genus_abundance.csv ────────────────────────────────────────────────
# Verify all genus_df sids correspond to a patient
expected_sids = {f"{p}_stool_baseline" for p in patients_df["patient_id"]}
orphan_sids   = set(genus_df["sid"]) - expected_sids
if orphan_sids:
    print(f"\n  WARNING: {len(orphan_sids)} sids in genus_abundance "
          f"have no patient row — dropping")
    genus_df = genus_df[genus_df["sid"].isin(expected_sids)]

out = os.path.join(PROCESSED_DIR, "derosa_genus_abundance.csv")
genus_df.to_csv(out, index=False)
print(f"\n  ✓ derosa_genus_abundance.csv — {len(genus_df):,} rows")


# =============================================================================
# Summary
# =============================================================================

print("\n── Summary ───────────────────────────────────────────────")
print(f"  Patients with microbiome data: {len(patients_df)}")
print(f"  Genus abundance rows:          {len(genus_df):,}")
print(f"  Unique genera:                 {genus_df.genus.nunique()}")
print(f"\n  COVERAGE NOTE:")
print(f"    Discovery cohort (n=245): excluded — no microbiome data in repo.")
print(f"    Validation cohort (n=254): {len(patients_df)} patients loaded "
      f"(those with met4 data).")
print(f"\n  Output: {PROCESSED_DIR}/")
print("\nprocess_derosa.py complete.")