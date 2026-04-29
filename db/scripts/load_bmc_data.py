#!/usr/bin/env python3
"""
load_bmc_data.py — BMC cohort loader
=====================================
Populates Patient, Taxonomy, Sample, Observation, and GenusAbundance
from the BMC ampliseq pipeline outputs, then annotates Taxonomy.sig_group
from the Derosa SIG species lists.

Run AFTER schema.sql. Run BEFORE load_derosa_data.py.

Expected file layout (relative to this script):
    ../data/bmc/
        clinical_metadata.csv
        sample_metadata.csv
        taxonomy.csv
        feature-table.tsv
        rel-table-6.tsv
        rel-table-ASV_with-DADA2-tax.tsv
    ../data/derosa/
        sig1.txt
        sig2.txt

Execution order within this script:
    1. Patient (BMC)
    2. Taxonomy
    3. Sample (BMC)
    4. Observation (BMC) — counts from feature-table.tsv,
                           rel. abundance from rel-table-ASV_with-DADA2-tax.tsv
    5. GenusAbundance (BMC) — from rel-table-6.tsv
    6. SIG annotation — updates Taxonomy.sig_group from sig1/sig2 lists
                        (must run after Taxonomy is fully loaded)
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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BMC_DIR    = os.path.join(SCRIPT_DIR, "../data/bmc")
DEROSA_DIR = os.path.join(SCRIPT_DIR, "../data/derosa")


# =============================================================================
# Helpers
# =============================================================================

def clean(val):
    """Convert NaN/None to Python None so MariaDB receives SQL NULL."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


# Response status controlled vocabulary.
# Raw original value is always stored separately in response_status_raw.
RESPONSE_MAP = {
    "Complete/Partial Response":                                "R",
    "Stable Disease":                                           "SD",
    "Stable Disease - continue to check if treatment continued": "SD",
    "Progressive Disease":                                      "NR",
    "Deceased":                                                 "NR",  # PFS <50d confirmed
    "N/A":                                                      None,
    "N/A- never started treatment":                             None,
    "N/A-Hospice":                                              None,
}

def harmonize_response(raw_val):
    """Return (harmonized, raw_string) tuple. Warns on unrecognized values."""
    if pd.isna(raw_val):
        return None, None
    raw_str = str(raw_val).strip()
    if not raw_str:
        return None, None
    if raw_str not in RESPONSE_MAP:
        print(f"  WARNING: unmapped response_status value: '{raw_str}'")
    return RESPONSE_MAP.get(raw_str), raw_str


# Higher-rank taxonomic names that appear as first tokens in unclassified
# SIG species entries — these are family/order/phylum names, not genera.
# Assigning SIG group to these would incorrectly annotate all ASVs
# at those ranks regardless of their true genus.
NON_GENUS_RANKS = {
    "Firmicutes", "Bacteroidetes", "Proteobacteria", "Actinobacteria",
    "Lachnospiraceae", "Clostridiaceae", "Ruminococcaceae", "Erysipelotrichaceae",
    "Clostridiales", "Bacteroidales", "Eubacteriales",
    "Candidatus",   # prefix for uncultured organisms, not a genus
}

def parse_genus_from_sig_list(species_name):
    """
    Extract genus from a MetaPhlAn-format species name.
    'Actinomyces_graevenitzii'          -> 'Actinomyces'
    'Candidatus_Cibiobacter_qucibialis' -> 'Cibiobacter'  (skip Candidatus prefix)
    'Lachnospiraceae_bacterium_OM04'    -> None  (family name, not genus)
    'Firmicutes_bacterium_AF16'         -> None  (phylum name, not genus)
    Returns None for SGB-only, higher-rank names, or unrecognizable entries.
    """
    s = str(species_name).strip()
    parts = s.split("_")

    # Handle "Candidatus_Genus_species" — skip the Candidatus prefix
    if parts[0] == "Candidatus" and len(parts) >= 2:
        genus = parts[1]
    else:
        genus = parts[0]

    # Reject genome bin identifiers
    if not genus or genus[:3] in {"GGB", "SGB"}:
        return None
    # Must start with uppercase
    if not genus[0].isupper():
        return None
    # Reject known higher-rank names and generic organism labels
    if genus in NON_GENUS_RANKS:
        return None
    if genus.lower() in {"bacterium", "archaeon", "unclassified", "nan"}:
        return None
    return genus


# =============================================================================
# Section 1 — Patient (BMC)
# =============================================================================

print("\n── 1. BMC patients ───────────────────────────────────────")

df = pd.read_csv(os.path.join(BMC_DIR, "clinical_metadata.csv"))

# Normalize column names: BIOBANK_ID → source_id, etc.
df = df.rename(columns={
    "BIOBANK_ID":           "source_id",
    "CANCER_TYPE":          "cancer_type",
    "CANCER_CATEGORY":      "cancer_category",
    "IMMUNOTHERAPY_DRUG":   "immunotherapy",
    "COMBINATION_TX":       "combination_tx",
    "TYPE_OF_COMBINATION_TX": "type_of_combination_tx",
    "RESPONSE_STATUS":      "response_status_raw_col",
    "PFS":                  "PFS",
})
df = df[df["source_id"].notna()]

# Construct namespaced patient_id
df["patient_id"] = "BMC_" + df["source_id"].astype(int).astype(str)

sql = """
    INSERT IGNORE INTO Patient (
        patient_id, source_id, data_source,
        cancer_type, cancer_category,
        immunotherapy, combination_tx, type_of_combination_tx,
        response_status, response_status_raw,
        PFS
    ) VALUES (%s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s)
"""

rows = []
for _, row in df.iterrows():
    resp_h, resp_raw = harmonize_response(row.get("response_status_raw_col"))
    rows.append((
        row["patient_id"],
        int(row["source_id"]),
        "BMC",
        clean(row["cancer_type"]),
        clean(row.get("cancer_category")),
        clean(row.get("immunotherapy")),
        clean(row.get("combination_tx")),
        clean(row.get("type_of_combination_tx")),
        resp_h,
        resp_raw,
        clean(row.get("PFS")),
    ))

cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} BMC patients loaded")


# =============================================================================
# Section 2 — Taxonomy
# =============================================================================

print("\n── 2. Taxonomy ───────────────────────────────────────────")

df = pd.read_csv(os.path.join(BMC_DIR, "taxonomy.csv"))

df = df.rename(columns={
    "Domain":  "kingdom",
    "Phylum":  "phylum",
    "Class":   "class",
    "Order":   "ord",
    "Family":  "family",
    "Genus":   "genus",
    "Species": "species",
})

RANKS = ["kingdom", "phylum", "class", "ord", "family", "genus", "species"]
UNCLASSIFIED = {"nan", "", "unclassified", "uncultured bacterium",
                "uncultured organism", "metagenome"}

def get_lowest_rank(row):
    for rank in reversed(RANKS):
        val = row.get(rank)
        if pd.notna(val) and str(val).strip().lower() not in UNCLASSIFIED:
            return rank
    return "kingdom"

df["lowest_rank"] = df.apply(get_lowest_rank, axis=1)
df["lowest_rank"] = df["lowest_rank"].replace("ord", "order")
df = df.where(pd.notnull(df), None)

# ASV sequences come from rel-table-ASV_with-DADA2-tax.tsv
# Load them into a lookup dict: asvid → sequence
asv_path = os.path.join(BMC_DIR, "rel-table-ASV_with-DADA2-tax.tsv")
asv_df = pd.read_csv(asv_path, sep="\t")
asv_df.columns = asv_df.columns.str.strip().str.strip('"')

# Normalize column name for ID column (may be "ID" or quoted)
id_col = asv_df.columns[0]
seq_col = "sequence" if "sequence" in asv_df.columns else None

seq_lookup = {}
if seq_col:
    seq_lookup = dict(zip(asv_df[id_col].astype(str), asv_df[seq_col]))
    print(f"  Loaded {len(seq_lookup)} ASV sequences from rel-table-ASV file")
else:
    print("  WARNING: 'sequence' column not found in rel-table-ASV file — ASV column will be NULL")

sql = """
    INSERT IGNORE INTO Taxonomy
        (asvid, kingdom, phylum, class, ord,
         family, genus, species, ASV, lowest_rank)
    VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s)
"""

rows = []
for _, r in df.iterrows():
    asvid = clean(r.get("asvid") or r.get("ID") or r.get(df.columns[0]))
    rows.append((
        asvid,
        clean(r["kingdom"]),
        clean(r["phylum"]),
        clean(r["class"]),
        clean(r["ord"]),
        clean(r["family"]),
        clean(r["genus"]),
        clean(r["species"]),
        seq_lookup.get(str(asvid)) if asvid else None,
        clean(r["lowest_rank"]),
    ))

cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} taxonomy rows loaded")


# =============================================================================
# Section 3 — Sample (BMC)
# =============================================================================

print("\n── 3. BMC samples ────────────────────────────────────────")

df = pd.read_csv(os.path.join(BMC_DIR, "sample_metadata.csv"))

# Validate no nulls
if df.isnull().any().any():
    print("  WARNING: missing values in sample_metadata.csv:")
    print(df[df.isnull().any(axis=1)])
    raise ValueError("Fix NULLs in sample_metadata.csv before loading.")

# Validate sample_type values
valid_types = {"buccal", "stool", "nasal"}
bad = df[~df["sample_type"].isin(valid_types)]
if len(bad):
    print(f"  WARNING: {len(bad)} unexpected sample_type values:")
    print(bad[["sid", "sample_type"]])
    raise ValueError("Fix sample_type values before loading.")

df["sequencing_batch"] = pd.to_datetime(
    df["sequencing_batch"], format="%m/%d/%Y"
).dt.strftime("%Y-%m-%d")

# Map integer bbid → namespaced patient_id
df["patient_id"] = "BMC_" + df["bbid"].astype(int).astype(str)

sql = """
    INSERT IGNORE INTO Sample
        (sid, bbid, sample_type, timepoint,
         sequencing_batch, days_from_treatment)
    VALUES (%s,%s,%s,%s,%s,%s)
"""
rows = [
    (row["sid"], row["patient_id"], row["sample_type"],
     row["timepoint"], row["sequencing_batch"], row["days_from_treatment"])
    for _, row in df.iterrows()
]
cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} BMC samples loaded")


# =============================================================================
# Section 4 — Observation (BMC)
# =============================================================================
# abundance_counts:   feature-table.tsv  (wide: rows=asvid, cols=sid)
# relative_abundance: rel-table-ASV_with-DADA2-tax.tsv
#                     (rows=asvid, sample columns after taxonomy columns)
#
# Sample IDs in feature-table.tsv must match sample_metadata.csv format.
# feature-table.tsv confirmed to use hyphens (BUCCAL-004-000) — no normalization needed.
# rel-table-ASV_with-DADA2-tax.tsv uses dots (BUCCAL.004.000) — normalize on load.

print("\n── 4. BMC observations ───────────────────────────────────")

# ── 4a. Load raw counts from feature-table.tsv ────────────────────────────────
ft = pd.read_csv(
    os.path.join(BMC_DIR, "feature-table.tsv"),
    sep="\t", index_col=0
)
ft.index.name = "asvid"

counts_long = ft.reset_index().melt(
    id_vars="asvid",
    var_name="sid",
    value_name="abundance_counts"
)
counts_long = counts_long[counts_long["abundance_counts"] > 0].copy()

# ── 4b. Load relative abundance from rel-table-ASV_with-DADA2-tax.tsv ─────────
# Identify taxonomy columns (fixed) vs sample columns (variable)
TAXONOMY_COLS = {"Domain", "Kingdom", "Phylum", "Class", "Order",
                 "Family", "Genus", "Species", "confidence", "sequence"}

asv_df.columns = asv_df.columns.str.strip().str.strip('"')
id_col = asv_df.columns[0]
sample_cols = [c for c in asv_df.columns
               if c not in TAXONOMY_COLS and c != id_col]

rel_long = asv_df[[id_col] + sample_cols].melt(
    id_vars=id_col,
    var_name="sid_dots",
    value_name="relative_abundance"
)
rel_long = rel_long.rename(columns={id_col: "asvid"})

# Normalize dot-separated sample IDs to hyphen format
rel_long["sid"] = rel_long["sid_dots"].str.replace(".", "-", regex=False)
rel_long = rel_long[rel_long["relative_abundance"] > 0][["asvid", "sid", "relative_abundance"]]

# ── 4c. Merge counts and relative abundance ───────────────────────────────────
merged = counts_long.merge(rel_long, on=["asvid", "sid"], how="left")

# Join bbid (patient_id) from sample map
sample_map = pd.read_csv(
    os.path.join(BMC_DIR, "sample_metadata.csv"),
    usecols=["sid", "bbid"]
)
sample_map["patient_id"] = "BMC_" + sample_map["bbid"].astype(int).astype(str)
merged = merged.merge(sample_map[["sid", "patient_id"]], on="sid", how="left")

missing_patients = merged["patient_id"].isna().sum()
if missing_patients:
    print(f"  WARNING: {missing_patients} observation rows have no matching patient — skipping")
    merged = merged[merged["patient_id"].notna()].copy()

sql = """
    INSERT IGNORE INTO Observation
        (sid, asvid, bbid, abundance_counts, relative_abundance)
    VALUES (%s,%s,%s,%s,%s)
"""
rows = [
    (str(r.sid), str(r.asvid), str(r.patient_id),
     int(r.abundance_counts),
     round(float(r.relative_abundance), 6) if pd.notna(r.relative_abundance) else None)
    for _, r in merged.iterrows()
]
cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} BMC observation rows loaded")

# Sanity check: how many ASVs had rel. abundance matched?
matched = merged["relative_abundance"].notna().sum()
total   = len(merged)
print(f"  Rel. abundance matched: {matched}/{total} rows "
      f"({100*matched/total:.1f}%)")


# =============================================================================
# Section 5 — GenusAbundance (BMC)
# =============================================================================
# Source: rel-table-6.tsv
# Format: rows = full QIIME2 taxonomy string, cols = sample IDs (hyphens)
# Values: relative abundance (0.0–1.0) already collapsed to genus level
#
# Row index format:
#   Bacteria;Bacteria;Proteobacteria;Gammaproteobacteria;...;Conservatibacter
# Genus is the 7th semicolon-delimited token (index 6).
# If that token is empty or unclassified, use the last non-empty token
# and record at the appropriate level — but skip from GenusAbundance
# (genus must be known for SIG matching).

print("\n── 5. BMC genus abundance ────────────────────────────────")

# rel-table-6.tsv structure (verified from file):
#   Line 0: "#OTU ID\tBUCCAL-004-000\t..."  <- real header, sample IDs as columns
#   Line 1+: taxonomy_string\tvalue\t...    <- data rows
#
# The index column is named "#OTU ID" — pandas reads this correctly with
# index_col=0. No skiprows needed. Rename index to "tax_string" for clarity.
# Do NOT use comment="#" — it would skip this header line entirely.
gt = pd.read_csv(
    os.path.join(BMC_DIR, "rel-table-6.tsv"),
    sep="\t",
    index_col=0
)
gt.index.name = "tax_string"

genus_long = gt.reset_index().melt(
    id_vars="tax_string",
    var_name="sid",
    value_name="relative_abundance"
)
genus_long = genus_long[genus_long["relative_abundance"] > 0].copy()

UNCLASSIFIED_GENERA = {
    "", "nan", "unclassified", "uncultured", "uncultured bacterium",
    "metagenome", "ambiguous taxa", "unknown"
}

def extract_genus_from_qiime2_string(tax_str):
    """
    Parse genus from a QIIME2 semicolon-delimited taxonomy string.
    'Bacteria;Bacteria;Proteobacteria;...;Pasteurellaceae;Conservatibacter'
    Level indices: 0=kingdom, 1=phylum, 2=class, 3=order, 4=family, 5=genus (L6)
    """
    parts = [p.strip() for p in str(tax_str).split(";")]
    # L6 file: genus is at index 5 (0-based) after kingdom, phylum, class, order, family
    if len(parts) >= 6:
        genus = parts[5].strip()
        if genus.lower() not in UNCLASSIFIED_GENERA:
            return genus
    # Fallback: walk right-to-left for last non-empty, non-unclassified token
    for part in reversed(parts):
        part = part.strip()
        if part.lower() not in UNCLASSIFIED_GENERA:
            return part
    return None

genus_long["genus"] = genus_long["tax_string"].apply(extract_genus_from_qiime2_string)

# Drop rows where genus could not be determined
n_before = len(genus_long)
genus_long = genus_long[genus_long["genus"].notna()]
n_dropped = n_before - len(genus_long)
if n_dropped:
    print(f"  Skipped {n_dropped} rows with unresolved genus")

# Join patient_id from sample map
genus_long = genus_long.merge(sample_map[["sid", "patient_id"]], on="sid", how="left")

# Drop rows with no matching sample (sids in rel-table-6 not in sample_metadata)
n_unmatched = genus_long["patient_id"].isna().sum()
if n_unmatched:
    print(f"  Skipped {n_unmatched} rows with no matching sample")
    genus_long = genus_long[genus_long["patient_id"].notna()].copy()

sql = """
    INSERT IGNORE INTO GenusAbundance
        (sid, bbid, genus, relative_abundance, data_source)
    VALUES (%s,%s,%s,%s,%s)
"""
rows = [
    (str(r.sid), str(r.patient_id), str(r.genus),
     round(float(r.relative_abundance), 6), "BMC")
    for _, r in genus_long.iterrows()
]
cursor.executemany(sql, rows)
conn.commit()
print(f"  ✓ {cursor.rowcount} BMC genus abundance rows loaded")


# =============================================================================
# Section 6 — SIG Group Annotation
# =============================================================================
# Reads sig1.txt and sig2.txt (tab-separated, header row).
# Column 0 = species_Jan21 in MetaPhlAn format: "Genus_species"
# Genus extracted by splitting on first underscore.
# Updates Taxonomy.sig_group for all ASVs whose genus matches.
# Must run after Taxonomy is fully loaded.

print("\n── 6. SIG group annotation ───────────────────────────────")

def load_sig_genera(filepath):
    genera = set()
    with open(filepath) as f:
        next(f)   # skip header: "species_Jan21  SGB_Jan21  GTDB_r207  GTDB_r220"
        for line in f:
            line = line.strip()
            if not line:
                continue
            species_col = line.split("\t")[0]
            genus = parse_genus_from_sig_list(species_col)
            if genus:
                genera.add(genus)
    return genera

sig1_path = os.path.join(DEROSA_DIR, "sig1.txt")
sig2_path = os.path.join(DEROSA_DIR, "sig2.txt")

sig1_genera = load_sig_genera(sig1_path)
sig2_genera = load_sig_genera(sig2_path)

# Handle overlap genera — resolved case by case based on paper species lists:
#
#   Blautia:      SIG1=producta vs SIG2=wexlerae/massiliensis → SIG2 (majority SIG2)
#   Anaerostipes: SIG1=caccae   vs SIG2=hadrus               → SIG2 (hadrus is prevalent)
#   Clostridium:  SIG1=innocuum/perfringens/symbiosum         → SIG1
#                 SIG2=fessum/sp_AF34 (obscure strains)
#                 SIG1 species are clinically dominant — assign SIG1
#
# Default: SIG2 takes precedence EXCEPT for explicitly overridden genera.
FORCE_SIG1 = {'Clostridium'}   # override: SIG1 despite appearing in both lists

overlap = sig1_genera & sig2_genera
if overlap:
    print(f"  WARNING: {len(overlap)} genera in both lists: {overlap}")
    # Apply forced SIG1 overrides first
    forced = overlap & FORCE_SIG1
    if forced:
        print(f"  Forcing SIG1 for: {forced}")
        sig2_genera -= forced   # remove from SIG2 so SIG1 wins
    # Remaining overlap: SIG2 takes precedence
    remaining_overlap = overlap - FORCE_SIG1
    sig1_genera -= remaining_overlap

print(f"  SIG1 genera parsed from sig1.txt: {len(sig1_genera)}")
print(f"  SIG2 genera parsed from sig2.txt: {len(sig2_genera)}")
print(f"  (subset will match Taxonomy — depends on 16S classifier coverage)")

for genera_set, label in [(sig1_genera, "SIG1"), (sig2_genera, "SIG2")]:
    if not genera_set:
        continue
    placeholders = ",".join(["%s"] * len(genera_set))
    cursor.execute(
        f"UPDATE Taxonomy SET sig_group = %s WHERE genus IN ({placeholders})",
        (label, *genera_set)
    )

conn.commit()
print("  ✓ Taxonomy.sig_group updated")

cursor.execute("SELECT sig_group, COUNT(*) FROM Taxonomy GROUP BY sig_group")
sig_counts = {row[0]: row[1] for row in cursor.fetchall()}
for label in ["SIG1", "SIG2", "none"]:
    print(f"    {label}: {sig_counts.get(label, 0):,} ASVs")

# Show how many genera from the sig lists actually matched Taxonomy
cursor.execute(
    "SELECT sig_group, COUNT(DISTINCT genus) AS matched_genera "
    "FROM Taxonomy WHERE sig_group != 'none' GROUP BY sig_group"
)
print("\n  Genera matched in Taxonomy (16S coverage):")
for sig_group, n in cursor.fetchall():
    print(f"    {sig_group}: {n} genera")


# =============================================================================
# Sanity check
# =============================================================================

print("\n── Sanity check ──────────────────────────────────────────")
for table in ["Patient", "Taxonomy", "Sample", "Observation", "GenusAbundance"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    n = cursor.fetchone()[0]
    print(f"  {table}: {n:,} rows")

cursor.execute(
    "SELECT data_source, COUNT(*) FROM Patient GROUP BY data_source"
)
print("\n  Patient rows by cohort:")
for source, count in cursor.fetchall():
    print(f"    {source}: {count}")

cursor.close()
conn.close()
print("\nBMC loading complete.")