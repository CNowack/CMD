#!/usr/bin/env python3
import mariadb
import pandas as pd
import getpass

user = getpass.getpass("Enter BU username: ")
pswd = getpass.getpass("Enter BU password: ")
db = getpass.getpass("Enter database: ")

conn = mariadb.connect(
    host='bioed-new.bu.edu',
    user=user,
    password=pswd,
    db=db,
    port=4253
)

cursor = conn.cursor()

# Helper to convert NaN → None
def clean(val):
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val

# ── Load Patient ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
df = pd.read_csv("clinical_metadata.csv")
df = df.rename(columns={
    "BIOBANK_ID": "bbid",
    "IMMUNOTHERAPY_DRUG": "immunotherapy"
})
df.columns = df.columns.str.lower()
df = df[df["bbid"].notna()]

sql = """
    INSERT IGNORE INTO Patient
        (bbid, cancer_type, cancer_category,
         immunotherapy, combination_tx, type_of_combination_tx, response_status, PFS)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

for _, row in df.iterrows():
    cursor.execute(sql, (
        clean(row["bbid"]),
        clean(row["cancer_type"]),
        clean(row.get("cancer_category")),
        clean(row.get("immunotherapy")),
        clean(row.get("combination_tx")),
        clean(row.get("type_of_combination_tx")),
        clean(row.get("response_status")),
        clean(row.get("pfs"))
    ))

# ── Load Sample ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
df = pd.read_csv("sample_metadata.csv", sep=',')

# Check for ANY missing values
if df.isnull().any().any():
    print("WARNING: Missing values found in sample_metadata.csv:")
    print(df[df.isnull().any(axis=1)])  # Print offending rows
    raise ValueError("Sample metadata should have no NULL values — fix CSV before inserting")

# Validate sample_type
valid = {'buccal', 'stool', 'nasal'}
bad = df[~df["sample_type"].isin(valid)]
if len(bad) > 0:
    print(f"WARNING: {len(bad)} rows have unexpected sample_type values:")
    print(bad[["sid", "sample_type"]])
    raise ValueError("Unexpected sample_type values — fix CSV before inserting")

df["sequencing_batch"] = pd.to_datetime(
    df["sequencing_batch"], format="%m/%d/%Y"
).dt.strftime("%Y-%m-%d")

sql = """
    INSERT IGNORE INTO Sample
        (sid, bbid, sample_type, timepoint,
         sequencing_batch, days_from_treatment)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

for _, row in df.iterrows():
    cursor.execute(sql, (
        row["sid"], row["bbid"], row["sample_type"],
        row["timepoint"], row["sequencing_batch"],
        row["days_from_treatment"]
    ))


# ── Load Taxonomy ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

df = pd.read_csv("taxonomy.csv")

df = df.rename(columns={
    "Domain":     "kingdom",
    "Phylum":     "phylum",
    "Class":      "class",
    "Order":      "ord",
    "Family":     "family",
    "Genus":      "genus",
    "Species":    "species",
})

# Compute lowest_rank — walk right to left, find first non-null
RANKS = ['kingdom', 'phylum', 'class', 'ord', 'family', 'genus', 'species']
UNCLASSIFIED = {'nan', ''}

def get_lowest_rank(row):
    for rank in reversed(RANKS):
        val = row.get(rank)
        if pd.notna(val) and str(val).strip().lower() not in UNCLASSIFIED:
            return rank
    return 'kingdom'  # fallback

df['lowest_rank'] = df.apply(get_lowest_rank, axis=1)

# Fix lowest_rank ENUM: 'ord' in the df but schema ENUM uses 'order'
df['lowest_rank'] = df['lowest_rank'].replace('ord', 'order')

# Replace NaN with None → proper SQL NULL
df = df.where(pd.notnull(df), None)

sql = """
    INSERT IGNORE INTO Taxonomy
        (asvid, kingdom, phylum, class, ord,
         family, genus, species, lowest_rank)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Replace NaN with None using clean()
rows = [
    (
        clean(r["asvid"]),
        clean(r["kingdom"]),
        clean(r["phylum"]),
        clean(r["class"]),
        clean(r["ord"]),
        clean(r["family"]),
        clean(r["genus"]),
        clean(r["species"]),
        clean(r["lowest_rank"])
    )
    for _, r in df.iterrows()
]

cursor.executemany(sql, rows)


# ── Load Observation ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
# feature-table.tsv is WIDE format:
#   rows = ASV IDs, columns = sample IDs, values = counts
#
# We need LONG format:
#   one row per (asvid, sid) pair with abundance_counts


ft = pd.read_csv("feature-table.tsv",
                 sep='	', index_col=0)

# Melt wide → long in one line
long_df = ft.reset_index().melt(
    id_vars="asvid",
    var_name="sid",
    value_name="abundance_counts"
)

# Drop zero-count rows — a missing row means zero abundance
long_df = long_df[long_df["abundance_counts"] > 0]

# Join bbid in from Sample so we can populate the FK
sample_map = pd.read_csv(
    "sample_metadata.csv", sep=',',
    usecols=["sid", "bbid"]
)

long_df = long_df.merge(sample_map, on="sid", how="left")

# Compute relative abundance per sample
totals = long_df.groupby("sid")["abundance_counts"].transform("sum")
long_df["relative_abundance"] = long_df["abundance_counts"] / totals

sql = """
    INSERT IGNORE INTO Observation
        (sid, asvid, bbid, abundance_counts, relative_abundance)
    VALUES (%s, %s, %s, %s, %s)
"""

rows = [
    (r.sid, r.asvid, r.bbid,
     int(r.abundance_counts),
     round(r.relative_abundance, 6))
    for _, r in long_df.iterrows()
]

# executemany is much faster than looping execute() for large tables
cursor.executemany(sql, rows)
conn.commit()
print(f"Loaded {cursor.rowcount} observations")


cursor.close()
conn.close()