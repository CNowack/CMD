# Cancer Microbiome Database (CMD)
## Team: Cam Nowack, Jack Sherry, Simon Lu

## Supervisor: Chao Zhang (Computational Biomedicine Department)

### Overview
`The Cancer Microbiome Database (CMD) is a relational database and interactive web application that integrates 16S rRNA microbiome sequencing data with clinical patient metadata from Boston Medical Center (BMC). Built for deployment on the BU bioed-new.bu.edu server, it provides a unified, publicly accessible resource to explore microbiome composition, visualize phylogenetic trees, and track longitudinal shifts in patients' microbiome profiles across cancer treatment timepoints.`

### Tech Stack

**Database:** MariaDB (SQL)

**Backend:** Python / Flask

**Frontend:** HTML, CSS, JavaScript

### Repository Structure

CMD/                                         # github.com/CNowack/CMD
│
├── scripts/
│   ├── schema.sql                           # DROP + CREATE all 5 tables
│   ├── load_bmc_data.py                     # loads BMC cohort + SIG annotation
│   ├── process_derosa.py                    # transforms raw Derosa → processed CSVs
│   ├── load_derosa_data.py                  # loads processed Derosa CSVs
│   ├── sanity_check.sql                     # 12 post-load verification queries
│   └── README.md                            # setup + run order instructions
│
├── data/
│   ├── bmc/                                 # BMC ampliseq pipeline outputs
│   │   ├── clinical_metadata.csv            # 182 patients, 8 clinical columns
│   │   ├── sample_metadata.csv              # 547 samples (buccal/stool/nasal)
│   │   ├── taxonomy.csv                     # 16,331 ASVs with taxonomy strings
│   │   ├── feature-table.tsv                # wide: rows=asvid, cols=sid, vals=counts
│   │   ├── rel-table-6.tsv                  # genus-level relative abundance (L6)
│   │   └── rel-table-ASV_with-DADA2-tax.tsv # ASV rel. abundance + taxonomy columns
│   │
│   ├── derosa/                              # Derosa et al. Cell 2024 (MIT license)
│   │   ├── DS1_oncology_clinical_data_DiscValid.csv  # 499 NSCLC patients
│   │   ├── DS1_oncology_clinical_data_Uro_RCC.csv    # 216 RCC/urothelial (not loaded)
│   │   ├── DS3_healthy_donor_clinical_data.csv       # healthy volunteers (not loaded)
│   │   ├── DS5_longitudinal_clinical_data.csv        # 32 NSCLC longitudinal (not loaded)
│   │   ├── DS6_longitudinal_microbiome_data.csv      # DS5 microbiome (not loaded)
│   │   ├── met4_valid_complete.csv          # 188 NSCLC validation microbiome
│   │   ├── sig1.txt                         # 37 SIG1 species (tab-separated)
│   │   └── sig2.txt                         # 45 SIG2 species (tab-separated)
│   │
│   └── processed/                           # output of process_derosa.py
│       ├── derosa_patients.csv              # 188 rows → INSERT INTO Patient
│       └── derosa_genus_abundance.csv       # (sid,genus,rel_abund,data_source)
│
├── app/                                     # Flask web application
│   ├── app.py                               # routes, DB connection, Shannon query
│   ├── templates/
│   │   └── index.html                       # Plotly box plot + filter UI
│   └── static/
│       └── # CSS / JS assets (if any)
│
├── README.md                                # meta-analysis framework + data provenance
├── .env.example                             # DB_HOST, DB_USER, DB_PASS (no real values)
└── .gitignore                               # .env, __pycache__, *.pyc

## Directory & File Details
* docs/: Stores the proposal drafts, final proposal, team task sheet, and annotated ER diagram.

* db/schema.sql: Defines the database tables, primary/foreign keys, and indexes.

* db/load_data.py: Python script to parse ampliseq Nextflow outputs and clinical metadata into the MariaDB database.

* app/app.py: The main Flask server script that handles database connections and routing.

* app/templates/ & app/static/: HTML, CSS, and JavaScript files for an intuitive web interface.

* scripts/external_processing.py: Handles computations outside the database, such as generating phylogenetic trees.

## Project To-Do List
### Phase 1: Planning & Database Design

[ ] Finalize an annotated Entity-Relationship (ER) model with conceptual schema, key, and participation constraints.

[ ] Write schema.sql defining all tables, keys, and indexes.

[ ] Write load_data.py to clean and insert the 547 samples and patient metadata into MariaDB on bioed-new.bu.edu.

[ ] Draft a set of common user questions and design efficient SQL queries to answer them.

### Phase 2: Web Application Development

[ ] Set up the Flask application structure and routing (app.py).

[ ] Build an easy-to-use and intuitive front-end interface using HTML, CSS, and JavaScript.

[ ] Create informative, graphical ways to view query results (phylogenetic trees, abundance charts).

[ ] Implement the longitudinal tracking visualization.

[ ] Create a series of help pages for the database.

### Phase 3: Deliverables & Presentations

[ ] March 31: Submit the Final Proposal.

[ ] April 28 & 30: Present the database to the class.

[ ] May 5 & 6: Final database demonstrations with Dr. Benson.