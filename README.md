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

    CMD_Project/
    ├── .gitignore               # Prevents large sequencing/raw data from uploading
    ├── README.md                # Project overview and task tracking
    ├── requirements.txt         # Python environment dependencies
    ├── docs/                    # Proposals, task sheets, and ER diagrams
    ├── db/                      # SQL schemas, sample queries, and load scripts
    ├── app/                     # Main web application (routes, HTML, CSS, JS)
    ├── scripts/                 # External processing scripts (e.g., statistical analysis)
    └── data/                    # Local storage for raw/processed ASV tables (Gitignored)

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

