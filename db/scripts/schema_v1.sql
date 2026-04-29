USE Team13


DROP TABLE IF EXISTS Observation;
DROP TABLE IF EXISTS Taxonomy;
DROP TABLE IF EXISTS Sample;
DROP TABLE IF EXISTS Patient;




-- ─────────────────────────────────────────
-- Table 1: Patient
-- One row per BMC cancer patient.
-- bbid (biobank ID) uniquely identifies each patient.
-- ─────────────────────────────────────────
CREATE TABLE Patient (
    bbid             INT            NOT NULL,
    cancer_type      VARCHAR(100)   NOT NULL,
    cancer_category  VARCHAR(100),
    immunotherapy    VARCHAR(100),
    combination_tx   VARCHAR(100),
    type_of_combination_tx VARCHAR(100),
    response_status  VARCHAR(50),
    PFS              INT,

    -- Primary key: clustered index on bbid
    PRIMARY KEY (bbid)
);

-- ─────────────────────────────────────────
-- Table 2: Sample
-- One row per collected sample (547 total).
-- Each sample belongs to exactly one patient (bbid FK).
-- ─────────────────────────────────────────
CREATE TABLE Sample (
    sid                  VARCHAR(20)   NOT NULL,
    bbid                 INT           NOT NULL,
    sample_type          ENUM('buccal','stool','nasal')  NOT NULL,
    timepoint VARCHAR(50)   NOT NULL,
    sequencing_batch     DATE          NOT NULL,
    days_from_treatment  INT           NOT NULL,

    -- Primary key: clustered index on sid
    PRIMARY KEY (sid),

    -- Foreign key: bbid must exist in Patient
    FOREIGN KEY (bbid) REFERENCES Patient(bbid),

    -- Unclustered index on bbid for fast patient → samples lookup
    INDEX idx_sample_bbid (bbid)
);

-- ─────────────────────────────────────────
-- Table 3: Taxonomy
-- One row per unique ASV (amplicon sequence variant).
-- Built before Observation — Observation references it.
-- ─────────────────────────────────────────
CREATE TABLE Taxonomy (
    asvid     VARCHAR(64)    NOT NULL,
    kingdom   VARCHAR(100),
    phylum    VARCHAR(100),
    class     VARCHAR(100),
    ord       VARCHAR(100),   
    family    VARCHAR(100),
    genus     VARCHAR(100),
    species   VARCHAR(100),
    lowest_rank  ENUM('kingdom','phylum','class',
                 'order','family','genus','species')
                 NOT NULL,

    -- Primary key: clustered index on asvid
    PRIMARY KEY (asvid)
);


-- ─────────────────────────────────────────
-- Table 4: Observation
-- One row per (sample, species) pair — the abundance count.
-- This is your central join table: references Patient,
-- Sample, and Species simultaneously.
-- ─────────────────────────────────────────
CREATE TABLE Observation (
    sid               VARCHAR(20)   NOT NULL,
    asvid             VARCHAR(64)   NOT NULL,
    bbid              INT           NOT NULL,
    abundance_counts  INT           NOT NULL,
    relative_abundance FLOAT,

    -- Composite primary key: one row per (sample, species) pair
    PRIMARY KEY (sid, asvid),

    -- Foreign keys to all three related tables
    FOREIGN KEY (sid)  REFERENCES Sample(sid),
    FOREIGN KEY (asvid) REFERENCES Taxonomy(asvid),
    FOREIGN KEY (bbid) REFERENCES Patient(bbid),

    -- Unclustered index on bbid: fast patient-level queries
    INDEX idx_obs_bbid (bbid)
);