-- =============================================================================
-- schema.sql — Cancer Microbiome Database
-- Team 13 / CNowack/CMD
--
-- Creates a fully reproducible database from scratch.
-- DROP + CREATE ensures every run produces an identical schema.
--
-- Phase 1 scope:
--   BMC cohort    — 142 patients, 547 samples, 16,331 ASVs
--   Derosa cohort — 188 NSCLC validation patients (microbiome data only)
--   Analyses      — Shannon diversity (BMC), Sankey (BMC),
--                   SIG prevalence meta-analysis (both cohorts, stool only),
--                   Approximate TOPOSCORE (both cohorts, stool only)
--
-- Table creation order (foreign key dependency):
--   1. Patient        — no dependencies
--   2. Taxonomy       — no dependencies
--   3. Sample         — requires Patient
--   4. Observation    — requires Patient, Sample, Taxonomy
--   5. GenusAbundance — requires Patient, Sample
-- =============================================================================

USE Team13;

-- Drop in reverse dependency order
DROP TABLE IF EXISTS GenusAbundance;
DROP TABLE IF EXISTS Observation;
DROP TABLE IF EXISTS Sample;
DROP TABLE IF EXISTS Taxonomy;
DROP TABLE IF EXISTS Patient;


-- =============================================================================
-- Table 1: Patient
-- One row per individual.
--
-- patient_id:  namespaced surrogate PK
--              BMC patients:    "BMC_4"      (from integer bbid)
--              Derosa patients: "NSCLC_10039" (from integer Patient_ID)
--
-- source_id:   VARCHAR — stores original ID string from source file.
--              BMC: integer bbid as string. Derosa NSCLC: integer Patient_ID as string.
--              VARCHAR (not INT) to accommodate future cohorts with non-integer IDs.
--
-- data_source: 'BMC' | 'Derosa_NSCLC'
--
-- BMC-only columns (NULL for Derosa):
--   cancer_category, combination_tx, type_of_combination_tx, PFS
--
-- Derosa-only columns (NULL for BMC):
--   os_months, os_event, os12, treatment_line, treatment_line_raw, cohort,
--   score, sig_category, pred_topo, n_sig1, n_sig2,
--   akk_abundance, akk_tricho, atb, ecog_ps, age, bmi
--
-- Shared columns (populated for both cohorts):
--   cancer_type, immunotherapy, response_status, response_status_raw
--
-- response_status controlled vocabulary:
--   R  — responder       (BMC: Complete/Partial Response | Derosa: CR, PR)
--   SD — stable disease  (BMC: Stable Disease             | Derosa: SD)
--   NR — non-responder   (BMC: Progressive Disease/Deceased | Derosa: PD, Death)
--   Raw original value always preserved in response_status_raw
--
-- treatment_line controlled vocabulary:
--   1L          — first line
--   2L+         — second line or later
--   Maintenance — maintenance post-CRT
--   Raw LIGNE|Treatment_line value preserved in treatment_line_raw
-- =============================================================================

CREATE TABLE Patient (
    -- Identifiers
    patient_id              VARCHAR(40)     NOT NULL,
    source_id               VARCHAR(100)    NOT NULL,
    data_source             VARCHAR(20)     NOT NULL,

    -- Shared clinical
    cancer_type             VARCHAR(100)    NOT NULL,
    cancer_category         VARCHAR(100)    DEFAULT NULL,
    immunotherapy           VARCHAR(100)    DEFAULT NULL,
    combination_tx          VARCHAR(100)    DEFAULT NULL,
    type_of_combination_tx  VARCHAR(100)    DEFAULT NULL,
    response_status         ENUM('R','SD','NR')             DEFAULT NULL,
    response_status_raw     VARCHAR(100)    DEFAULT NULL,

    -- Survival
    PFS                     INT             DEFAULT NULL,
    os_months               FLOAT           DEFAULT NULL,
    os_event                TINYINT         DEFAULT NULL,
    os12                    TINYINT         DEFAULT NULL,

    -- Treatment
    treatment_line          ENUM('1L','2L+','Maintenance')  DEFAULT NULL,
    treatment_line_raw      VARCHAR(100)    DEFAULT NULL,
    cohort                  VARCHAR(20)     DEFAULT NULL,

    -- Pre-computed TOPOSCORE outputs (Derosa_NSCLC only)
    score                   FLOAT           DEFAULT NULL,
    sig_category            VARCHAR(10)     DEFAULT NULL,
    pred_topo               VARCHAR(10)     DEFAULT NULL,
    n_sig1                  INT             DEFAULT NULL,
    n_sig2                  INT             DEFAULT NULL,
    akk_abundance           FLOAT           DEFAULT NULL,
    akk_tricho              VARCHAR(10)     DEFAULT NULL,

    -- Clinical covariates (Derosa_NSCLC only)
    atb                     TINYINT         DEFAULT NULL,
    ecog_ps                 TINYINT         DEFAULT NULL,
    age                     INT             DEFAULT NULL,
    bmi                     FLOAT           DEFAULT NULL,

    PRIMARY KEY (patient_id),
    INDEX idx_patient_source   (data_source),
    INDEX idx_patient_cancer   (cancer_type),
    INDEX idx_patient_response (response_status)
);


-- =============================================================================
-- Table 2: Taxonomy
-- One row per unique ASV from QIIME2/DADA2. BMC cohort only.
-- Source: taxonomy.csv — all 16,331 ASVs.
--
-- lowest_rank: deepest level successfully resolved for this ASV.
--
-- sig_group: SIG1/SIG2 annotation from Derosa sig1.txt / sig2.txt.
--   Matched at genus level by load_bmc_data.py after all data is loaded.
--   Used by both BMC-only (Sankey) and meta-analysis (SIG prevalence) queries.
--
-- Note: 2,158 ASVs have no Observation rows — they passed taxonomy
-- classification but were filtered by QIIME2 for low abundance. Expected.
-- =============================================================================

CREATE TABLE Taxonomy (
    asvid       VARCHAR(64)     NOT NULL,
    kingdom     VARCHAR(100)    DEFAULT NULL,
    phylum      VARCHAR(100)    DEFAULT NULL,
    class       VARCHAR(100)    DEFAULT NULL,
    ord         VARCHAR(100)    DEFAULT NULL,
    family      VARCHAR(100)    DEFAULT NULL,
    genus       VARCHAR(100)    DEFAULT NULL,
    species     VARCHAR(100)    DEFAULT NULL,
    ASV         TEXT            DEFAULT NULL,
    lowest_rank ENUM('kingdom','phylum','class','order','family','genus','species')
                                NOT NULL DEFAULT 'kingdom',
    sig_group   ENUM('SIG1','SIG2','none')
                                NOT NULL DEFAULT 'none',

    PRIMARY KEY (asvid),
    INDEX idx_tax_genus  (genus),
    INDEX idx_tax_sig    (sig_group),
    INDEX idx_tax_phylum (phylum)
);


-- =============================================================================
-- Table 3: Sample
-- One row per collected sample.
--
-- BMC samples (data_source = 'BMC' via Patient FK):
--   sid    = real IDs from sample_metadata.csv (e.g. BUCCAL-004-000)
--   sample_type = buccal | stool | nasal
--   sequencing_batch and days_from_treatment populated
--
-- Derosa samples (data_source = 'Derosa_NSCLC' via Patient FK):
--   sid    = synthesized "{patient_id}_stool_baseline"
--   sample_type = stool (Derosa collected stool only)
--   timepoint   = 'Baseline'
--   sequencing_batch and days_from_treatment NULL
-- =============================================================================

CREATE TABLE Sample (
    sid                 VARCHAR(60)                     NOT NULL,
    bbid                VARCHAR(40)                     NOT NULL,
    sample_type         ENUM('buccal','stool','nasal')  NOT NULL,
    timepoint           VARCHAR(50)                     NOT NULL,
    sequencing_batch    DATE                            DEFAULT NULL,
    days_from_treatment INT                             DEFAULT NULL,

    PRIMARY KEY (sid),
    FOREIGN KEY (bbid) REFERENCES Patient(patient_id),
    INDEX idx_sample_bbid      (bbid),
    INDEX idx_sample_type      (sample_type),
    INDEX idx_sample_timepoint (timepoint)
);


-- =============================================================================
-- Table 4: Observation
-- One row per (sample, ASV) pair. BMC cohort only.
-- Used by: Shannon diversity analysis, Sankey plot.
--
-- Sources:
--   abundance_counts   — feature-table.tsv (raw DADA2 read counts)
--   relative_abundance — rel-table-ASV_with-DADA2-tax.tsv (QIIME2-normalized)
--
-- 14,173 of 16,331 Taxonomy ASVs appear here.
-- The 2,158 taxonomy-only ASVs have no rows here — correct and expected.
--
-- Derosa data does NOT go here — no ASV identifiers exist for shotgun MG data.
-- Cross-cohort analysis uses GenusAbundance instead.
-- =============================================================================

CREATE TABLE Observation (
    sid                 VARCHAR(60)     NOT NULL,
    asvid               VARCHAR(64)     NOT NULL,
    bbid                VARCHAR(40)     NOT NULL,
    abundance_counts    INT             NOT NULL,
    relative_abundance  FLOAT           DEFAULT NULL,

    PRIMARY KEY (sid, asvid),
    FOREIGN KEY (sid)   REFERENCES Sample(sid),
    FOREIGN KEY (asvid) REFERENCES Taxonomy(asvid),
    FOREIGN KEY (bbid)  REFERENCES Patient(patient_id),
    INDEX idx_obs_bbid (bbid)
);


-- =============================================================================
-- Table 5: GenusAbundance
-- One row per (sample, genus) pair. Holds BOTH cohorts.
-- Used by: SIG prevalence meta-analysis, approximate TOPOSCORE meta-analysis.
--
-- Why separate from Observation?
--   Observation PK is (sid, asvid). Multiple ASVs share the same genus,
--   so genus-collapsed rows cannot be inserted without PK collisions.
--   GenusAbundance PK (sid, genus) accommodates collapsed data cleanly.
--
-- BMC rows (data_source = 'BMC'):
--   Source: rel-table-6.tsv — QIIME2 taxa collapse at genus level (L6).
--   Covers all BMC sample types (buccal, stool, nasal).
--   Meta-analysis queries filter to sample_type = 'stool' via Sample JOIN.
--   Genus names: QIIME2 taxonomy strings with prefix stripped
--                ("g__Blautia" → "Blautia").
--
-- Derosa rows (data_source = 'Derosa_NSCLC'):
--   Source: met4_valid_complete.csv — MetaPhlAn 4.0 validation cohort data.
--   188 patients, stool only, baseline timepoint.
--   Genus parsed from MetaPhlAn species column names (first token before "_").
--   SGB-only columns (e.g. GGB260_SGB362) skipped — no genus extractable.
--   Multiple species columns sharing a genus are summed within each sample.
--
-- data_source is denormalized from Patient to avoid a JOIN on every
-- meta-analysis query that filters by cohort.
-- =============================================================================

CREATE TABLE GenusAbundance (
    sid                 VARCHAR(60)     NOT NULL,
    bbid                VARCHAR(40)     NOT NULL,
    genus               VARCHAR(100)    NOT NULL,
    relative_abundance  FLOAT           NOT NULL,
    data_source         VARCHAR(20)     NOT NULL,

    PRIMARY KEY (sid, genus),
    FOREIGN KEY (sid)  REFERENCES Sample(sid),
    FOREIGN KEY (bbid) REFERENCES Patient(patient_id),
    INDEX idx_ga_bbid   (bbid),
    INDEX idx_ga_genus  (genus),
    INDEX idx_ga_source (data_source)
);