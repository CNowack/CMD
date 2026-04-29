USE Team13

-- 1. Row counts — do these match your expectations?
SELECT 'Patient'     AS tbl, COUNT(*) AS n FROM Patient
UNION ALL
SELECT 'Sample'      AS tbl, COUNT(*) FROM Sample
UNION ALL
SELECT 'Species'     AS tbl, COUNT(*) FROM Taxonomy
UNION ALL
SELECT 'Observation' AS tbl, COUNT(*) FROM Observation;

-- 2. Taxonomy resolution breakdown
SELECT lowest_rank, COUNT(*) AS n_asvs
FROM Taxonomy
GROUP BY lowest_rank
ORDER BY FIELD(lowest_rank,
    'species','genus','family','order','class','phylum','kingdom');

-- 3. Samples per patient — should be 1–10 roughly
SELECT bbid, COUNT(*) AS n_samples
FROM Sample
GROUP BY bbid
ORDER BY n_samples DESC


-- 4. Spot-check: one patient's full microbiome profile
SELECT s.sample_type, s.timepoint,
       sp.genus, sp.species, o.abundance_counts
FROM Observation o
JOIN Sample  s  ON o.sid   = s.sid
JOIN Taxonomy sp ON o.asvid = sp.asvid
WHERE o.bbid = '4'
ORDER BY o.abundance_counts DESC
LIMIT 20;