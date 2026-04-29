SELECT 'cancer_type' AS filter_name, cancer_type AS value
FROM Patient
WHERE cancer_type IS NOT NULL
GROUP BY cancer_type

UNION ALL

SELECT 'treatment' AS filter_name, immunotherapy AS value
FROM Patient
WHERE immunotherapy IS NOT NULL
GROUP BY immunotherapy

UNION ALL

SELECT 'sid' AS filter_name, sid AS value
FROM Sample
WHERE sid IS NOT NULL
GROUP BY sid

UNION ALL

SELECT 'sample_type' AS filter_name, sample_type AS value
FROM Sample
WHERE sample_type IS NOT NULL
GROUP BY sample_type

ORDER BY filter_name, value;