CREATE TABLE IF NOT EXISTS target_schema.dim_unemployment (
    unemployment_id INT PRIMARY KEY,
    year INT,
    region VARCHAR(50),
    unemployment_rate NUMERIC,
    CONSTRAINT unique_year_region_unemployment
	UNIQUE (year, region)
);
CREATE INDEX IF NOT EXISTS idx_unemployment ON target_schema.dim_unemployment (unemployment_id);

WITH CTE_BLOOMINGTON_UNEMPLOYMENT AS (
    SELECT
        ROW_NUMBER() OVER () AS unemployment_id,
        unemployment_bloomington.year AS year,
        'Bloomington' AS region,
        ROUND(CAST(unemployment_bloomington.bloomington AS NUMERIC),2) AS unemployment_rate
    FROM target_schema.stg_per_capita_bloomington_in_msa_income AS unemployment_bloomington
),
CTE_AUSTIN_UNEMPLOYMENT AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY unemployment_austin.year) + (SELECT MAX(unemployment_id) FROM CTE_BLOOMINGTON_UNEMPLOYMENT) AS unemployment_id,
        unemployment_austin.year AS year,
        'Austin' AS region,
        ROUND(CAST(unemployment_austin.austin AS NUMERIC),2) AS unemployment_rate
    FROM target_schema.stg_unemployment_rate_austin AS unemployment_austin
),
CTE_DALLAS_UNEMPLOYMENT AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY unemployment_dallas.year) + (SELECT MAX(unemployment_id) FROM CTE_AUSTIN_UNEMPLOYMENT) AS unemployment_id,
        unemployment_dallas.year AS year,
        'Dallas' AS region,
        ROUND(CAST(unemployment_dallas.dallas AS NUMERIC),2) AS unemployment_rate
    FROM target_schema.stg_unemployment_rate_dallas AS unemployment_dallas
),
CTE_NORFOLK_UNEMPLOYMENT AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY unemployment_norfolk.year) + (SELECT MAX(unemployment_id) FROM CTE_DALLAS_UNEMPLOYMENT) AS unemployment_id,
        unemployment_norfolk.year AS year,
        'Norfolk' AS region,
        ROUND(CAST(unemployment_norfolk.norfolk AS NUMERIC),2) AS unemployment_rate
    FROM target_schema.stg_unemployment_rate_norfolk_city AS unemployment_norfolk
),
CTE_SONOMA_UNEMPLOYMENT AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY unemployment_sonoma.year) + (SELECT MAX(unemployment_id) FROM CTE_NORFOLK_UNEMPLOYMENT) AS unemployment_id,
        unemployment_sonoma.year  AS year,
        'Sonoma' AS region,
        ROUND(CAST(unemployment_sonoma.sonoma AS NUMERIC),2) AS unemployment_rate
    FROM target_schema.stg_unemployment_rate_sonoma AS unemployment_sonoma
)

INSERT INTO target_schema.dim_unemployment (unemployment_id, year, region, unemployment_rate)
SELECT * FROM CTE_BLOOMINGTON_UNEMPLOYMENT
UNION 
SELECT * FROM CTE_AUSTIN_UNEMPLOYMENT
UNION 
SELECT * FROM CTE_DALLAS_UNEMPLOYMENT
UNION 
SELECT * FROM CTE_NORFOLK_UNEMPLOYMENT
UNION 
SELECT * FROM CTE_SONOMA_UNEMPLOYMENT
ORDER BY unemployment_id
ON CONFLICT (year, region) DO UPDATE
SET
    unemployment_rate = EXCLUDED.unemployment_rate