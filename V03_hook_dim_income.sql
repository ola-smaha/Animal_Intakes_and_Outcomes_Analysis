CREATE TABLE IF NOT EXISTS target_schema.dim_per_capita_income (
    income_id INT PRIMARY KEY,
    year INT,
    region VARCHAR(50),
    income INT
);

WITH CTE_BLOOMINGTON_INCOME AS (
    SELECT
        ROW_NUMBER() OVER () AS income_id,
        income_bloomington.year AS year,
        'Bloomington' AS region,
        income_bloomington.bloomington AS income
    FROM target_schema.stg_per_capita_bloomington_monroe_income AS income_bloomington
),
CTE_AUSTIN_INCOME AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY income_austin.year) + (SELECT MAX(income_id) FROM CTE_BLOOMINGTON_INCOME) AS income_id,
        income_austin.year AS year,
        'Austin' AS region,
        income_austin.austin AS income
    FROM target_schema.stg_per_capita_austin_income AS income_austin
),
CTE_DALLAS_INCOME AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY income_dallas.year) + (SELECT MAX(income_id) FROM CTE_AUSTIN_INCOME) AS income_id,
        income_dallas.year AS year,
        'Dallas' AS region,
        income_dallas.dallas AS income
    FROM target_schema.stg_per_capita_dallas_income AS income_dallas
),
CTE_NORFOLK_INCOME AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY income_norfolk.year) + (SELECT MAX(income_id) FROM CTE_DALLAS_INCOME) AS income_id,
        income_norfolk.year AS year,
        'Norfolk' AS region,
        income_norfolk.norfolk AS income
    FROM target_schema.stg_per_capita_norfolk_income AS income_norfolk
),
CTE_SONOMA_INCOME AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY income_sonoma.year) + (SELECT MAX(income_id) FROM CTE_NORFOLK_INCOME) AS income_id,
        income_sonoma.year  AS year,
        'Sonoma' AS region,
        income_sonoma.sonoma AS income
    FROM target_schema.stg_per_capita_sonoma_income AS income_sonoma
)

INSERT INTO target_schema.dim_per_capita_income (income_id, year, region, income)
SELECT * FROM CTE_BLOOMINGTON_INCOME
UNION 
SELECT * FROM CTE_AUSTIN_INCOME
UNION 
SELECT * FROM CTE_DALLAS_INCOME
UNION 
SELECT * FROM CTE_NORFOLK_INCOME
UNION 
SELECT * FROM CTE_SONOMA_INCOME
ORDER BY income_id;
