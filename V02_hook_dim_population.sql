CREATE TABLE IF NOT EXISTS target_schema.dim_population (
    population_id INT PRIMARY KEY,
    year INT,
    region VARCHAR(50),
    population INT
);

WITH CTE_BLOOMINGTON_POPULATION AS (
    SELECT
        ROW_NUMBER() OVER () AS population_id,
        population_bloomington.year AS year,
        'Bloomington' AS region,
        population_bloomington.bloomington * 1000 AS population
    FROM target_schema.stg_population_bloomington_monroe AS population_bloomington
),
CTE_AUSTIN_POPULATION AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY population_austin.year) + (SELECT MAX(population_id) FROM CTE_BLOOMINGTON_POPULATION) AS population_id,
        population_austin.year AS year,
        'Austin' AS region,
        population_austin.austin AS population
    FROM target_schema.stg_population_austin AS population_austin
),
CTE_DALLAS_POPULATION AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY population_dallas.year) + (SELECT MAX(population_id) FROM CTE_AUSTIN_POPULATION) AS population_id,
        population_dallas.year AS year,
        'Dallas' AS region,
        population_dallas.dallas AS population
    FROM target_schema.stg_population_dallas AS population_dallas
),
CTE_NORFOLK_POPULATION AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY population_norfolk.year) + (SELECT MAX(population_id) FROM CTE_DALLAS_POPULATION) AS population_id,
        population_norfolk.year AS year,
        'Norfolk' AS region,
        population_norfolk.norfolk AS population
    FROM target_schema.stg_population_norfolk AS population_norfolk
),
CTE_SONOMA_POPULATION AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY population_sonoma.year) + (SELECT MAX(population_id) FROM CTE_NORFOLK_POPULATION) AS population_id,
        population_sonoma.year  AS year,
        'Sonoma' AS region,
        population_sonoma.sonoma * 1000 AS population
    FROM target_schema.stg_population_sonoma AS population_sonoma
)

INSERT INTO target_schema.dim_population (population_id, year, region, population)
SELECT * FROM CTE_BLOOMINGTON_POPULATION
UNION 
SELECT * FROM CTE_AUSTIN_POPULATION
UNION 
SELECT * FROM CTE_DALLAS_POPULATION
UNION 
SELECT * FROM CTE_NORFOLK_POPULATION
UNION 
SELECT * FROM CTE_SONOMA_POPULATION
ORDER BY population_id;
