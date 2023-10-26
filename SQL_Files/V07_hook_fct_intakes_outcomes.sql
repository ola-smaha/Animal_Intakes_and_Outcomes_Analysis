CREATE TABLE IF NOT EXISTS target_schema.fct_intakes_outcomes
(
	intake_outcome_id SERIAL PRIMARY KEY,
	animal_id TEXT REFERENCES target_schema.dim_animal(animal_id),
	breed_id INT REFERENCES target_schema.dim_animal_breed(breed_id),
	color_id INT REFERENCES target_schema.dim_color(color_id),
	intake_date TIMESTAMP,
	intake_type TEXT,
	outcome_date TIMESTAMP,
	outcome_type TEXT,
	region TEXT,
	population_id INT REFERENCES target_schema.dim_population (population_id),
	income_id INT REFERENCES target_schema.dim_per_capita_income (income_id),
	unemployment_id INT REFERENCES target_schema.dim_unemployment (unemployment_id),
	CONSTRAINT unique_animal_intake_outcome
	UNIQUE (animal_id, intake_date)
);
CREATE INDEX IF NOT EXISTS idx_intakes_outcomes ON target_schema.fct_intakes_outcomes (intake_outcome_id);
WITH CTE_COMBINED_DATA AS
(
	SELECT DISTINCT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_austin
	UNION ALL
	SELECT DISTINCT id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_bloomington
	UNION ALL
	SELECT DISTINCT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_dallas
	UNION ALL
	SELECT DISTINCT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_norfolk
	UNION ALL
	SELECT DISTINCT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_sonoma
)
INSERT INTO target_schema.fct_intakes_outcomes
	(animal_id,
	breed_id,
	color_id,
	intake_date,
	intake_type,
	outcome_date,
	outcome_type,
	region)
SELECT
	animal.animal_id,
	breed.breed_id,
	color.color_id,
	CTE_COMBINED_DATA.intake_date,
	CTE_COMBINED_DATA.intake_type,
	CTE_COMBINED_DATA.outcome_date,
	CTE_COMBINED_DATA.outcome_type,
	CTE_COMBINED_DATA.region
FROM CTE_COMBINED_DATA
LEFT OUTER JOIN target_schema.dim_animal animal
	ON CTE_COMBINED_DATA.animal_id = animal.animal_id
INNER JOIN target_schema.dim_color color
	ON CTE_COMBINED_DATA.color = color.animal_color
LEFT OUTER JOIN target_schema.dim_animal_breed breed
	ON CTE_COMBINED_DATA.breed = breed.animal_breed
ON CONFLICT (animal_id, intake_date) DO UPDATE
SET
	outcome_date = EXCLUDED.outcome_date,
    outcome_type = EXCLUDED.outcome_type;

CREATE TEMPORARY TABLE temp_region_year AS
    SELECT
        fct.intake_outcome_id AS fct_id,
        fct.region AS fct_region,
        EXTRACT(YEAR FROM fct.intake_date) AS fct_year
    FROM target_schema.fct_intakes_outcomes fct
    WHERE fct.population_id IS NULL
	OR fct.income_id IS NULL
	OR fct.unemployment_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_temp_region_year ON temp_region_year(fct_id);
	
UPDATE target_schema.fct_intakes_outcomes fct
SET population_id = dim.population_id
FROM temp_region_year temp
INNER JOIN target_schema.dim_population dim
ON dim.region = temp.fct_region
   AND dim.year = temp.fct_year
WHERE fct.intake_outcome_id = temp.fct_id
   AND fct.population_id IS NULL;
   
UPDATE target_schema.fct_intakes_outcomes fct
SET income_id = dim.income_id
FROM temp_region_year temp
INNER JOIN target_schema.dim_per_capita_income dim
ON dim.region = temp.fct_region
   AND dim.year = temp.fct_year
WHERE fct.intake_outcome_id = temp.fct_id
   AND fct.income_id IS NULL;

UPDATE target_schema.fct_intakes_outcomes fct
SET unemployment_id = dim.unemployment_id
FROM temp_region_year temp
INNER JOIN target_schema.dim_unemployment dim
ON dim.region = temp.fct_region
   AND dim.year = temp.fct_year
WHERE fct.intake_outcome_id = temp.fct_id
   AND fct.unemployment_id IS NULL;

