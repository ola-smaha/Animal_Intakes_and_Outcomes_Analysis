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
	population_id INT REFERENCES target_schema.dim_population(population_id),
	income_id INT REFERENCES target_schema.dim_per_capita_income(income_id),
	unemployment_id INT REFERENCES target_schema.dim_unemployment(unemployment_id),
	CONSTRAINT unique_animal_intake_outcome
	UNIQUE (animal_id, intake_date, intake_type, outcome_date, outcome_type)
);
CREATE INDEX IF NOT EXISTS idx_intakes_outcomes ON target_schema.fct_intakes_outcomes (intake_outcome_id);
WITH CTE_COMBINED_DATA AS
(
	SELECT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_austin
	UNION ALL
	SELECT id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_bloomington
	UNION ALL
	SELECT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_dallas
	UNION ALL
	SELECT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
	FROM target_schema.stg_intakes_outcomes_norfolk
	UNION ALL
	SELECT animal_id, breed, color, intake_date, intake_type, outcome_date, outcome_type, region
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
	region,
	population_id,
	income_id,
	unemployment_id)
SELECT
	animal.animal_id,
	breed.breed_id,
	color.color_id,
	CTE_COMBINED_DATA.intake_date,
	CTE_COMBINED_DATA.intake_type,
	CTE_COMBINED_DATA.outcome_date,
	CTE_COMBINED_DATA.outcome_type,
	CTE_COMBINED_DATA.region,
	population.population_id,
	income.income_id,
	unemployment.unemployment_id
FROM CTE_COMBINED_DATA
LEFT OUTER JOIN target_schema.dim_population population
    ON population.region = CTE_COMBINED_DATA.region
    AND EXTRACT(YEAR FROM CTE_COMBINED_DATA.outcome_date) = population.year
LEFT OUTER JOIN target_schema.dim_per_capita_income income
    ON CTE_COMBINED_DATA.region = income.region
    AND EXTRACT(YEAR FROM CTE_COMBINED_DATA.outcome_date) = income.year
LEFT OUTER JOIN target_schema.dim_unemployment unemployment
    ON CTE_COMBINED_DATA.region = unemployment.region
    AND EXTRACT(YEAR FROM CTE_COMBINED_DATA.outcome_date) = unemployment.year
LEFT OUTER JOIN target_schema.dim_animal animal
	ON CTE_COMBINED_DATA.animal_id = animal.animal_id
INNER JOIN target_schema.dim_color color
	ON CTE_COMBINED_DATA.color = color.animal_color
LEFT OUTER JOIN target_schema.dim_animal_breed breed
	ON CTE_COMBINED_DATA.breed = breed.animal_breed
ON CONFLICT (animal_id, intake_date, intake_type, outcome_date, outcome_type) DO UPDATE
SET
	outcome_date = EXCLUDED.outcome_date,
    outcome_type = EXCLUDED.outcome_type,
    population_id = EXCLUDED.population_id,
    income_id = EXCLUDED.income_id,
    unemployment_id = EXCLUDED.unemployment_id;
	