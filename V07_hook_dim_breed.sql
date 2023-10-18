CREATE TABLE IF NOT EXISTS dw_reporting_schema.dim_animal_breed
(
	breed_id SERIAL PRIMARY KEY,
	animal_breed TEXT
);
CREATE INDEX idx_animal_breed ON dw_reporting_schema.dim_animal_breed (breed_id);
WITH CTE_ANIMAL_BREED AS
(
	SELECT DISTINCT breed
	FROM dw_reporting_schema.stg_intakes_outcomes_austin
	UNION
	SELECT DISTINCT breed
	FROM dw_reporting_schema.stg_intakes_outcomes_bloomington
	UNION
	SELECT DISTINCT breed
	FROM dw_reporting_schema.stg_intakes_outcomes_dallas
	UNION
	SELECT DISTINCT breed
	FROM dw_reporting_schema.stg_intakes_outcomes_norfolk
	UNION
	SELECT DISTINCT breed
	FROM dw_reporting_schema.stg_intakes_outcomes_sonoma
)
INSERT INTO dw_reporting_schema.dim_animal_breed (animal_breed)
SELECT DISTINCT
	breed
FROM CTE_ANIMAL_BREED

