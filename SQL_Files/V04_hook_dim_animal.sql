CREATE TABLE IF NOT EXISTS target_schema.dim_animal
(
	animal_id TEXT PRIMARY KEY,
	sex TEXT,
	type TEXT,
	dob TEXT
);
CREATE INDEX IF NOT EXISTS idx_animal ON target_schema.dim_animal (animal_id);

WITH CTE_COMBINED_DATA AS
(
	SELECT DISTINCT animal_id, type, sex, date_of_birth
	FROM target_schema.stg_intakes_outcomes_austin
	UNION
	SELECT DISTINCT id, type, sex, date_of_birth
	FROM target_schema.stg_intakes_outcomes_bloomington
	UNION
	SELECT DISTINCT animal_id, type, sex, date_of_birth
	FROM target_schema.stg_intakes_outcomes_dallas
	UNION
	SELECT DISTINCT animal_id, type, sex, date_of_birth
	FROM target_schema.stg_intakes_outcomes_norfolk
	UNION
	SELECT DISTINCT animal_id, type, sex, date_of_birth
	FROM target_schema.stg_intakes_outcomes_sonoma
)
INSERT INTO target_schema.dim_animal
SELECT *
FROM CTE_COMBINED_DATA
ON CONFLICT (animal_id) DO UPDATE
SET
	sex = EXCLUDED.sex
	