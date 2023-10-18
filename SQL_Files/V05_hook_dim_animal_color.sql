CREATE TABLE IF NOT EXISTS target_schema.dim_color
(
	color_id SERIAL PRIMARY KEY,
	animal_color TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_animal_color ON target_schema.dim_color (color_id);
WITH CTE_ANIMAL_COLOR AS
(
	SELECT DISTINCT color
	FROM target_schema.stg_intakes_outcomes_austin
	UNION
	SELECT DISTINCT color
	FROM target_schema.stg_intakes_outcomes_bloomington
	UNION
	SELECT DISTINCT color
	FROM target_schema.stg_intakes_outcomes_dallas
	UNION
	SELECT DISTINCT color
	FROM target_schema.stg_intakes_outcomes_norfolk
	UNION
	SELECT DISTINCT color
	FROM target_schema.stg_intakes_outcomes_sonoma
)
INSERT INTO target_schema.dim_color (animal_color)
SELECT DISTINCT
	color
FROM CTE_ANIMAL_COLOR
ON CONFLICT (animal_color) DO NOTHING;
