CREATE TABLE IF NOT EXISTS target_schema.agg_daily
(
	date DATE PRIMARY KEY,
	total_intakes INT,
	total_outcomes INT,
	total_stray_intakes INT,
	total_abandoned_intakes INT,
	total_adoption_return_intakes INT,
	total_biting_intakes INT,
	total_behavioral_issues_intakes INT,
	total_owner_surrender_intakes INT,
	total_foster_intakes INT,
	total_sick_injured_intakes INT,
	total_wildlife_intakes INT,
	total_adoptions_outcomes INT,
	total_deaths_outcomes INT,
	total_euthanasia_outcomes INT,
	total_foster_outcomes INT,
	total_transfers_outcomes INT,
	total_treatments_outcomes INT,
	total_rtos_outcomes INT,
	total_return_native_habitat_outcomes INT
);
CREATE INDEX IF NOT EXISTS idx_agg_daily ON target_schema.agg_daily (date);

CREATE TEMPORARY TABLE temp_agg AS
WITH CTE_TOTAL_INTAKES AS
(
	SELECT 
		CAST(fct.intake_date AS DATE) intake_date,
		COUNT(fct.animal_id) total_intakes
	FROM target_schema.fct_intakes_outcomes fct
	GROUP BY CAST(intake_date AS DATE)
	ORDER BY CAST(fct.intake_date AS DATE) DESC
),
CTE_TOTAL_OUTCOMES AS
(
	SELECT
		CAST(fct.outcome_date AS DATE) outcome_date,
		COUNT(fct.animal_id) total_outcomes
	FROM target_schema.fct_intakes_outcomes fct
	WHERE NOT LOWER(fct.outcome_type) ILIKE '%pending%'
	GROUP BY CAST(outcome_date AS DATE)
	ORDER BY CAST(fct.outcome_date AS DATE) DESC
),
CTE_TOTAL_OUTCOMES_TYPES AS
(
	SELECT
		CAST(fct.outcome_date AS DATE) outcome_date,
		COUNT(CASE WHEN fct.outcome_type = 'Adoption' THEN fct.animal_id END) total_adoptions_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Transfer' THEN fct.animal_id END) total_transfers_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Died' THEN fct.animal_id END) total_deaths_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Euthanize' THEN fct.animal_id END) total_euthanasia_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Foster' THEN fct.animal_id END) total_foster_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Treatment' THEN fct.animal_id END) total_treatments_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Return to Owner' THEN fct.animal_id END) total_rtos_outcomes,
		COUNT(CASE WHEN fct.outcome_type = 'Returned to Native Habitat' THEN fct.animal_id END) total_return_native_habitat_outcomes
	FROM target_schema.fct_intakes_outcomes fct
	GROUP BY CAST(outcome_date AS DATE)
),
CTE_TOTAL_INTAKES_TYPES AS
(
	SELECT
		CAST(fct.intake_date AS DATE) intake_date,
		COUNT(CASE WHEN fct.intake_type = 'Stray' THEN fct.animal_id END) total_stray_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Abandoned' THEN fct.animal_id END) total_abandoned_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Adoption Return' THEN fct.animal_id END) total_adoption_return_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Biting' THEN fct.animal_id END) total_biting_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Behavioral Issues' THEN fct.animal_id END) total_behavioral_issues_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Owner Surrender' THEN fct.animal_id END) total_owner_surrender_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Foster' THEN fct.animal_id END) total_foster_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Sick/Injured' THEN fct.animal_id END) total_sick_injured_intakes,
		COUNT(CASE WHEN fct.intake_type = 'Wildlife' THEN fct.animal_id END) total_wildlife_intakes
	FROM target_schema.fct_intakes_outcomes fct
	GROUP BY CAST(intake_date AS DATE)
)
SELECT
	CTE_TOTAL_INTAKES.intake_date date,
	CTE_TOTAL_INTAKES.total_intakes,
	COALESCE(CTE_TOTAL_OUTCOMES.total_outcomes,0) total_outcomes,
	CTE_TOTAL_INTAKES_TYPES.total_stray_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_abandoned_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_adoption_return_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_biting_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_behavioral_issues_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_owner_surrender_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_foster_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_sick_injured_intakes,
	CTE_TOTAL_INTAKES_TYPES.total_wildlife_intakes,
	CTE_TOTAL_OUTCOMES_TYPES.total_adoptions_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_deaths_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_euthanasia_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_foster_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_transfers_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_treatments_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_rtos_outcomes,
	CTE_TOTAL_OUTCOMES_TYPES.total_return_native_habitat_outcomes
FROM CTE_TOTAL_INTAKES 
LEFT OUTER JOIN CTE_TOTAL_OUTCOMES
	ON CTE_TOTAL_INTAKES.intake_date = CTE_TOTAL_OUTCOMES.outcome_date
LEFT OUTER JOIN CTE_TOTAL_OUTCOMES_TYPES 
	ON CTE_TOTAL_INTAKES.intake_date = CTE_TOTAL_OUTCOMES_TYPES.outcome_date 
LEFT OUTER JOIN CTE_TOTAL_INTAKES_TYPES
	ON CTE_TOTAL_INTAKES.intake_date = CTE_TOTAL_INTAKES_TYPES.intake_date;

CREATE INDEX IF NOT EXISTS idx_temp_agg ON temp_agg (date);

INSERT INTO target_schema.agg_daily
SELECT *
FROM temp_agg
WHERE temp_agg.date NOT IN 
(
	SELECT
		date
	FROM target_schema.agg_daily
)
ORDER BY temp_agg.date DESC;

UPDATE target_schema.agg_daily
SET
	date = subquery.date,
	total_intakes = subquery.total_intakes,
	total_outcomes = subquery.total_outcomes,
	total_stray_intakes = subquery.total_stray_intakes,
	total_abandoned_intakes = subquery.total_abandoned_intakes,
	total_adoption_return_intakes = subquery.total_adoption_return_intakes,
	total_biting_intakes = subquery.total_biting_intakes,
	total_behavioral_issues_intakes = subquery.total_behavioral_issues_intakes,
	total_owner_surrender_intakes = subquery.total_owner_surrender_intakes,
	total_foster_intakes = subquery.total_foster_intakes,
	total_sick_injured_intakes = subquery.total_sick_injured_intakes,
	total_wildlife_intakes = subquery.total_wildlife_intakes,
	total_adoptions_outcomes = subquery.total_adoptions_outcomes,
	total_deaths_outcomes = subquery.total_deaths_outcomes,
	total_euthanasia_outcomes = subquery.total_euthanasia_outcomes,
	total_foster_outcomes = subquery.total_foster_outcomes,
	total_transfers_outcomes = subquery.total_transfers_outcomes,
	total_treatments_outcomes = subquery.total_treatments_outcomes,
	total_rtos_outcomes = subquery.total_rtos_outcomes,
	total_return_native_habitat_outcomes = subquery.total_return_native_habitat_outcomes
FROM (
	SELECT *
	FROM temp_agg
	) subquery
WHERE subquery.date = agg_daily.date


