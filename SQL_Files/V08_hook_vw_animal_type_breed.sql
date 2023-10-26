CREATE OR REPLACE VIEW target_schema.animal_type_breed AS
	SELECT
		fct.animal_id,
		animal.type,
		breed.animal_breed
	FROM target_schema.dim_animal_breed breed
	INNER JOIN target_schema.fct_intakes_outcomes fct
		ON breed.breed_id = fct.breed_id
	INNER JOIN target_schema.dim_animal animal
		ON animal.animal_id = fct.animal_id;