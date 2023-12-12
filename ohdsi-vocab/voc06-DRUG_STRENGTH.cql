:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/DRUG_STRENGTH.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    CALL {
        WITH row
        WITH row
        WHERE row.drug_concept_id = row.ingredient_concept_id
        MATCH (ingt:Concept {id:toInteger(row.ingredient_concept_id)}),
            (amountUnit:Concept {id:toInteger(row.amount_unit_concept_id)})
        MERGE (ingt)-[:ASSOC_UNIT]->(amountUnit)
    }

    WITH row
    WHERE row.drug_concept_id <> row.ingredient_concept_id

    MATCH (drug: Concept {id:toInteger(row.drug_concept_id)}),
        (ingt: Concept {id:toInteger(row.ingredient_concept_id)})
    WITH row, drug, ingt
    SET drug:AStdDrug
    MERGE (drug)-[:HAS_ING_STR]->(strength:IngredientStrength)
        ON CREATE
        SET strength.boxSize = toInteger(row.box_size),
            strength.validStartDate = date(row.valid_start_date),
            strength.validEndDate = date(row.valid_end_date),
            strength.invalidReason = row.invalid_reason
    MERGE (strength)-[:OF_INGREDIENT]->(ingt)
        ON CREATE
        SET ingt:AStdIngredient

    // set amount unit
    WITH row, strength
    OPTIONAL MATCH (amountUnit:Concept {id:toInteger(row.amount_unit_concept_id)})
    FOREACH(ignore IN CASE WHEN amountUnit IS NOT NULL THEN [1] ELSE [] END |
        set strength.amountValue = toFloat(row.amount_value)
        set strength.amountUnitId = amountUnit.id
        /*MERGE (strength)-[amountRel:HAS_AMOUNT_UNIT]->(amountUnit)
            ON CREATE
            SET strength.amountValue = toFloat(row.amount_value)*/
    )

    // set numerator unit
    WITH row, strength
    OPTIONAL MATCH (numeratorUnit:Concept {id:toInteger(row.numerator_unit_concept_id)})
    FOREACH(ignore IN CASE WHEN numeratorUnit IS NOT NULL THEN [1] ELSE [] END |
        set strength.numeratorValue = toFloat(row.numerator_value)
        set strength.numeratorUnitId = numeratorUnit.id
        /*MERGE (strength)-[numeratorRel:HAS_NUMER_UNIT]->(numeratorUnit)
            ON CREATE
            SET strength.numerValue = toFloat(row.numerator_value)*/
    )

    // set denominator unit
    WITH row, strength
    OPTIONAL MATCH (denominatorUnit:Concept {id:toInteger(row.denominator_unit_concept_id)})
    FOREACH(ignore IN CASE WHEN denominatorUnit IS NOT NULL THEN [1] ELSE [] END |
        set strength.denominatorValue = toFloat(row.denominator_value)
        set strength.denominatorUnitId = denominatorUnit.id
        /*MERGE (strength)-[denominatorRel:HAS_DENOM_UNIT]->(denominatorUnit)
            ON CREATE
            SET strength.denomValue = toFloat(row.denominator_value)*/
    )
} IN TRANSACTIONS OF 2000 ROWS

