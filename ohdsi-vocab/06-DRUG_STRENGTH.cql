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
        MATCH (ingt:Concept {cepId:toInteger(row.ingredient_concept_id)}),
            (amountUnit:Concept {cepId:toInteger(row.amount_unit_concept_id)})
        MERGE (ingt)-[:HAS_POSS_UNIT]->(amountUnit)
    }

    WITH row
    WHERE row.drug_concept_id <> row.ingredient_concept_id

    MATCH (drug: Concept {cepId:toInteger(row.drug_concept_id)}),
        (ingt: Concept {cepId:toInteger(row.ingredient_concept_id)})   
    WITH row, drug, ingt
    SET drug:AstdDrug
    MERGE (drug)-[:HAS_ING_STR]->(strength:IngStrength)
        ON CREATE
        SET strength.boxSize = toInteger(row.box_size),
            strength.validStartDate = date(row.valid_start_date),
            strength.validEndDate = date(row.valid_end_date),
            strength.invalidReason = row.invalid_reason
    MERGE (strength)-[:OF_INGREDIENT]->(ingt)
        ON CREATE
        SET ingt:AstdIngredient

    // set amount unit
    WITH row, strength
    OPTIONAL MATCH (amountUnit:Concept {cepId:toInteger(row.amount_unit_concept_id)})
    FOREACH(ignore IN CASE WHEN amountUnit IS NOT NULL THEN [1] ELSE [] END |
        MERGE (strength)-[amountRel:HAS_AMOUNT_UNIT]->(amountUnit)
            ON CREATE
            SET strength.amountValue = toFloat(row.amount_value)
    )

    // set numerator unit
    WITH row, strength
    OPTIONAL MATCH (numeratorUnit:Concept {cepId:toInteger(row.numerator_unit_concept_id)})
    FOREACH(ignore IN CASE WHEN numeratorUnit IS NOT NULL THEN [1] ELSE [] END |
        MERGE (strength)-[numeratorRel:HAS_NUMER_UNIT]->(numeratorUnit)
            ON CREATE
            SET strength.numerValue = toFloat(row.numerator_value)
    )

    // set denominator unit
    WITH row, strength
    OPTIONAL MATCH (denominatorUnit:Concept {cepId:toInteger(row.denominator_unit_concept_id)})
    FOREACH(ignore IN CASE WHEN denominatorUnit IS NOT NULL THEN [1] ELSE [] END |
        MERGE (strength)-[denominatorRel:HAS_DENOM_UNIT]->(denominatorUnit)
            ON CREATE
            SET strength.denomValue = toFloat(row.denominator_value)
    )
} IN TRANSACTIONS OF 2000 ROWS

