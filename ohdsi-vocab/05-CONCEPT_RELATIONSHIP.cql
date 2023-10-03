:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT_RELATIONSHIP.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MATCH (c1:Concept {cepId:toInteger(row.concept_id_1)}),
        (c2:Concept {cepId:toInteger(row.concept_id_2)})
    WITH row, c1, c2
    CALL apoc.create.relationship(c1, row.relationship_id, 
        {validStartDate:date(row.valid_start_date), validEndDate:date(row.valid_end_date)}, c2)
        YIELD rel AS matchedRel
    WITH row, matchedRel
    FOREACH(ignore IN CASE WHEN row.invalid_reason IS NOT NULL AND row.invalid_reason <> '' THEN [1] ELSE [] END |
        SET matchedRel.invalidReason = row.invalid_reason
    )
} IN TRANSACTIONS OF 5000 ROWS
