PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT_CLASS.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MERGE (n:ConceptClass {classId: row.concept_class_id})
        ON CREATE
        SET n:Concept,
            n.id = toInteger(row.concept_class_concept_id),
            n.code = 'N/A',
            n.name = 'N/A',
            n.validStartDate = date(),
            n.validEndDate = date(),
            n.className = row.concept_class_name
}
