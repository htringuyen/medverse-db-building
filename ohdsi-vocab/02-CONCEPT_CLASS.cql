PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT_CLASS.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MERGE (c:ConceptClass {classId:row.concept_class_id})
        ON CREATE
        SET c:Concept,
            c.cepId = toInteger(row.concept_class_concept_id),
            c.className = row.concept_class_name
}
