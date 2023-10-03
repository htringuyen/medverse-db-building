PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/VOCABULARY.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MERGE (v:Vocabulary {vocId:row.vocabulary_id})
        ON CREATE
        SET v:Concept,
            v.cepId = toInteger(row.vocabulary_concept_id),
            v.vocName = row.vocabulary_name,
            v.vocRef = row.vocabulary_reference,
            v.vocVersion = row.vocabulary_version
}
