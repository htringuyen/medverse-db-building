PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/VOCABULARY.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MERGE (n:Vocabulary {vocabId: row.vocabulary_id})
        ON CREATE
        SET n:Concept,
            n.id = toInteger(row.vocabulary_concept_id),
            n.code = 'N/A',
            n.name = 'N/A',
            n.validStartDate = date(),
            n.validEndDate = date(),
            n.vocabName = row.vocabulary_name,
            n.vocabReference = row.vocabulary_reference,
            n.vocabVersion = row.vocabulary_version
}
