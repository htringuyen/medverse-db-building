:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT.csv" AS row
FIELDTERMINATOR '\t'
WITH row
CALL {
    WITH row
    MERGE (c:Concept {id:toInteger(row.concept_id)})
    SET c.name = row.concept_name,
        c.code = row.concept_code,
        c.validStartDate = date(row.valid_start_date),
        c.validEndDate = date(row.valid_end_date)
    FOREACH(ignore IN CASE WHEN row.invalid_reason IS NOT NULL AND row.invalid_reason <> '' THEN [1] ELSE [] END |
        SET c.invalidReason = row.invalid_reason
    )

    WITH row, c,
        CASE 
            WHEN row.standard_concept = 'S' THEN "STANDARD"
            WHEN row.standard_concept = 'C' THEN "CLASSIFICATION"
            ELSE "NON_STD" END AS cepType
    CALL apoc.create.addLabels(c, [cepType]) YIELD node

    WITH row, c
    MATCH (vocabulary:Vocabulary {vocabId:row.vocabulary_id})
    CREATE (c)-[:OF_VOCAB]->(vocabulary)

    WITH row, c
    MATCH (domain:Domain {domainId:row.domain_id})
    CREATE (c)-[:OF_DOMAIN]->(domain)

    WITH row, c
    MATCH (class:ConceptClass {classId:row.concept_class_id})
    CREATE (c)-[:OF_CLASS]->(class)
    
} IN TRANSACTIONS OF 5000 ROWS
