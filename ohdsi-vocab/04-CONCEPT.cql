:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT.csv" AS row
FIELDTERMINATOR '\t'
WITH row
CALL {
    WITH row
    WITH row
    WHERE EXISTS{ (:Concept {cepId:toInteger(row.concept_id)}) } OR 
        (row.vocabulary_id <> "Vocabulary" AND row.vocabulary_id <> "Domain" AND row.vocabulary_id <> "Concept Class" AND row.vocabulary_id <> "Relationship")

    MERGE (c:Concept {cepId:toInteger(row.concept_id)})
    SET c.cepName = row.concept_name,
        c.cepCode = row.concept_code,
        c.validStartDate = date(row.valid_start_date),
        c.validEndDate = date(row.valid_end_date)
    FOREACH(ignore IN CASE WHEN row.invalid_reason IS NOT NULL AND row.invalid_reason <> '' THEN [1] ELSE [] END |
        SET c.invalidReason = row.invalid_reason
    )

    WITH row, c,
        CASE 
            WHEN row.standard_concept = 'S' THEN "STDCEP" 
            WHEN row.standard_concept = 'C' THEN "CLFCEP"
            ELSE "STDNON" END AS stdStatus
    CALL apoc.create.setLabels(c, ["Concept", row.vocabulary_id, stdStatus]) YIELD node

    WITH row, c
    MATCH (vocabulary:Vocabulary {vocId:row.vocabulary_id})
    CREATE (c)-[:OF_VOC]->(vocabulary)

    WITH row, c
    MATCH (domain:Domain {domainId:row.domain_id})
    CREATE (c)-[:OF_DOMAIN]->(domain)

    WITH row, c
    MATCH (class:ConceptClass {classId:row.concept_class_id})
    CREATE (c)-[:OF_CLASS]->(class)
    
} IN TRANSACTIONS OF 5000 ROWS
