:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT_SYNONYM.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MATCH (c:Concept {id:toInteger(row.concept_id)}),
        (l:Concept {id:toInteger(row.language_concept_id)})
    WITH c, l, row
    MERGE (s:Synonym {idText:row.concept_synonym_name})
    MERGE (c)-[:HAS_SYNONYM]->(s)
    MERGE (s)-[:IN_LANGUAGE]->(l)
} IN TRANSACTIONS OF 2000 rows
