PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/DOMAIN.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MERGE (d:Domain {domainId:row.domain_id})
        ON CREATE
        SET d:Concept,
            d.cepId = toInteger(row.domain_concept_id),
            d.domainName = row.domain_name
}
