PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/DOMAIN.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MERGE (n:Domain {domainId: row.domain_id})
        ON CREATE
        SET n:Concept,
            n.id = toInteger(row.domain_concept_id),
            n.code = 'N/A',
            n.name = 'N/A',
            n.validStartDate = date(),
            n.validEndDate = date(),
            n.domainName = row.domain_name
}
