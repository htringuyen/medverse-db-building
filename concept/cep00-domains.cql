profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/local_domains.csv" AS row
call {
    with row
    merge (d:LocalDomain {id: toInteger(row.domain_id)})
    set d.code = row.domain_code,
        d.name = row.domain_name,
        d.reference = row.reference
}