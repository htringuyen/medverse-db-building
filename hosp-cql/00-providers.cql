:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/provider.csv" AS row
call {
    with row
    merge (:Provider {id:row.provider_id})
}