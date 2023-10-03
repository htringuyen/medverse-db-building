profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_services.csv" AS row
call {
    with row
    match (rs:RefSet {id:1302})
    merge (lc:LocalConcept {id:rs.code + '|' + row.code})
        on create
        set lc:Service,
            lc.code = row.code,
            lc.name = row.name,
            lc.source = "dataset::mimiciv/2.2/hosp/d_services"
    merge (lc)-[:BELONGS_TO]->(rs)
}