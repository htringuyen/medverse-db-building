profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_pers_timeline.csv" AS row
call {
    with row
    match (rs:RefSet {id:1102})
    merge (lc:LocalConcept {id:rs.code + '|' + row.code})
        on create
        set lc:Item,
            lc.code = row.code,
            lc.name = row.name,
            lc.source = "custom::otmv/pers/timeline"
    merge (lc)-[:BELONGS_TO]->(rs)
}