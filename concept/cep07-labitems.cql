profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_labitems.csv" AS row
call {
    with row
    match (rs:RefSet {id:1704})
    merge (lc:LocalConcept {id: rs.code + '|' + trim(row.itemid)})
        set lc:Item:CATEGORIZED,
            lc.code = trim(row.itemid),
            lc.name = row.label + '|' + row.fluid + '|' + row.category,
            lc.source = "dataset::mimiciv/2.2/hosp/d_labitems",
            lc.category = row.category
    merge (lc)-[:BELONGS_TO]->(rs)

    with row
    match (rs:RefSet {id:1703})
    merge (lc:LocalConcept {id: rs.code + '|' + trim(row.fluid)})
        set lc:Item,
            lc.code = trim(row.fluid),
            lc.name = trim(row.fluid),
            lc.source = "dataset::mimiciv/2.2/hosp/d_labitems"
    merge (lc)-[:BELONGS_TO]->(rs)
}