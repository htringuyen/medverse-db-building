profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_orgtests.csv" AS row
call {
    with row
    with row
    where row.test_itemid is not null and row.test_itemid <> ''
          and row.test_name is not null and row.test_name <> ''

    // get lookup value supplier
    match (rs:RefSet {id:1706})
    merge (lc:LocalConcept {id:rs.code + '|' + row.test_itemid})
    set lc:Item,
        lc.code = row.test_itemid,
        lc.name = row.test_name,
        lc.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (lc)-[:BELONGS_TO]->(rs)
}