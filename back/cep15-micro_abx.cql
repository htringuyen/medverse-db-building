profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_abx.csv" AS row
call {
    with row
    with row
    where row.ab_itemid is not null and row.ab_itemid <> '' and row.ab_name is not null and row.ab_name <> ''

    match (rs:RefSet {id:1708})
    merge (lc:LocalConcept {id:rs.code + '|' + row.ab_itemid})
    set lc:Item,
        lc.code = row.ab_itemid,
        lc.name = row.ab_name,
        lc.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (lc)-[:BELONGS_TO]->(rs)
}