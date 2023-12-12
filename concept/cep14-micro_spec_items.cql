profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_spec_items.csv" AS row
call {
    with row
    with row,
        case when row.spec_type_desc is not null and row.spec_type_desc <> '' then row.spec_type_desc else "N/A" end as spec_type_desc

    match (rs:RefSet {id:1710})
    merge (lc:LocalConcept {id:rs.code + '|' + row.spec_itemid})
    set lc:Item,
        lc.code = row.spec_itemid,
        lc.name = spec_type_desc,
        lc.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (lc)-[:BELONGS_TO]->(rs)
}