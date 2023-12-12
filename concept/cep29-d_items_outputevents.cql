profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_items.csv" AS row
call {
    with row
    with row
    where row.linksto = "outputevents"
    match (rs:RefSet {id:1701})
    merge (item:LocalConcept {id:rs.code + "|" + row.itemid})
        on create
        set item:Item:CATEGORIZED,
            item.category = row.category,
            item.code = row.itemid,
            item.name = row.label,
            item.source = "dataset::mimiciv/2.2/icu/d_items_outputevents"
    merge (item)-[:BELONGS_TO]->(rs)
}