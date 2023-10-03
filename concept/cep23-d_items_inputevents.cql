profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_items.csv" AS row
call {
    with row
    with row
    where row.linksto = "inputevents"
    match (rs:RefSet {id:2101})
    merge (drug:LocalConcept {id:rs.code + "|" + row.itemid})
        on create
        set drug:Drug:CATEGORIZED,
            drug.category = row.category,
            drug.code = row.itemid,
            drug.name = row.label,
            drug.source = "dataset::mimiciv/2.2/icu/d_items"
}