profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_abx.csv" AS row
call {
    with row
    with row
    where row.ab_itemid is not null and row.ab_itemid <> '' and row.ab_name is not null and row.ab_name <> ''

    match (rs:RefSet {id:1708})
    merge (item:LocalConcept {id:rs.code + '|' + row.ab_itemid})
    set item:Item,
        item.code = row.ab_itemid,
        item.name = row.ab_name,
        item.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (item)-[:BELONGS_TO]->(rs)

    // link item to possible param
    with row, item
    where row.interpretation is not null and row.interpretation <> ''
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    match (rs:RefSet {id:1709})
    merge (lc:LocalConcept {id:rs.code + '|' + row.interpretation})
        on create
        set lc:TextParam:Lookup,
            lc.value = lookupValue,
            lc.code = row.interpretation,
            lc.name = row.interpretation,
            lc.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (lc)-[:BELONGS_TO]->(rs)
    merge (item)-[:ASSOC_PARAM]->(lc)
}