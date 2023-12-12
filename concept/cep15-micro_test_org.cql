profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_test_org.csv" AS row
call {
    with row
    with row
    where row.test_itemid is not null and row.test_itemid <> ''
          and row.test_name is not null and row.test_name <> ''

    match (rs:RefSet {id:1706})
    merge (item:LocalConcept {id:rs.code + '|' + row.test_itemid})
        on create
        set item:Item,
            item.code = row.test_itemid,
            item.name = row.test_name,
            item.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (item)-[:BELONGS_TO]->(rs)

    // link item to possible param
    with row, item
    where row.org_itemid is not null and row.org_itemid <> ''
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    match (rs:RefSet {id:1707})
    merge (lc:LocalConcept {id:rs.code + '|' + row.org_itemid})
    set lc:TextParam:Lookup,
        lc.value = lookupValue,
        lc.code = row.org_itemid,
        lc.name = row.org_name,
        lc.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (lc)-[:BELONGS_TO]->(rs)
    merge (item)-[:ASSOC_PARAM]->(lc)
}