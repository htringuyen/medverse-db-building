profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_organs.csv" AS row
call {
    with row
    with row
    where row.org_itemid is not null and row.org_itemid <> '' and row.org_name is not null and row.org_name <> ''

    // get lookup value supplier
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
}