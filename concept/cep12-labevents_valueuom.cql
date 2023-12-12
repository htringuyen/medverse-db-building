profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_labevents_valueuom.csv" AS row
call {
    with row
    with row where row.valueuom is not null and trim(row.valueuom) <> ''
    // get lookup value supplier
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    match (rs:RefSet {id:2201})
    merge (lc:LocalConcept {id: rs.code + '|' + trim(row.valueuom)})
        on create
        set lc:Unit:Lookup,
            lc.value = lookupValue,
            lc.code = trim(row.valueuom),
            lc.name = trim(row.valueuom),
            lc.source = "dataset::mimiciv/2.2/hosp/d_labitems"
    merge (lc)-[:BELONGS_TO]->(rs)

    with lc, row
    match (rs:RefSet {id: 1704})
    match (item:LocalConcept {id:rs.code + '|' + row.itemid})
    merge (item)-[:ASSOC_UNIT]->(lc)
}