profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_micro_organs.csv" AS row
call {
    with row
    with row
    where row.interpretation is not null and row.interpretation <> ''

    // get lookup value supplier
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    match (rs:RefSet {id:1709})
    merge (lc:LocalConcept {id:rs.code + '|' + row.interpretation})
    set lc:TextParam:Lookup,
        lc.value = lookupValue,
        lc.code = row.interpretation,
        lc.name = row.interpretation,
        lc.source = "dataset::mimiciv/2.2/hosp/microbiologyevents"
    merge (lc)-[:BELONGS_TO]->(rs)
}