profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_drug_pharmacy_flags.csv" AS row
call {
    with row
    match (lvs:LookupValueSupplier {id:'otmv'})

    // create drug
    match (rs:RefSet {id:2105})
    merge (item:LocalConcept {id:rs.code + '|' + row.proc_type})
    set item:Item,
        item.code = row.proc_type,
        item.name = row.proc_type,
        item.source = "dataset::mimiciv/2.2/hosp/pharmacy"
    merge (item)-[:BELONGS_TO]->(rs)

    // create associated flag
    with row, item, lvs
    match (rs:RefSet {id:2401})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (lc:LocalConcept {id:rs.code + '|' + row.status})
    set lc:Flag:CATEGORIZED:Lookup,
        lc.value = lookupValue,
        lc.category = "Medication",
        lc.flagDescription = "Medication Status",
        lc.code = row.status,
        lc.name = row.status,
        lc.source = "dataset::mimiciv/2.2/hosp/pharmacy"
    merge (lc)-[:BELONGS_TO]->(rs)
    merge (item)-[:HAS_POSS_FLAG]->(lc)
}