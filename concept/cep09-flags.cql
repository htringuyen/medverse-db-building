profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_flags.csv" AS row
call {
    with row
    // get lookup value supplier
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    match (rs:RefSet {id: 2401})
    merge (lc:LocalConcept {id:rs.code + '|' + row.code})
    set lc:Flag:Lookup:CATEGORIZED,
        lc.value = lookupValue,
        lc.category = row.category,
        lc.flagDescription = row.flag_description,
        lc.code = row.code,
        lc.name = row.name,
        lc.source = "custom::otmv/flag/flag"
    merge (lc)-[:BELONGS_TO]->(rs)
}