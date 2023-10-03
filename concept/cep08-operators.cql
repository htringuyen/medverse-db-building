profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_operators.csv" AS row
call {
    with row

    // get lookup value supplier
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    match (rs:RefSet {id:2301})
    merge (lc:LocalConcept {id:rs.code + '|' + row.code})
    set lc:Operator:Lookup,
        lc.value = lookupValue,
        lc.code = row.code,
        lc.name = row.name,
        lc.source = "custom::otmv/opr/operator"
    merge (lc)-[:BELONGS_TO]->(rs)
}