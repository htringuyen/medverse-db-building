profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_labevents_comments.csv" AS row
call {
    with row
    with row where row.comments is not null and row.comments <> ''
    // get lookup value supplier
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    merge (cmt:RefComment {idText: row.comments})
        on create
        set cmt:Lookup,
            cmt.value = lookupValue

    with cmt, row
    match (rs:RefSet {id: 1704})
    match (item:LocalConcept {id:rs.code + '|' + row.itemid})
    merge (item)-[:HAS_POSS_CMT]->(cmt)
}