profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_labitems_ref_ranges.csv" AS row
call {
    with row
    match (rs:RefSet {id: 1704})
    match (lc:LocalConcept {id: rs.code + '|' + row.itemid})
    set lc.ref_range_lower = toFloat(row.ref_range_lower),
        lc.ref_range_upper = toFloat(row.ref_range_upper)
}