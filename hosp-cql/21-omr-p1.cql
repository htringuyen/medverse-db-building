:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/omr_items.csv" AS row
call {
    with row
    match (rs:RefSet {id:1701})
    merge (lc:LocalConcept {id:rs.code + "|" + row.result_name})
        on create
        set lc.code = rs.code + "|" + row.result_name,
            lc.name = row.result_name,
            lc.source = "dataset::mimiciv/2.2/hosp/omr"
}