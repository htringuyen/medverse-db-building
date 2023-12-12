:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/hcpcsevents.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    match (vis:Visit {id:row.hadm_id})

    with row, vis,
        row.hcpcs_cd as proc_code,
         datetime(row.chartdate) - pa.shiftedYears as chartdate

    match (rs:RefSet {id:1904})
    match (lc:LocalConcept {id:rs.code + '|' + proc_code})

    create (proc:Procedure:Billing:EXISTENT)
        set proc.priority = toInteger(row.seq_num),
            proc.chartDate = chartdate
    create (proc)-[:OF_ITEM]->(lc)
    create (vis)-[:HAS_PROC]->(proc)
} in transactions of 5000 rows