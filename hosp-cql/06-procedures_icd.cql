:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/procedures_icd.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    match (vis:Visit {id:row.hadm_id})-[:VIS_STARTED]->(visStartAnc)

    with row, vis,
        case when row.icd_version = '9' then 'V09-' + row.icd_code else 'V10-' + row.icd_code end as proc_code,
         date(row.chartdate) - pa.shiftedYears as chartdate

    match (rs:RefSet {id:1903})
    match (lc:LocalConcept {id:rs.code + '|' + proc_code})

    create (proc:Procedure:BilledProc:EXISTENT)
        set proc.priority = toInteger(row.seq_num),
            proc.chartDate = chartdate
    create (proc)-[:OF_ITEM]->(lc)
    create (vis)-[:HAS_PROC]->(proc)
} in transactions of 5000 rows