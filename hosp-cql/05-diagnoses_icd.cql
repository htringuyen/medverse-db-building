:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/diagnoses_icd.csv" AS row
call {
    with row
    match (vis:Visit {id:row.hadm_id}),
          (rs:RefSet {id:1501})
    with row, vis, rs,
        case when row.icd_version = '9' then 'V09-' + row.icd_code else 'V10-' + row.icd_code end as dx_code

    match (item:LocalConcept {id: rs.code + '|' + dx_code})

    create (cond:Condition:HospDx:EXISTENT)
        set cond.priority = toInteger(row.seq_num)

    create (cond)-[:OF_ITEM]->(item)
    create (vis)-[:HAS_COND]->(cond)
} in transactions of 5000 rows