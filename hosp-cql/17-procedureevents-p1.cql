:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/procedureevents_eras.csv" AS row
call {
    with row
    match (pa:Patient {id: toInteger(row.subject_id)})
    with row, pa
    merge (era:ProcedureEra {id:row.linkorderid})
    merge (pa)-[:HAS_PROC_ERA]->(era)
}