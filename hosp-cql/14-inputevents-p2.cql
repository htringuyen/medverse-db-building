:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/inputevents_eras_root.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})

    with row, pa
    create (rxad:RxAdmin {id: row.orderid})
    create (era:DrugEra {id: "I-" + row.orderid})
    create (era)-[:INCLUDES]->(rxad)
    create (pa)-[:HAS_DRUG_ERA]->(era)
}
