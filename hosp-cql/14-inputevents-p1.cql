:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/inputevents_rxad_eras.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    create (rxad:RxAdmin {id: row.orderid})

    foreach (ignore in case when row.orderid = row.linkorderid then [1] else [] end |
        create (era:DrugEra {id: row.orderid})
        create (era)-[:INCLUDES]->(rxad)
        create (pa)-[:HAS_DRUG_ERA]->(era)
        set era.subjectId = toInteger(row.subject_id)
    )

    with row, rxad
    where row.linkorderid <> row.orderid
    match (era)-[:INCLUDES]->(prevRxad: RxAdmin {id: row.linkorderid})
    create (prevRxad)-[:HAS_CONT]->(rxad)
    create (era)-[:INCLUDES]->(rxad)
}