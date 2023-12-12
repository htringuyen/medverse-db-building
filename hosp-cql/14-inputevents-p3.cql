:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/inputevents_eras_child.csv" AS row
call {
    with row
    match (era)-[:INCLUDES]->(rootRxad:RxAdmin {id:row.linkorderid})
    with era, row
    create (rxad:RxAdmin {id:row.orderid})
    create (era)-[:INCLUDES]->(rxad)
}