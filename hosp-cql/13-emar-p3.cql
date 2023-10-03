:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/emar_eras.csv" AS row
call {
    with row
    create (rxad:RxAdmin:INSTANT {id: row.emar_id})
    with row, rxad
    where row.pharmacy_id is not null and row.pharmacy_id <> ""
    match (era:DrugEra {id: row.pharmacy_id})
    create (era)-[:INCLUDES]->(rxad)
}