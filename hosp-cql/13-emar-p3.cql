:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/emar_eras.csv" AS row
with row
limit 20000
call {
    with row
    with row,
        toInteger(split(row.emar_id, '-')[0]) as subject_id
    match (pa:Patient {id: subject_id})

    with row, pa,
         datetime({epochseconds: toInteger(row.chartepoch)}) - pa.shiftedYears as charttime

    create (rxad:RxAdmin {id: row.emar_id})

    with row, rxad, charttime
    //where row.pharmacy_id is not null and row.pharmacy_id <> ""
    match (era:DrugEra {id: row.pharmacy_id})
    with rxad, era
    create (era)-[:INCLUDES]->(rxad)
} in transactions of 5000 rows
