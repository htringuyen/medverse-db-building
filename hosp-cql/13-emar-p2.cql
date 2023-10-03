:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/emar_pharmacies.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row, pa
    create (era:DrugEra {id:row.pharmacy_id})
    create (pa)-[:HAS_DRUG_ERA]->(era)

} in transactions of 5000 rows