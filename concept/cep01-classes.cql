profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/local_classes.csv" AS row
call {
    with row
    merge (cls:LocalClass {id: toInteger(row.class_id)})
    set cls.code = row.class_code,
        cls.name = row.class_name,
        cls.reference = row.reference
}