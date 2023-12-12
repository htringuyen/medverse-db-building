profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/refsets.csv" AS row
call {
    with row
    merge (rs:RefSet {id: toInteger(row.id)})
    set rs.code = row.code,
        rs.name = row.name,
        rs.reference = row.reference

    with rs, row
    call apoc.create.setLabels(rs, ["RefSet", row.label]) yield node

    with row, rs
    match (dom:LocalDomain {id:toInteger(row.domain_id)}),
          (cls:LocalClass {id:toInteger(row.class_id)})
    merge (rs)-[:OF_DOMAIN]->(dom)
    merge (rs)-[:OF_CLASS]->(cls)
}