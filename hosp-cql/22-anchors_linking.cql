:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/patients.csv" AS row
call {
    with row
    match (anc:Anchor)
    where anc.id starts with row.subject_id + '-'
    with row, anc
    order by anc.id
    with row, collect(anc) as anchors
    call apoc.nodes.link(anchors, 'NEXT')
    with row, head(anchors) as first_anchor, last(anchors) as last_anchor
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row, pa, first_anchor, last_anchor
    create (tl:Timeline {id:toInteger(row.subject_id)})
    create (pa)-[:HAS_TIMELINE]->(tl)
    create (tl)-[:HEAD]->(first_anchor)
    create (tl)-[:TAIL]->(last_anchor)
} in transactions of 500 rows