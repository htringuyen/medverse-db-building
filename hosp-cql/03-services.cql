:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/services.csv" AS row
call {
    with row
    match (vis:Visit {id: row.hadm_id})
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row, vis,
         (datetime({epochmillis: apoc.date.parse(row.transfertime, 'ms', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears).epochSeconds as svEpoch
    match (rs:RefSet {id: 1302})
    match (lc:LocalConcept {id:rs.code + '|' + row.curr_service})
    create (visInf:VisitInfo:ServiceTransfer:EXISTENT)-[:OF_ITEM]->(lc)
    create (vis)-[:HAS_VISINF]->(visInf)
    with row, visInf, svEpoch
    merge (anc:Anchor {id:row.hadm_id + '-' + svEpoch})
        on create
        set anc.value = datetime({epochseconds: svEpoch})
    create (visInf)-[:HAPPENED]->(anc)
} in transactions of 5000 rows