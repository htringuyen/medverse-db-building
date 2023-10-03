:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/services.csv" AS row
call {
    with row
    match (vis:Visit {id: row.hadm_id})-[:HAS_STOP]->(stop:Stop),
          (stop)-[:STOP_STARTED]->(startAnchor),
          (stop)-[:STOP_ENDED]->(endAnchor)
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row, stop,
         (datetime({epochmillis: apoc.date.parse(row.transfertime, 'ms', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears).epochSeconds as svEpoch,
        toInteger(split(startAnchor.id, '-')[1]) as startEpoch,
        toInteger(split(endAnchor.id, '-')[1]) as endEpoch
    where svEpoch >= startEpoch and svEpoch <= endEpoch
    match (rs:RefSet {id: 1302})
    with row, stop, rs
    match (lc:LocalConcept {id:rs.code + '|' + row.curr_service})
    create (stop)-[:HAS_INF]->(lc)
} in transactions of 5000 rows