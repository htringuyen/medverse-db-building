match (rxad:RxAdmin)-[:RXAD_CHARTED]->(chartAnc)
where rxad.hadmId is null
call {
    with rxad, chartAnc
    match (pa:Patient {id:rxad.subjectId})-[:HAS_CASE]->(ca)-[:HAS_VIS]->(vis)-[:HAS_STOP]->(stop),
          (stop)-[:STOP_STARTED]->(stopStarted),
          (stop)-[:STOP_ENDED]->(stopEnded)
    where stopStarted.value <= chartAnc.value and stopEnded.value > chartAnc.value

    with rxad, stop
    limit 1
    create (stop)-[:HAS_RXAD]->(rxad)
    remove rxad.hadmId, rxad.subjectId
}

;

call apoc.apoc.periodic.iterate(
"
match (rxad:RxAdmin)-[:RXAD_CHARTED]->(chartAnc)
where rxad.hadmId is null
return rxad, chartAnc
"
,
"
match (pa:Patient {id:rxad.subjectId})-[:HAS_CASE]->(ca)-[:HAS_VIS]->(vis)-[:HAS_STOP]->(stop),
          (stop)-[:STOP_STARTED]->(stopStarted),
          (stop)-[:STOP_ENDED]->(stopEnded)
where stopStarted.value <= chartAnc.value and stopEnded.value > chartAnc.value

with rxad, stop
limit 1
create (stop)-[:HAS_RXAD]->(rxad)
remove rxad.hadmId, rxad.subjectId
"
,
{batchSize:5000, parallel:true, concurrency:10}
)