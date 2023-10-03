match (rxad:RxAdmin)-[:RXAD_CHARTED]->(chartAnc)
where rxad.hadmId is not null
call {
    with rxad, chartAnc
    match (vis:Visit {id:toString(rxad.hadmId)})-[:HAS_STOP]->(stop),
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
where rxad.hadmId is not null
return rxad, chartAnc
"
,
"
match (vis:Visit {id:toString(rxad.hadmId)})-[:HAS_STOP]->(stop),
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