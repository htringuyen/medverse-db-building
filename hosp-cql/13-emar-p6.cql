match (rxad:RxAdmin)-[:CHARTED]->(chartAnc)
where rxad.hadmId is not null
call {
    with rxad, chartAnc
    match (vis:Visit {id:toString(rxad.hadmId)})-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    where stopStarted.value <= chartAnc.value and stopEnded.value > chartAnc.value
    with rxad, vis, stop
    create (stop)-[:HAS_ADMIN]->(rxad)
    //remove rxad.hadmId, rxad.subjectId
}


;

call apoc.periodic.iterate(
"
match (rxad:RxAdmin)-[:CHARTED]->(chartAnc)
where rxad.hadmId is not null
return rxad, chartAnc
"
,
"
with rxad, chartAnc
match (vis:Visit {id:toString(rxad.hadmId)})-[:HAS_STOP]->(stop),
      (stop)-[:STARTED]->(stopStarted),
      (stop)-[:ENDED]->(stopEnded)
where stopStarted.value <= chartAnc.value and stopEnded.value > chartAnc.value
with rxad, vis, stop
create (stop)-[:HAS_ADMIN]->(rxad)
//remove rxad.hadmId, rxad.subjectId
"
,
{batchSize:5000, parallel:true, concurrency:10, retries:5}
)