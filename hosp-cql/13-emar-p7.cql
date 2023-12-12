match (rxad:RxAdmin)-[:CHARTED]->(chartAnc)
where rxad.hadmId is null
call {
    with rxad, chartAnc
    match (pa:Patient {id:rxad.subjectId})-[:HAS_VISIT]->(vis)-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    where stopStarted.value <= chartAnc.value and stopEnded.value > chartAnc.value

    with rxad, stop
    match (rs:RefSet {id:2101})
    with rxad, stop
    create (stop)-[:HAS_ADMIN]->(rxad)
    //remove rxad.hadmId, rxad.subjectId
}

;

call apoc.periodic.iterate(
"
match (rxad:RxAdmin)-[:CHARTED]->(chartAnc)
where rxad.hadmId is null
return rxad, chartAnc
"
,
"
with rxad, chartAnc
match (pa:Patient {id:rxad.subjectId})-[:HAS_VISIT]->(vis)-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
where stopStarted.value <= chartAnc.value and stopEnded.value > chartAnc.value

with rxad, stop
match (rs:RefSet {id:2101})
with rxad, stop
create (stop)-[:HAS_ADMIN]->(rxad)
//remove rxad.hadmId, rxad.subjectId
"
,
{batchSize:5000, parallel:true, concurrency:10, retries:5}
)