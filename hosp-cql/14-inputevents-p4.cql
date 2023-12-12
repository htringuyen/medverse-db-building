:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/inputevents_rxad_main.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row,
        datetime({epochseconds:apoc.date.parse(row.starttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as starttime,
        datetime({epochseconds:apoc.date.parse(row.endtime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as endtime

    match (rxad:RxAdmin {id:row.orderid})

    match (startAnc:Anchor {id:row.subject_id + '-' + starttime.epochSeconds})
    match (endAnc:Anchor {id:row.subject_id + '-' + endtime.epochSeconds})

    match (adIRs:RefSet {id:2104})
    match (adItem:LocalConcept {id:adIRs.code + '|' + row.ordercategorydescription})

    match (flagRs:RefSet {id:2401})
    match (adFlag:LocalConcept {id:flagRs.code + '|' + row.statusdescription})

    with row, starttime, rxad, adItem, adFlag, startAnc, endAnc
    set rxad.patientWeight = toFloat(row.patientweight),
        rxad.valueFlag = adFlag.value
    create (rxad)-[:OF_ITEM]->(adItem)

    with row, rxad, starttime, startAnc, endAnc,
        case when startAnc.value + duration({seconds:60}) >= endAnc.value then false else true end as isDurative

    foreach (ignore in case when not isDurative then [1] else [] end |
        create (rxad)-[:CHARTED]->(startAnc)
    )

    foreach (ignore in case when isDurative then [1] else [] end |
        create (rxad)-[:STARTED]->(startAnc)
        create (rxad)-[:ENDED]->(endAnc)
    )

    with row, rxad, starttime

    match (vis)-[:HAS_STOP]->(stop:Stop {id:row.stay_id}),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)

    with row, rxad, starttime, vis, stop, stopStarted, stopEnded
    foreach (ignore in case when stopStarted.value <= starttime < stopEnded.value then [1] else [] end |
        create (stop)-[:HAS_ADMIN]->(rxad)
    )

    with row, rxad, starttime, vis
    where not (stopStarted.value <= starttime < stopEnded.value)
    match (vis:Visit {id:row.hadm_id})-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    where stopStarted.value <= starttime < stopEnded.value
    create (stop)-[:HAS_ADMIN]->(rxad)
}













