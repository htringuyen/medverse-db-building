:auto
profile
match (ms:MicroSampling)-[:CHARTED]->(chartedAnc)
where not exists (()-[:HAS_MEAS]->(ms))
call {
    with ms, chartedAnc
    optional match (pa:Patient {id:ms.subjectId})-[:HAS_VISIT]->()-[:HAS_STOP]->(stop),
                   (stop)-[:STARTED]->(startAnc),
                   (stop)-[:ENDED]->(endAnc)
    where (startAnc.value <= chartedAnc.value < endAnc.value)
        or ((not ms.hasChartTime) and ( date({date:startAnc.value}) = date({date:chartedAnc.value})
                    or date({date:endAnc.value}) = date({date:chartedAnc.value}) ) )
/// ----------------
    with ms, chartedAnc, stop

    limit 1 // check point

    foreach (ignore in case when stop is not null then [1] else [] end |
        create (stop)-[:HAS_MEAS]->(ms)
    )

    with ms, chartedAnc
    where stop is null

    match (pa:Patient {id: ms.subjectId})

    with ms, chartedAnc, pa

    // create visit and stop
    merge (vis:Visit {id:ms.subjectId + "-" + chartedAnc.value.epochSeconds})
      on create
      set vis:OUTPATIENT,
          vis.newCreated = true
    merge (stop:Stop {id:ms.subjectId + "-" + chartedAnc.value.epochSeconds})
        on create
        set stop:NonStay

    // link to meas
    merge (stop)-[:HAS_MEAS]->(ms)

    with ms, chartedAnc, pa, vis, stop
    where vis.newCreated is not null

    // create start and end anchors
    match (startAnc:Anchor {id:ms.subjectId + "-" + chartedAnc.value.epochSeconds})

    merge (endAnc:Anchor {id:ms.subjectId + "-" + (chartedAnc.value.epochSeconds + 60)})
      on create
      set endAnc.value = chartedAnc.value + duration({seconds: 60})

    with ms, vis, stop, startAnc, endAnc, pa

    remove vis.newCreated
    create (pa)-[:HAS_VISIT]->(vis)
    create (vis)-[:HAS_STOP]->(stop)
    create (vis)-[:STARTED]->(startAnc)
    create (vis)-[:ENDED]->(endAnc)
    create (stop)-[:STARTED]->(startAnc)
    create (stop)-[:ENDED]->(endAnc)
} in transactions of 5000 rows