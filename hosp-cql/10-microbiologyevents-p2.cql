:auto
profile
match (ms:MicroSampling)-[:MEAS_CHARTED]->(anc)
match (pa:Patient {id:ms.subjectId})
with ms, pa, anc.value as charttime
call {
    with ms, pa, charttime

    optional match (pa)-[:HAS_CASE]->()-[:HAS_VIS]->()-[:HAS_STOP]->(stop),
                   (stop)-[:STOP_STARTED]->(startAnc),
                   (stop)-[:STOP_ENDED]->(endAnc)
    where charttime >= startAnc.value and charttime < endAnc.value

    with ms, charttime, stop

    limit 1 // check point

    foreach (ignore in case when stop is not null then [1] else [] end |
        merge (stop)-[:HAS_MEAS]->(ms)
    )


    with ms, charttime
    where stop is null

    match (pa:Patient {id: ms.subjectId})-[:HAS_CASE]->(ca)

    with ms, charttime, ca
    merge (vis:Visit {id:ms.subjectId + '-' + charttime.epochSeconds})
        on create
        set vis.newCreated = true
    merge (stop:Stop {id:ms.subjectId + '-' + charttime.epochSeconds})
    merge (stop)-[:HAS_MEAS]->(ms)

    foreach (ignore in case when vis.newCreated is not null then [1] else [] end |
        remove vis.newCreated
        merge (ca)-[:HAS_VIS]->(vis)
        merge (vis)-[:HAS_STOP]->(stop)

        merge (startAnc:Anchor {id:ms.subjectId + '-' + charttime.epochSeconds})
            on create
            set startAnc.value = charttime

        merge (endAnc:Anchor {id:ms.subjectId + '-' + (charttime.epochSeconds + 60)})
            on create
            set endAnc.value = charttime + duration({seconds: 60})

        merge (vis)-[:VIS_STARTED]->(startAnc)
        merge (vis)-[:VIS_ENDED]->(endAnc)

        merge (stop)-[:STOP_STARTED]->(startAnc)
        merge (stop)-[:STOP_ENDED]->(endAnc)
    )
} in transactions of 5000 rows