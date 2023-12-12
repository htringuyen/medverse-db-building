:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/icu/outputevents.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})

    with row,
        datetime({epochseconds:apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime

    match (itemRs:RefSet {id:1701})
    match (unitRs:RefSet {id:2201})

    match (item:LocalConcept {id:itemRs.code + '|' + row.itemid})
    match (unit:LocalConcept {id:unitRs.code + '|' + row.valueuom})

    match (chartedAnc: Anchor {id:row.subject_id + '-' + charttime.epochSeconds})

    with row, chartedAnc, item, unit, charttime
    create (meas:Measurement:Miscellaneous:NUMERIC)
    set meas.valueNum = toFloat(row.value),
        meas.valueUnit = unit.value

    create (meas)-[:OF_ITEM]->(item)
    create (meas)-[:CHARTED]->(chartedAnc)

    with row, charttime, meas

    match (vis)-[:HAS_STOP]->(stop:Stop {id:row.stay_id}),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)

    with row, charttime, meas, vis, stop, stopStarted, stopEnded

    foreach (ignore in case when stopStarted.value <= charttime < stopEnded.value then [1] else [] end |
        create (stop)-[:HAS_MEAS]->(meas)
    )

    with row, charttime, meas, vis
    where not (stopStarted.value <= charttime < stopEnded.value)
    match (vis)-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    where stopStarted.value <= charttime < stopEnded.value
    with meas, stop
    create (stop)-[:HAS_MEAS]->(meas)
}