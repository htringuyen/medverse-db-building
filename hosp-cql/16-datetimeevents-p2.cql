:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/icu/datetimeevents.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    match (itemRs:RefSet {id:1901})
    match (unitRs:RefSet {id:2201})
    match (flagRs:RefSet {id:2401})
    with row,
        datetime({epochseconds:apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime,
        datetime({epochseconds:apoc.date.parse(row.value, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as value_time,
        itemRs.code + '|' + row.itemid as item_id,
        unitRs.code + '|' + row.valueuom as unit_id,
        case when row.warning = "1" then flagRs.code + '|' + 'warning' else "UNDEF|ALL" end as flag_id

    match (item:LocalConcept {id:item_id})
    match (unit:LocalConcept {id:unit_id})
    match (flag:LocalConcept {id:flag_id})
    match (chartedAnc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})
    match (vis:Visit {id:row.hadm_id})

    with row, charttime, item, unit, flag, chartedAnc, value_time, vis
    create (proc:Procedure:Miscellaneous:DATETIME)
        set proc.valueUnit = unit.value,
            proc.valueTime = value_time,
            proc.valueFlag = case when flag.value <> 0 then flag.value else null end
    create (proc)-[:CHARTED]->(chartedAnc)
    create (proc)-[:OF_ITEM]->(item)
    create (vis)-[:HAS_PROC]->(proc)


    /*with row, charttime, proc

    match (vis)-[:HAS_STOP]->(stop:Stop {id:row.stay_id}),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)

    with row, charttime, proc, vis, stop, stopStarted, stopEnded

    foreach (ignore in case when stopStarted.value <= charttime < stopEnded.value then [1] else [] end |
        create (stop)-[:HAS_PROC]->(proc)
    )

    with row, charttime, proc, vis
    where not (stopStarted.value <= charttime < stopEnded.value)
    match (vis)-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    where stopStarted.value <= charttime < stopEnded.value
    with proc, stop
    create (stop)-[:HAS_PROC]->(event)*/
}






















