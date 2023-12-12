:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/icu/procedureevents.csv" AS row
call {
    with row
    match (pa:Patient {id: toInteger(row.subject_id)})
    match (itemRs:RefSet {id:1901})
    match (item:LocalConcept {id:itemRs.code + "|" + row.itemid})

    match (unitRs:RefSet {id:2201})
    match (flagRs:RefSet {id:2401})

    with row, item,
        unitRs.code + "|" + row.valueuom as unit_id,
        flagRs.code + "|" + row.statusdescription as flag_id,
        case when row.loccation is not null and row.location <> '' then flagRs.code + "|" + row.location else "UNDEF|ALL" end as flag2_id,
        datetime({epochseconds:apoc.date.parse(row.starttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as starttime,
        datetime({epochseconds:apoc.date.parse(row.endtime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as endtime

    match (unit:LocalConcept {id:unit_id})
    match (flag:LocalConcept {id:flag_id})
    match (flag2:LocalConcept {id:flag2_id})
    match (vis:Visit {id:row.hadm_id})

    with row, item, unit, flag, flag2, starttime, endtime, vis
    create (proc:Procedure)
    set proc:Miscellaneous:NUMERIC,
        proc.valueNum = toFloat(row.value),
        proc.valueUnit = unit.value,
        proc.valueFlag = flag.value,
        proc.value2ndFlag = case when flag2.value <> 0 then flag2.value else null end
    create (vis)-[:HAS_PROC]->(proc)
    create (proc)-[:OF_ITEM]->(item)
    merge (startAnc:Anchor {id:row.subject_id + "-" + starttime.epochSeconds})
        on create
        set startAnc.value = starttime

    merge (endAnc:Anchor {id:row.subject_id + "-" + endtime.epochSeconds})
        on create
        set endAnc.value = endtime

    create (proc)-[:STARTED]->(startAnc)
    create (proc)-[:ENDED]->(endAnc)

    with row, proc, starttime
    optional match (era:ProcedureEra {id:row.linkorderid})

    with row, proc, era, starttime
    foreach (ignore in case when era is not null then [1] else [] end |
        create (era)-[:INCLUDES]->(proc)
    )

    /*with row, proc, starttime
    match (vis)-[:HAS_STOP]->(stop:Stop {id:row.stay_id}),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    foreach (ignore in case when stopStarted.value <= starttime < stopEnded.value then [1] else [] end |
        create (stop)-[:HAS_RECORD]->(proc)
    )

    with row, proc, vis, starttime
    where not (stopStarted.value <= starttime < stopEnded.value)
    match (vis)-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(stopStarted),
          (stop)-[:ENDED]->(stopEnded)
    where stopStarted.value <= starttime < stopEnded.value

    with proc, stop
    create (stop)-[:HAS_PROC]->(proc)*/
}
















