:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/pharmacy_datetimes.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    with pa, row,
        datetime({epochseconds:apoc.date.parse(row.starttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as starttime,
        case when row.stoptime is not null and row.stoptime <> ""
            then datetime({epochseconds:apoc.date.parse(row.stoptime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears else null end as endtime

    merge (startAnc:Anchor {id:row.subject_id + "-" + starttime.epochSeconds})
        on create
        set startAnc.value = starttime

    with pa, row, endtime
    where endtime is not null
    merge (endAnc:Anchor {id:row.subject_id + "-" + endtime.epochSeconds})
        on create
        set endAnc.value = endtime
}