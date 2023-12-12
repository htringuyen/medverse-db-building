:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/microbiologyevents_datetimes.csv" AS row
call {
    with row
    with row,
         case when row.charttime is not null and row.charttime <> ''
            then row.charttime
            else row.chartdate
        end as timevalue

    match (pa:Patient {id:toInteger(row.subject_id)})
    with row,
        datetime({epochseconds: apoc.date.parse(timevalue, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime
    merge (anc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})
        on create
        set anc.value = charttime
}