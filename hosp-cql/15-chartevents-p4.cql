:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/chartevents_datetimes.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row,
        datetime({epochseconds:apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime
    merge (anc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})
        on create
        set anc.value = charttime
}