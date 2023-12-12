:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/micro_samplings.csv" AS row
call {
    with row
    match (rs:RefSet {id:1710})
    match (lc:LocalConcept {id:rs.code + '|' + row.spec_itemid})
    match (pa:Patient {id:toInteger(row.subject_id)})

    with row, lc,
        case when row.charttime is not null and row.charttime <> ''
        then datetime ({epochseconds: apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
        else datetime ({epochseconds: apoc.date.parse(row.chartdate, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
        end as charttime,
        case when row.charttime is not null and row.charttime <> '' then true else false end as has_charttime
    match (chartedAnc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})

    with row, lc, chartedAnc, charttime, has_charttime
    create (ms:MicroSampling {specimenId:toInteger(row.micro_specimen_id)})
        set ms:EXISTENT:Measurement,
            ms.subjectId = toInteger(row.subject_id),
            ms.hasChartTime = has_charttime
    create (ms)-[:OF_ITEM]->(lc)
    create (ms)-[:CHARTED]->(chartedAnc)

    with row, ms, charttime
    match (pa:Patient {id:toInteger(row.subject_id)})-[:HAS_VISIT]->()-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(startAnc),
          (stop)-[:ENDED]->(endAnc)
    where startAnc.value <= charttime < endAnc.value
    with ms, pa, stop
    limit 1
    create (stop)-[:HAS_MEAS]->(ms)
}


