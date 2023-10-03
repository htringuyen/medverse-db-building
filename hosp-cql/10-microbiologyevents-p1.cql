:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/micro_samplings.csv" AS row
call {
    with row
    match (rs:RefSet {id:1710})
    match (lc:LocalConcept {id:rs.code + '|' + row.spec_itemid})
    match (pa:Patient {id:toInteger(row.subject_id)})

    with row, lc, pa,
        case when row.charttime is not null and row.charttime <> ''
        then datetime ({epochseconds: apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
        else datetime ({epochseconds: apoc.date.parse(row.chartdate, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
        end as charttime
    create (ms:Measurement:MicroSampling:EXISTENT {specimenId:toInteger(row.micro_specimen_id)})
        set ms.subjectId = toInteger(row.subject_id),
            ms.visitId = case when row.hadm_id is not null and row.hadm_id <> ''
                        then row.hadm_id else row.subject_id + "-" + charttime.epochSeconds end
    create (ms)-[:OF_ITEM]->(lc)
    merge (chartedAnc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})
        on create
        set chartedAnc.value = charttime
    create (ms)-[:MEAS_CHARTED]->(chartedAnc)
}




















