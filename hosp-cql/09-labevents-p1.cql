:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/labevents_specimens.csv" AS row
// labevent-part01: load specimen, param and comment
call {
    with row
    match (pa:Patient {id: toInteger(row.subject_id)})
    with row,
        datetime( {epochseconds: apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')} ) - pa.shiftedYears as charttime
    // create lab sampling
    match (rs:RefSet {id:1703})
    match (specItem: LocalConcept {id: rs.code + '|' + row.fluid})
    merge (labSamp: LabSampling {specimenId: toInteger(row.specimen_id)})
        on create
        set labSamp:EXISTENT:Measurement,
            labSamp.visitId = case when row.hadm_id is not null and row.hadm_id <> '' then row.hadm_id else row.subject_id
                            + '-' + charttime.epochseconds end,
            labSamp.subjectId = toInteger(row.subject_id),
            labSamp.newCreated = true

    foreach (ignore in case when labSamp.newCreated is not null then [1] else [] end |
        merge (chartedAnc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})
            on create
            set chartedAnc.value = charttime
        merge (labSamp)-[:MEAS_CHARTED]->(chartedAnc)
        merge (labSamp)-[:OF_ITEM]->(specItem)
        remove labSamp.newCreated
    )
}