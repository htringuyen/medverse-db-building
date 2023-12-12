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
    match (chartedAnc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})

    with row, chartedAnc, specItem, charttime
    create (labSamp: LabSampling {specimenId: toInteger(row.specimen_id)})
        set labSamp:EXISTENT:Measurement,
            //labSamp.hadmId = toInteger(row.hadm_id),
            labSamp.subjectId = toInteger(row.subject_id)
    create (labSamp)-[:CHARTED]->(chartedAnc)
    create (labSamp)-[:OF_ITEM]->(specItem)

    with row, charttime, labSamp
    // link lab sampling to visit-stop
    match (pa:Patient {id:toInteger(row.subject_id)})-[:HAS_VISIT]->()-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(started),
          (stop)-[:ENDED]->(ended)
    where started.value <= charttime < ended.value
    with pa, stop, labSamp
    limit 1
    create (stop)-[:HAS_MEAS]->(labSamp)
} in transactions of 5000 rows