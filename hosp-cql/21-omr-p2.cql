:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/omr.csv" AS row
call {
    with row
    match (rs:RefSet {id:1701})
    match (item:LocalConcept {id:rs.code + '|' + row.result_name})
    match (pa:Patient {id:toInteger(row.subject_id)})

    with row, item,
        date(row.chartdate) - pa.shiftedYears as chartdate

    match (pa:Patient {id:toInteger(row.subject_id)})-[:HAS_VISIT]->(vis),
          (vis)-[:STARTED]->(visStart),
          (vis)-[:ENDED]->(visEnd)
    where date({date:visStart.value}) <= chartdate <= date({date:visEnd.value})
    with row, item, vis
    match (vis)-[:HAS_STOP]->(stop)-[:STARTED]->(stopStart)
    with row, item, vis, stop, stopStart
    order by stopStart.value
    create (meas:Measurement:Miscellaneous:NUMERIC)
        set meas.valueNum = toFloat(row.result_value)
    create (stop)-[:HAS_MEAS]->(meas)
    create (meas)-[:CHARTED]->(stopStart)
    create (meas)-[:OF_ITEM]->(item)
}