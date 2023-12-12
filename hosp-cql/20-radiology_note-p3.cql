:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimic-iv-note/2.2/note/radiology.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    with row,
         datetime({epochseconds:apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime
    match (rs:RefSet {id:2501})
    match (item:LocalConcept {id:rs.code + '|' + row.note_type})
    match (chartedAnc:Anchor {id:row.subject_id + '-' + charttime.epochSeconds})
    match (note:Note {id:row.note_id})

    with row, note, chartedAnc, item, charttime
    set note:EventReport,
        note.chartTime = charttime,
        note.text = row.text
    create (note)-[:OF_ITEM]->(item)
    create (note)-[:CHARTED]->(chartedAnc)

    with row, note, charttime
    match (pa:Patient {id:toInteger(row.subject_id)})-[:HAS_VISIT]->()-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(startedAnc),
          (stop)-[:ENDED]->(endedAnc)
    where startedAnc.value <= charttime < endedAnc.value

    with note, stop
    create (stop)-[:HAS_NOTE]->(note)
}