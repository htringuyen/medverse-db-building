:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimic-iv-note/2.2/note/discharge.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    match (rs:RefSet {id:2501})
    match (item:LocalConcept {id:rs.code + '|' + row.note_type})
    match (vis:Visit {id:row.hadm_id})

    with row, item, vis,
        datetime({epochseconds:apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime
    create (note:Note {id:row.note_id})
        set note:EraSummary,
            note.chartTime = charttime,
            note.text = row.text
    create (vis)-[:HAS_NOTE]->(note)
    create (note)-[:OF_ITEM]->(item)
}