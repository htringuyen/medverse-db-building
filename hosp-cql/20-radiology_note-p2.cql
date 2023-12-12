:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/radiology_note_addendum.csv" AS row
call {
    with row
    where not row.note_id contains "AR"
    create (note:Note {id:row.note_id})
        set note:EventReport,
            note.chartTime = datetime(),
            note.text = "N/A"

    with row, note
    where row.addendum_note_id is not null and row.addendum_note_id <> ""
    create (addm:Note {id:row.addendum_note_id})
        set addm:EventReport,
            addm.chartTime = datetime(),
            addm.text = "N/A"
    create (note)-[:HAS_SEQ]->(addm)
}