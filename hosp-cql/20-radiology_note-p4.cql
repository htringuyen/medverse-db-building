:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/radiology_extracted_terms.csv" AS row
call {
    with row
    match (rs:RefSet {id:2502})
    match (item:LocalConcept {id:rs.code + '|' + row.exam_code})
    match (note:Note {id:row.note_id})

    with row, item, note,
        case when row.cpt_code is not null and row.cpt_code <> ''
            then {code:row.cpt_code, system:'CPT'}
            else null
        end as mapping

    create (ex:Annotation {id:row.note_id + '-' + row.field_ordinal})
        set ex.mappingCode = mapping.code,
            ex.mappingCodeSystem = mapping.system

    create (note)-[:HAS_CONT]->(ex)
    create (ex)-[:OF_ITEM]->(item)
}