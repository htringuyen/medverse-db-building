:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/drgcodes.csv" AS row
call {
    with row
    match (vis:Visit {id: row.hadm_id})
    match (rs:RefSet {id:1502})
    with row, vis, rs,
        row.drg_type + '-' + row.drg_code as admrsn_code,
        "DRG-" + row.drg_type as category
    merge (item:LocalConcept {id:rs.code + '|' + admrsn_code})
        on create
        set item:Item:CATEGORIZED,
            item.code = admrsn_code,
            item.name = row.description,
            item.source = "dataset::mimiciv/2.2/hosp/drgcodes",
            item.category = category

    // condition
    with row, vis, item
    create (cond:Condition:AdmReason:EXISTENT)-[:OF_ITEM]->(item)
    set cond.severity = toInteger(row.drg_severity),
        cond.mortality = toInteger(row.drg_mortality)
    create (vis)-[:HAS_COND]->(cond)
} in transactions of 5000 rows
