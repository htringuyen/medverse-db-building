:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_hcpcs.csv" AS row
call {
    with row
    with row,
        case when row.category is not null and row.category <> "" then "CPT-CAT_" + row.category else "CPT-CAT_UNK" end as category
    match (rs:RefSet {id:1904})
    merge (lc:LocalConcept {id:rs.code + '|' + row.code})
        on create
        set lc:Item:CATEGORIZED,
            lc.code = row.code,
            lc.name = row.short_description,
            lc.source = "dataset::mimiciv/2.2/hosp/d_hcpcs",
            lc.category = category
    merge (lc)-[:BELONGS_TO]->(rs)
} in transactions of 5000 rows