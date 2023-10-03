:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_icd_procedures.csv" AS row
call {
    with row
    match (rs:RefSet {id:1903})
    with row, rs,
        case when row.icd_version = '9' then 'V09-' + row.icd_code else 'V10-' + row.icd_code end as proc_code,
        case when row.icd_version = '9' then "ICD9Proc" else "ICD10PCS" end as proc_category
    merge (lc:LocalConcept {id:rs.code + '|' + proc_code})
        on create
        set lc:Item:CATEGORIZED,
            lc.code = proc_code,
            lc.name = row.long_title,
            lc.source = "dataset::mimiciv/2.2/hosp/d_icd_procedures",
            lc.category = proc_category
    merge (lc)-[:BELONGS_TO]->(rs)
} in transactions of 5000 rows