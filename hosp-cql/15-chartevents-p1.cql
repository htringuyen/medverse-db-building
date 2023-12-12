:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/chartevents_d_items.csv" AS row
call {
    with row

    with row,
        case when row.post_mappping is not null and row.post_mapping <> ""
            then row.post_mappping
            else row.domain
        end as chart_domain

    with row, chart_domain,
        case
            when chart_domain contains "Meas" then 1701
            when chart_domain = "Observation" then 1601
            when chart_domain = "Procedure" then 1901
            when chart_domain = "Device" then 1801
            when chart_domain = "Condition" then 1503
        end as item_rs_id

    match (rs:RefSet {id: item_rs_id})

    with row, rs, chart_domain

    merge (lc:LocalConcept {id:rs.code + "|" + row.itemid})
      on create
      set lc:Item:CATEGORIZED,
          lc.category = row.category,
          lc.refRangeLower = toFloat(row.lownormalvalue),
          lc.refRangeUpper = toFloat(row.highnormalvalue),
          lc.code = row.itemid,
          lc.name = row.label,
          lc.source = "dataset::mimiciv/2.2/icu/chartevents"

    merge (lc)-[:BELONGS_TO]->(rs)

    merge (md:IngestMetadata {id:"chartevents|" + row.itemid})
        on create
        set md.chartDomain = chart_domain,
            md.itemRsCode = rs.code
}
