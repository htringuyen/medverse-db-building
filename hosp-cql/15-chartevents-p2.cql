:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/chartevents_textvals.csv" AS row
call {
    with row
    with row
    where row.value is not null and row.value <> ''
    match (md:IngestMetadata {id:"chartevents|" + row.itemid})
    match (item:LocalConcept {id:md.itemRsCode + '|' + row.itemid})

    with row, item, md,
        case
            when md.chartDomain contains "Meas" then 1702
            when md.chartDomain = "Observation" then 1602
            when md.chartDomain = "Procedure" then 1902
            when md.chartDomain = "Device" then 1802
            when md.chartDomain = "Condition" then 1504
        end as param_rs_id

    match (paramRs:RefSet {id:param_rs_id})
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    with row, item, md, paramRs, lookupValue
    set md.paramRsCode = paramRs.code
    merge (param:LocalConcept {id:paramRs.code + '|' + row.value})
        on create
        set param:TextParam:Lookup,
            param.value = lookupValue,
            param.code = row.value,
            param.name = row.value,
            param.source = "dataset::mimiciv/2.2/icu/chartevents"
    merge (param)-[:BELONGS_TO]->(paramRs)
    merge (item)-[:ASSOC_PARAM]->(param)
}