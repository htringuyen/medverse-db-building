profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_rxad_flag_emar.csv" AS row
call {
    with row
    match (adIRs:RefSet {id:2104})
    match (flagRs:RefSet {id:2401})
    match (lvs:LookupValueSupplier {id:'otmv'})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    foreach(ignore in case when trim(row.administration_type) is not null and trim(row.administration_type) <> '' then [1] else [] end |
        merge (adItem:LocalConcept {id:adIRs.code + '|' + trim(row.administration_type)})
        set adItem:Item:CATEGORIZED,
          adItem.category = "EMAR ADMINISTRATION",
          adItem.code = trim(row.administration_type),
          adItem.name = trim(row.administration_type),
          adItem.source = "dataset::mimiciv/2.2/hosp/emar"
        merge (adItem)-[:BELONGS_TO]->(adIRs)
    )


    foreach (ignore in case when trim(row.event_txt) is not null and trim(row.event_txt) <> '' then [1] else [] end |
        merge (flag:LocalConcept {id:flagRs.code + '|' + trim(row.event_txt)})
        set flag:Flag:CATEGORIZED:Lookup,
          flag.value = lookupValue,
          flag.flagDescription = "Administration Event",
          flag.category = "Drug Administration",
          flag.code = trim(row.event_txt),
          flag.name = trim(row.event_txt),
          flag.source = "dataset::mimiciv/2.2/hosp/emar"
        merge (rs)-[:BELONGS_TO]->(flagRs)
    )

    with row, adIRs, flagRs
    where trim(row.administration_type) is not null and trim(row.administration_type) <> ''
          and trim(row.event_txt) is not null and trim(row.event_txt) <> ''
    match (adItem:LocalConcept {id:adIRs.code + '|' + trim(row.administration_type)})
    match (flag:LocalConcept {id:flagRs.code + '|' + trim(row.event_txt)})
    merge (adItem)-[:ASSOC_FLAG]->(flag)
}