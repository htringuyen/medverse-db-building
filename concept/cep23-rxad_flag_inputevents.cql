profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_rxad_flag_inputevents.csv" AS row
call {
    with row
    match (adIRs:RefSet {id:2104})
    match (flagRs:RefSet {id:2401})
    match (lvs:LookupValueSupplier {id:'otmv'})

    with row, adIRs, flagRs, lvs
    merge (adItem:LocalConcept {id:adIRs.code + '|' + row.ordercategorydescription})
        on create
        set adItem:Item:CATEGORIZED,
            adItem.category = "MetaVision ADMINISTRATION",
            adItem.code = row.ordercategorydescription,
            adItem.name = row.ordercategorydescription,
            adItem.source = "dataset::mimiciv/2.2/icu/inputevents"
    merge (adItem)-[:BELONGS_TO]->(adIRs)

    with row, adItem, flagRs, lvs
    where row.statusdescription is not null and row.statusdescription <> ''
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    merge (flag:LocalConcept {id:flagRs.code + '|' + row.statusdescription})
        on create
        set flag:Flag:Lookup:CATEGORIZED,
            flag.value = lookupValue,
            flag.flagDescription = "Adminstration/Process Status",
            flag.category = "Drug Administration",
            flag.code = row.statusdescription,
            flag.name = row.statusdescription,
            flag.source = "dataset::mimiciv/2.2/icu/inputevents"
    merge (flag)-[:BELONGS_TO]->(flagRs)
    merge (adItem)-[:ASSOC_FLAG]->(flag)
}