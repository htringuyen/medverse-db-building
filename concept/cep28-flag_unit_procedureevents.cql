profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_flag_unit_procedureevents.csv" AS row
call {
    with row
    match (itemRs:RefSet {id:1901})
    match (item:LocalConcept {id: itemRs.code + '|' + row.itemid})
    match (lvs:LookupValueSupplier {id:'otmv'})
    match (flagRs:RefSet {id:2401})
    match (unitRs:RefSet {id:2201})

    with row, item, lvs, flagRs, unitRs
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (flag:LocalConcept {id:flagRs.code + '|' + row.statusdescription})
        on create
        set flag:Flag:Lookup:CATEGORIZED,
            flag.value = lookupValue,
            flag.flagDescription = "Adminstration/Process Status",
            flag.category = "Drug Administration",
            flag.code = row.statusdescription,
            flag.name = row.statusdescription,
            flag.source = "dataset::mimiciv/2.2/icu/procedureevents"
    merge (flag)-[:BELONGS_TO]->(flagRs)
    merge (item)-[:ASSOC_FLAG]->(flag)

    with row, item, lvs, unitRs
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (unit:LocalConcept {id:unitRs.code + '|' + row.valueuom})
        on create
        set unit:Unit:Lookup,
            unit.value = lookupValue,
            unit.code = row.valueuom,
            unit.name = row.valueuom,
            unit.source = "dataset::mimiciv/2.2/icu/procedureevents"
    merge (unit)-[:BELONGS_TO]->(unitRs)
    merge (item)-[:ASSOC_UNIT]->(unit)

    with row, item, lvs
    where row.location is not null and row.location <> ''
    match (flagRs:RefSet {id:2401})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (flag:LocalConcept {id:flagRs.code + '|' + row.location})
        on create
        set flag:Flag:Lookup:CATEGORIZED,
            flag.value = lookupValue,
            flag.flagDescription = "Body site",
            flag.category = row.locationcategory,
            flag.code = row.location,
            flag.name = row.location,
            flag.source = "dataset::mimiciv/2.2/icu/procedureevents"
}