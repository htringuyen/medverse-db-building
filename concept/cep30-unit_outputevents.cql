profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_unit_outputevents.csv" AS row
call {
    with row
    match (itemRs:RefSet {id:1901})
    match (item:LocalConcept {id: itemRs.code + '|' + row.itemid})

    match (lvs:LookupValueSupplier {id:'otmv'})

    match (unitRs:RefSet {id:2201})

    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    with row, item, unitRs, lookupValue
    merge (unit:LocalConcept {id: unitRs.code + '|' + row.valueuom})
        on create
        set unit:Unit:Lookup,
            unit.value = lookupValue,
            unit.code = row.valueuom,
            unit.name = row.valueuom,
            unit.source = "dataset::mimiciv/2.2/icu/d_items"
    merge (unit)-[:BELONGS_TO]->(unitRs)
    merge (item)-[:ASSOC_UNIT]->(unit)
}