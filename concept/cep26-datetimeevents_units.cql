profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_datetimeevents_units.csv" AS row
call {
    with row
    match (rs:RefSet {id: 1901})
    match (item:LocalConcept {id: rs.code + '|' + row.itemid})

    with row, item
    match (rs:RefSet {id: 2201})
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    merge (unit:LocalConcept {id: rs.code + '|' + row.valueuom})
        on create
        set unit:Unit:Lookup,
            unit.value = lookupValue,
            unit.code = row.valueuom,
            unit.name = row.valueuom,
            unit.source = "dataset::mimiciv/2.2/icu/d_datetimeevents"
    merge (unit)-[:BELONGS_TO]->(rs)
    merge (item)-[:ASSOC_UNIT]->(unit)
}