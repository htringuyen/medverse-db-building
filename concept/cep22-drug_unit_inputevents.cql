profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_drug_unit_inputevents.csv" AS row
call {
    with row
    match (drugRs:RefSet {id:2101})
    match (drug:LocalConcept {id:drugRs.code + '|' + row.itemid})
    match (lvs:LookupValueSupplier {id:'otmv'})
    match (unitRs:RefSet {id:2201})

    // create amount unit and link if possible
    with drug, row, lvs, unitRs
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (amountUnit:LocalConcept {id:unitRs.code + '|' + row.amountuom})
        on create
        set amountUnit:Unit:Lookup,
            amountUnit.value = lookupValue,
            amountUnit.code = row.amountuom,
            amountUnit.name = row.amountuom,
            amountUnit.source = "dataset::mimiciv/2.2/icu/inputevents"
    merge (amountUnit)-[:BELONGS_TO]->(unitRs)
    merge (drug)-[:ASSOC_UNIT]->(amountUnit)

    // create rate unit and link if possible
    with drug, row, lvs, unitRs
    where row.rateuom is not null and row.rateuom <> ''
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (rateUnit:LocalConcept {id:unitRs.code + '|' + row.rateuom})
        on create
        set rateUnit:Unit:Lookup,
            rateUnit.value = lookupValue,
            rateUnit.code = row.rateuom,
            rateUnit.name = row.rateuom,
            rateUnit.source = "dataset::mimiciv/2.2/icu/inputevents"
    merge (rateUnit)-[:BELONGS_TO]->(unitRs)
    merge (drug)-[:ASSOC_UNIT]->(rateUnit)
}