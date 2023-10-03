:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_drug_unit_emar.csv" AS row
call {
    with row
    match (drugRs:RefSet {id:2101})
    match (unitRs:RefSet {id:2201})
    match (lvs:LookupValueSupplier {id:'otmv'})


    foreach(ignore in case when row.medication is not null and row.medication <> '' then [1] else [] end |
        merge (drug:LocalConcept {id:drugRs.code + '|' + row.medication})
          on create
          set drug:Drug,
            drug.code = row.medication,
            drug.name = row.medication,
            drug.source = "dataset::mimiciv/2.2/hosp/emar"
        merge (drug)-[:BELONGS_TO]->(drugRs)
    )

    with row, unitRs, drugRs, lvs
    where row.dose_due_unit is not null and row.dose_due_unit <> ''
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue

    with row, unitRs, drugRs, lookupValue
    foreach (ignore in case when row.dose_due_unit is not null and row.dose_due_unit <> '' then [1] else [0] end |
        merge (unit:LocalConcept {id:unitRs.code + '|' + row.dose_due_unit})
          on create
          set unit:Unit:Lookup,
              unit.value = lookupValue,
              unit.code = row.dose_due_unit,
              unit.name = row.dose_due_unit,
              unit.source = "dataset::mimiciv/2.2/hosp/emar"
        merge (unit)-[:BELONGS_TO]->(unitRs)
    )

    with row, unitRs, drugRs
    match (drug:LocalConcept {id:drugRs.code + '|' + row.medication})
    match (unit:LocalConcept {id:unitRs.code + '|' + row.dose_due_unit})
    merge (drug)-[:HAS_POSS_UNIT]->(unit)
}