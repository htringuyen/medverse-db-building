:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/custom_partitions/chartevents_uom_flag.csv" AS row
call {
    with row
    match (md:IngestMetadata {id:"chartevents|" + row.itemid})
    match (item:LocalConcept {id:md.itemRsCode + '|' + row.itemid})

    with row, item
    match (unitRs:RefSet {id:2201})
    match (flagRs:RefSet {id:2401})
    match (lvs: LookupValueSupplier {id: "otmv"})

    with row, item, unitRs, flagRs, lvs
    where (row.valueuom is not null and row.valueuom <> '') or (row.warning = "1")

    match (flag:LocalConcept {id:flagRs.code + "|" + "warning"})
    foreach (ignore in case when row.warning = '1' then [1] else [] end |
        merge (item)-[:HAS_POSS_FLAG]->(flag)
    )

    with row, item, unitRs, lvs
    where row.valueuom is not null and row.valueuom <> ''
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    merge (unit:LocalConcept {id:unitRs.code + "|" + row.valueuom})
        on create
        set unit:Unit:Lookup,
            unit.value = lookupValue,
            unit.code = row.valueuom,
            unit.name = row.valueuom,
            unit.source = "dataset::mimiciv/2.2/icu/chartevents"
    merge (unit)-[:BELONGS_TO]->(unitRs)
    merge (item)-[:ASSOC_UNIT]->(unit)
}