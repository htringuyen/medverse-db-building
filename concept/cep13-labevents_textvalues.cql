profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_labevents_textvalues.csv" AS row
call {
    with row
    with row
    where row.value is not null and row.value <> ''
        and ((not row.value =~ '^(\<=|\>=|\>|\<|=|)[-]?[\d]+[.]?[\d]*\s*\D*')
            or (row.value =~ '^(\<=|\>=|\>|\<|=|)[-]?[\d]+[.]?[\d]*\s*\D+' and (row.valueuom is null or row.valueuom = '')))
    match (lvs: LookupValueSupplier {id: "otmv"})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    // extract text param
    match (rs:RefSet {id:1705})
    merge (lc:LocalConcept {id: rs.code + '|' + row.value})
        on create
        set lc:TextParam:Lookup,
            lc.value = lookupValue,
            lc.code = row.value,
            lc.name = row.value,
            lc.source = "dataset::mimiciv/2.2/hosp/labevents"
    merge (lc)-[:BELONGS_TO]->(rs)

    with lc, row
    match (rs:RefSet {id: 1704})
    match (item:LocalConcept {id:rs.code + '|' + row.itemid})
    merge (item)-[:HAS_POSS_PARAM]->(lc)
}