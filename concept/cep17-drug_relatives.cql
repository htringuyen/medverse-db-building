profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_drug_relatives.csv" AS row
call {
    with row
    match (lvs:LookupValueSupplier {id:'otmv'})

    with row, lvs
    where trim(row.drug) is not null and trim(row.drug) <> ""

    // create drug
    with row, lvs,
        case when row.gsn is not null and row.gsn <> "" then row.gsn else null end as gsn,
        case when row.ndc is not null and row.ndc <> "" then row.ndc else null end as ndc
    match (rs:RefSet {id:2101})
    merge (drug:LocalConcept {id: rs.code + '|' + trim(row.drug)})
      on create
      set drug:Drug,
          drug.code = trim(row.drug),
          drug.name = trim(row.drug),
          drug.source = "dataset::mimiciv/2.2/hosp/prescriptions",
          drug.ndcCode = ndc,
          drug.gsnCode = gsn
    merge (drug)-[:BELONGS_TO]->(rs)

    // create product strength
    with row, lvs, drug
    match (rs:RefSet {id:2103})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach (ignore in case when trim(row.prod_strength) is not null and trim(row.prod_strength) <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + trim(row.prod_strength)})
        on create
        set lc:ProdStrength:Lookup,
            lc.value = lookupValue,
            lc.code = trim(row.prod_strength),
            lc.name = trim(row.prod_strength),
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge(drug)-[:ASSOC_PSTR]->(lc)
    )


    // create dose unit
    with row, lvs, drug
    match (rs:RefSet {id:2201})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach (ignore in case when trim(row.dose_unit_rx) is not null and trim(row.dose_unit_rx) <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + trim(row.dose_unit_rx)})
        on create
        set lc:Unit:Lookup,
            lc.value = lookupValue,
            lc.code = trim(row.dose_unit_rx),
            lc.name = trim(row.dose_unit_rx),
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge(drug)-[:ASSOC_UNIT]->(lc)
    )


    // create form unit
    with row, lvs, drug
    match (rs:RefSet {id:2201})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach (ignore in case when trim(row.form_unit_disp) is not null and trim(row.form_unit_disp) <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + trim(row.form_unit_disp)})
        on create
        set lc:Unit:Lookup,
            lc.value = lookupValue,
            lc.code = trim(row.form_unit_disp),
            lc.name = trim(row.form_unit_disp),
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge(drug)-[:ASSOC_UNIT]->(lc)
    )

    // create route
    with row, lvs, drug
    match (rs:RefSet {id:2102})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach(ignore in case when trim(row.route) is not null and trim(row.route) <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + trim(row.route)})
        on create
        set lc:Route:Lookup,
            lc.value = lookupValue,
            lc.code = trim(row.route),
            lc.name = trim(row.route),
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge (drug)-[:ASSOC_ROUTE]->(lc)
    )
}





















