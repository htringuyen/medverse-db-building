profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_drug_relatives.csv" AS row
call {
    with row
    match (lvs:LookupValueSupplier {id:'otmv'})

    with row, lvs
    where row.drug is not null and row.drug <> ""

    // create drug
    with row, lvs,
        case when row.gsn is not null and row.gsn <> "" then row.gsn else null end as gsn,
        case when row.ndc is not null and row.ndc <> "" then row.ndc else null end as ndc
    match (rs:RefSet {id:2101})
    merge (drug:LocalConcept {id: rs.code + '|' + row.drug})
      on create
      set drug:Drug,
          drug.code = row.drug,
          drug.name = row.drug,
          drug.source = "dataset::mimiciv/2.2/hosp/prescriptions",
          drug.ndcCode = ndc,
          drug.gsnCode = gsn
    merge (drug)-[:BELONGS_TO]->(rs)

    // create product strength
    with row, lvs, drug
    match (rs:RefSet {id:2103})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach (ignore in case when row.prod_strength is not null and row.prod_strength <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.prod_strength})
        on create
        set lc:ProdStrength:Lookup,
            lc.value = lookupValue,
            lc.code = row.prod_strength,
            lc.name = row.prod_strength,
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge(drug)-[:HAS_POSS_PSTR]->(lc)
    )


    // create dose unit
    with row, lvs, drug
    match (rs:RefSet {id:2201})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach (ignore in case when row.dose_unit_rx is not null and row.dose_unit_rx <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.dose_unit_rx})
        on create
        set lc:Unit:Lookup,
            lc.value = lookupValue,
            lc.code = row.dose_unit_rx,
            lc.name = row.dose_unit_rx,
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge(drug)-[:HAS_POSS_DUNIT]->(lc)
    )


    // create form unit
    with row, lvs, drug
    match (rs:RefSet {id:2201})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach (ignore in case when row.form_unit_disp is not null and row.form_unit_disp <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.form_unit_disp})
        on create
        set lc:Unit:Lookup,
            lc.value = lookupValue,
            lc.code = row.form_unit_disp,
            lc.name = row.form_unit_disp,
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge(drug)-[:HAS_POSS_FUNIT]->(lc)
    )

    // create route
    with row, lvs, drug
    match (rs:RefSet {id:2102})
    call apoc.atomic.add(lvs, "value", 1, 10) yield oldValue as lookupValue
    foreach(ignore in case when row.route is not null and row.route <> "" then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.route})
        on create
        set lc:Route:Lookup,
            lc.value = lookupValue,
            lc.code = row.route,
            lc.name = row.route,
            lc.source = "dataset::mimiciv/2.2/hosp/prescriptions"
        merge (lc)-[:BELONGS_TO]->(rs)
        merge (drug)-[:HAS_POSS_ROUTE]->(lc)
    )
}





















