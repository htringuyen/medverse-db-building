:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/prescriptions.csv" AS row
call {
    with row
    match (med:Medication {id: toInteger(row.pharmacy_id)})

    match (drugRs:RefSet {id:2101})
    match (drug:LocalConcept {id: drugRs.code + '|' + trim(row.drug)})

    match (prodStrRs:RefSet {id:2103})
    match (routeRs:RefSet {id:2102})
    match (unitRs:RefSet {id:2201})

    with row, med, drug,
        case when trim(row.prod_strength) is not null and trim(row.prod_strength) <> '' then prodStrRs.code + '|' + trim(row.prod_strength) else "UNDEF|ALL" end as prod_str_id,
        case when trim(row.route) is not null and trim(row.route) <> '' then routeRs.code + '|' + trim(row.route) else "UNDEF|ALL" end as route_id,
        case when trim(row.dose_unit_rx) is not null and trim(row.dose_unit_rx) <> '' then unitRs.code + '|' + trim(row.dose_unit_rx) else "UNDEF|ALL" end as dose_unit_id,
        case when trim(row.form_unit_disp) is not null and trim(row.form_unit_disp) <> '' then unitRs.code + '|' + trim(row.form_unit_disp) else "UNDEF|ALL" end as form_unit_id

    match (doseUnit:LocalConcept {id: dose_unit_id})
    match (formUnit:LocalConcept {id: form_unit_id})
    match (prodStr:LocalConcept {id: prod_str_id})
    match (route:LocalConcept {id: route_id})

    with row, med, drug,
        case when doseUnit.value <> 0 then doseUnit.value else null end as dose_unit_value,
        case when formUnit.value <> 0 then formUnit.value else null end as form_unit_value,
        case when prodStr.value <> 0 then prodStr.value else null end as prod_str_value,
        case when route.value <> 0 then route.value else null end as route_value

    create (rx:Prescription)
        set rx.valueProdStr = prod_str_value,
            rx.valueDUnit = dose_unit_value,
            rx.valueFUnit = form_unit_value,
            rx.valueRoute = route_value,
            rx.valueDose = toFloat(split(row.dose_val_rx, '-')[0]),
            rx.valueDoseUpto = toFloat(split(row.dose_val_rx, '-')[1]),
            rx.valueForm = toFloat(split(row.form_val_disp, '-')[0]),
            rx.valueFormUpto = toFloat(split(row.form_val_disp, '-')[1]),
            rx.dosesPer24Hrs = toFloat(row.doses_per_24_hrs)

    with row, med, drug, rx
    call apoc.create.setLabels(rx, ["Prescription", trim(row.drug_type)]) yield node
    create (med)-[:HAS_SEQ]->(rx)
    create (rx)-[:OF_DRUG]->(drug)
} in transactions of 5000 rows