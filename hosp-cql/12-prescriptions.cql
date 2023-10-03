:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/prescriptions.csv" AS row
call {
    with row
    match (med:Medication {id: toInteger(row.pharmacy_id)})

    match (drugRs:RefSet {id:2101})
    match (drug:LocalConcept {id: drugRs.code + '|' + row.drug})

    match (prodStrRs:RefSet {id:2103})
    match (prodStr:LocalConcept {id: prodStrRs.code + '|' + row.prod_strength})

    match (unitRs:RefSet {id:2201})
    match (doseUnit:LocalConcept {id: unitRs.code + '|' + row.dose_unit_rx})
    match (formUnit:LocalConcept {id: unitRs.code + '|' + row.form_unit_disp})

    match (routeRs:RefSet {id:2102})
    match (route:LocalConcept {id: routeRs.code + '|' + row.route})

    with row, med, drug, prodStr, doseUnit, formUnit, route
    where row.dose_val_rx is not null and row.dose_val_rx <> '' and row.form_val_disp is not null and row.form_val_disp <> ''
    create (rx:Prescription)
        set rx.valueProdStr = prodStr.value,
            rx.valueDUnit = doseUnit.value,
            rx.valueFUnit = formUnit.value,
            rx.valueRoute = route.value,
            rx.valueDose = toFloat(split(row.dose_val_rx, '-')[0]),
            rx.valueDoseUpto = toFloat(split(row.dose_val_rx, '-')[1]),
            rx.valueForm = toFloat(split(row.form_val_disp, '-')[0]),
            rx.valueFormUpto = toFloat(split(row.form_val_disp, '-')[1]),
            rx.dosesPer24Hrs = toFloat(row.doses_per_24_hrs)

    with row, med, drug, rx
    call apoc.create.setLabels(rx, ["Prescription", trim(row.drug_type)]) yield node
    create (med)-[:HAS_RX]->(rx)
    create (rx)-[:OF_DRUG]->(drug)
} in transactions of 5000 rows