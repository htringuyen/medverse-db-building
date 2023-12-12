:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/icu/inputevents.csv" AS row
call {
    with row
    match (rxad:RxAdmin {id:row.orderid})

    with row, rxad,
        case when row.rate is not null and row.rate <> "" then toFloat(row.rate) else toFloat(row.amount) end as dose_value,
        case when row.rate is not null and row.rate <> "" then row.rateuom else row.amountuom end as dose_unit,
        case
            when row.ordercomponenttypedescription contains "Main" then "MAIN"
            when row.ordercomponenttypedescription contains "Additive" then "ADDITIVE"
            when row.ordercomponenttypedescription contains "Mixed" then "BASE"
            else "UNKNOWN"
        end as dose_type

    match (drugRs:RefSet {id:2101})
    match (unitRs:RefSet {id:2201})

    match (drug:LocalConcept {id:drugRs.code + '|' + row.itemid})
    match (doseUnit:LocalConcept {id:unitRs.code + '|' + dose_unit})

    with row, unitRs, rxad, drug, doseUnit, dose_value, dose_type
    create (rxad)-[:HAS_SEQ]->(exp:DrugExposure)
        set exp.valueDose = dose_value,
            exp.valueDUnit = doseUnit.value
    create (exp)-[:OF_DRUG]->(drug)

    with row, unitRs, exp, dose_type
    call apoc.create.setLabels(exp, ["DrugExposure", dose_type]) yield node

    with row, unitRs, exp
    where row.rate is not null and row.rate <> ""

    match (amountUnit:LocalConcept {id:unitRs.code + '|' + row.amountuom})

    with row, exp, amountUnit
    set exp:INFUSION,
        exp.infAmount = toFloat(row.amounnt),
        exp.totalInfAmount = toFloat(row.totalamount),
        exp.valueInfAmountUnit = amountUnit.value,
        exp.originalInfRate = toFloat(row.originalrate),
        exp.originalInfAmount = toFloat(row.originalamount),
        exp.fromOpenBag = case when row.isopenbag = "1" then true else false end
}