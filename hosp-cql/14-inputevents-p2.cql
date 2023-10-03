:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/icu/inputevents.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    match (rxad:RxAdmin {id:row.orderid})
    with row, rxad,
        datetime({epochseconds:apoc.date.parse(row.starttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as starttime,
        datetime({epochseconds:apoc.date.parse(row.endtime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as endtime

    match (vis:Visit {id:row.hadm_id})-[:HAS_STOP]->(stop: Stop),
          (stop)-[:STOP_STARTED]->(stopStarted),
          (stop)-[:STOP_ENDED]->(stopEnded)
    where stopStarted.value <= starttime and stopEnded.value > starttime

    with row, stop, rxad, starttime, endtime
    limit 1

    match (adIRs:RefSet {id:2104})
    match (adItem:LocalConcept {id:adIRs.code + '|' + row.ordercategorydescription})

    match (flagRs:RefSet {id:2401})
    match (adFlag:LocalConcept {id:flagRs.code + '|' + row.statusdescription})

    with row, rxad, stop, adItem, adFlag, starttime, endtime

    set rxad.patientWeight = toFloat(row.patientweight),
        rxad.valueFlag = adFlag.value

    merge (stop)-[:HAS_RXAD]->(rxad)

    merge (startAnc:Anchor {id:row.subject_id + '-' + starttime.epochSeconds})
        on create
        set startAnc.value = starttime

    merge (endAnc:Anchor {id:row.subject_id + '-' + endtime.epochSeconds})
        on create
        set endAnc.value = endtime

    merge (rxad)-[:RXAD_STARTED]->(startAnc)
    merge (rxad)-[:RXAD_ENDED]->(endAnc)
    merge (rxad)-[:OF_ITEM]->(adItem)

    with row, rxad
    match (linkAd:RxAdmin {id:row.linkorderid})

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

    with row, unitRs, rxad, drug, doseUnit, dose_value
    create (rxad)-[:HAS_EXPOSURE]->(exp:DrugExposure)
        set exp.valueDose = dose_value,
            exp.valueDUnit = doseUnit.value
    create (exp)-[:OF_DRUG]->(drug)

    with row, unitRs, exp, dose_type
    call apoc.create.setLabels(exp, ["DrugExposure", dose_type]) yield node

    with row, unitRs, exp
    where row.rate is not null and row.rate <> ""

    match (amoutUnit:LocalConcept {id:unitRs.code + '|' + row.amountuom})

    with row, exp, amoutUnit
    set exp:INFUSION,
        exp.infAmount = toFloat(row.amount),
        exp.totalInfAmount = toFloat(row.totalamount),
        exp.valueInfAmountUnit = amoutUnit.value,
        exp.originalInfRate = toFloat(row.originalrate),
        exp.originalInfAmount = toFloat(row.originalamount),
        exp.fromOpenBag = case when row.isopenbag = "1" then true else false end
}