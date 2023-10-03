:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/emar_detail.csv" AS row
call {
    with row
    with row
    where row.parent_field_ordinal is null or row.parent_field_ordinal = ""

    match (rxad:RxAdmin {id: row.emar_id})-[:HAS_EXPOSURE]->(exp)
    match (adIRs: RefSet {id: 2104})
    match (unitRs: RefSet {id: 2201})

    with row, rxad, exp, unitRs,
        case when row.administration_type is not null and row.administration_type <> "" then rs.code + '|' + row.administration_type else "UNDEF|ITEM" end as ad_id
    match (adItem: LocalConcept {id: ad_id})

    with row, rxad, exp, unitRs, adItem
    optional match (unit:LocalConcept {id:unitRs.code + '|' + row.dose_due_unit})

    with row, rxad, exp, adItem, unit
    create (rxad)-[:OF_ITEM]->(adItem)
    set exp.valueDose = toFloat(split(row.dose_due, '-')[0]),
        exp.valueDoseUpto = toFloat(split(row.dose_due, '-')[1]),
        exp.valueDUnit = unit.value

    with row, exp
    where row.dose_due_unit =~ ".+/(h|H|min|MIN|Min).*"
            or row.infusion_rate is not null and row.infusion_rate <> ""
            or row.infusion_rate_unit is not null and row.infusion_rate_unit <> ""
            or row.infusion_rate_adjustment is not null and row.infusion_rate_adjustment <> ""
    set exp:INFUSION,
        exp.priorInfRate = toFloat(split(row.prior_infusion_rate, "-")[0]),
        exp.priorInfRateUpto = toFloat(split(row.prior_infusion_rate, "-")[1]),
        exp.fromOpenBag = case when row.new_iv_bag_hung = "Y" then false else true end
}