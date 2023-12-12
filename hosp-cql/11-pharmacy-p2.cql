:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/pharmacy.csv" AS row
call {
    with row
    match (rs:RefSet {id:2105})
    match (lc:LocalConcept {id:rs.code + '|' + row.proc_type})

    match (flagRs:RefSet {id:2401})
    match (flag:LocalConcept {id:flagRs.code + '|' + row.status})

    match (pa:Patient {id:toInteger(row.subject_id)})
    with row, lc, flag,
        case when row.starttime is not null and row.starttime <> ""
            then datetime({epochseconds: apoc.date.parse(row.starttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
            else datetime({epochseconds: apoc.date.parse(row.entertime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
            end as starttime,
        case when row.stoptime is not null and row.stoptime <> ""
            then datetime({epochseconds: apoc.date.parse(row.stoptime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
            else null
            end as endtime,
        case when trim(row.medication) is not null and trim(row.medication) <> '' then true else false end as hasMainRx,
        case when row.frequency is not null and row.frequency <> '' then row.frequency else null end as frequency,
        case when row.disp_sched is not null and row.disp_sched <> '' then row.disp_sched else null end as disp_sched,
        case when row.infusion_type is not null and row.infusion_type <> '' then row.infusion_type else null end as infusion_type,
        case when row.sliding_scale is not null and row.sliding_scale <> '' then row.sliding_scale else null end as sliding_scale,
        case when row.lockout_interval is not null and row.lockout_interval <> '' then row.lockout_interval else null end as lockout_interval,
        case when row.basal_rate is not null and row.basal_rate <> '' then row.basal_rate else null end as basal_rate,
        case when row.one_hr_max is not null and row.one_hr_max <> '' then row.one_hr_max else null end as one_hr_max,
        case when row.doses_per_24_hrs is not null and row.doses_per_24_hrs <> '' then row.doses_per_24_hrs else null end as doses_per_24_hrs,
        case when row.duration is not null and row.duration <> '' then row.duration else null end as duration,
        case when row.duration_interval is not null and row.duration_interval <> '' then row.duration_interval else null end as duration_interval,
        case when row.expiration_value is not null and row.expiration_value <> '' then row.expiration_value else null end as expiration_value,
        case when row.expiration_unit is not null and row.expiration_unit <> '' then row.expiration_unit else null end as expiration_unit,
        case when row.fill_quantity is not null and row.fill_quantity <> '' then row.fill_quantity else null end as fill_quantity


    create (med:Medication {id:toInteger(row.pharmacy_id)})
        set med.valueFlag1 = flag.value,
            med.hasMainRx = hasMainRx,
            med.frequency = frequency,
            med.dispSched = disp_sched,
            med.infusionType = infusion_type,
            med.slidingScale = sliding_scale,
            med.lockoutInterval = lockout_interval,
            med.basalRate = toFloat(basal_rate),
            med.oneHrMax = one_hr_max,
            med.dosesPer24Hrs = toFloat(doses_per_24_hrs),
            med.duration = toFloat(duration),
            med.durationInterval = duration_interval,
            med.expirationValue = toInteger(expiration_value),
            med.expirationUnit = expiration_unit

    with row, lc, starttime, endtime, med

    match (startAnc:Anchor {id: row.subject_id + '-' + starttime.epochSeconds})
    match (endAnc:Anchor {id: coalesce(row.subject_id + '-' + endtime.epochSeconds, "UNDEFINED_UPPER")})
    match (vis:Visit {id:row.hadm_id})

    with row, lc, med, starttime, startAnc, endAnc
    create (med)-[:OF_ITEM]->(lc)
    create (med)-[:STARTED]->(startAnc)
    create (med)-[:ENDED]->(endAnc)
    create (vis)-[:HAS_MED]->(med)

} in transactions of 5000 rows