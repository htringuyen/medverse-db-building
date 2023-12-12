:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/emar.csv" AS row
call {
    with row
    match (pa:Patient {id:toInteger(row.subject_id)})
    match (rxad:RxAdmin {id:row.emar_id})
    match (flagRs:RefSet {id:2401})
    set rxad.subjectId = toInteger(row.subject_id),
        rxad.hadmId = toInteger(row.hadm_id)

    with row, rxad,
        datetime({epochseconds: apoc.date.parse(row.charttime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as charttime,
        case when row.event_txt is not null and row.event_txt <> '' then flagRs.code + '|' + row.event_txt else "UNDEF|ALL" end as flag_id

    match (chartAnc:Anchor {id:row.subject_id + "-" + charttime.epochSeconds})

    match (flag:LocalConcept {id:flag_id})
    match (drugRs:RefSet {id:2101})
    with row, rxad, drugRs, chartAnc, flag, charttime
    set rxad.valueFlag = case when flag.value <> 0 then flag.value else null end
    create (rxad)-[:CHARTED]->(chartAnc)
    create (rxad)-[:HAS_SEQ]->(rxexp:DrugExposure)

    with row, drugRs, rxad, rxexp, charttime
    optional match (era)-[:INCLUDES]->(rxad),
                   (era)-[:RELATES_TO]->(med)
    optional match (med)-[:HAS_SEQ]->(:MAIN)-[:OF_DRUG]->(mainDrug)
    optional match (med)-[:HAS_SEQ]->(:BASE)-[:OF_DRUG]->(baseDrug)
    optional match (matchedDrug:LocalConcept {id:drugRs.code + '|' + trim(row.medication)})

    with row, rxad, charttime, rxexp, mainDrug, baseDrug, matchedDrug
    foreach (ignore in case when mainDrug is not null and (trim(row.medication) contains mainDrug.name or baseDrug is null) then [1] else [] end |
        create (rxexp)-[:OF_DRUG]->(mainDrug)
        set rxexp:MAIN
    )

    foreach (ignore in case when baseDrug is not null
                                and ((trim(row.medication) contains baseDrug.name and not trim(row.medication) contains mainDrug.name)
                                    or trim(row.medication) is null or trim(row.medication) = '') then [1] else [] end |
        create (rxexp)-[:OF_DRUG]->(baseDrug)
        set rxexp:BASE
    )

    foreach (ignore in case when baseDrug is not null and mainDrug is not null and matchedDrug <> mainDrug
                                and matchedDrug <> baseDrug and matchedDrug is not null then [] else [] end |
        create (rxexp)-[:OF_DRUG]->(matchedDrug)
        set rxexp:ADDITIVE
    )

    with row, rxexp, matchedDrug
    where ((mainDrug is null and baseDrug is null) or (not trim(row.medication) contains mainDrug.name and not trim(row.medication) contains baseDrug.name)) and matchedDrug is not null
    create (rxexp)-[:OF_DRUG]->(matchedDrug)
    set rxexp:MAIN
}