:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/transfers.csv" AS row
call {
    with row
    match (pa:Patient {id: toInteger(row.subject_id)})
    match (ca:Case {id: row.subject_id + '-01'})
    with row, pa, ca,
         datetime({epochseconds: apoc.date.parse(row.intime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as intime

    foreach (ignore in case when row.careunit is null or row.careunit = '' then [1] else [] end |
        merge (vis:Visit {id: row.hadm_id})
        merge (ca)-[:HAS_VIS]->(vis)
        create (vis)-[:HAS_STOP]->(stop:Stop {id: row.transfer_id})
        set stop:DCStay:DISCHARGED
        merge (intime_anchor:Anchor {id: row.subject_id + '-' + intime.epochSeconds})
            on create
            set intime_anchor.value = datetime({epochseconds: apoc.date.parse(row.intime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears
        create (stop)-[:STOP_STARTED]->(intime_anchor)
    )

    with row, ca, pa
    where row.careunit is not null and row.careunit <> ''

    with row, ca,
        datetime({epochseconds: apoc.date.parse(row.intime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as intime,
        datetime({epochseconds: apoc.date.parse(row.outtime, 's', 'yyyy-MM-dd HH:mm:ss')}) - pa.shiftedYears as outtime
    with row, ca, intime, outtime,
        case when row.hadm_id is not null and row.hadm_id <> '' then row.hadm_id else row.subject_id + '-' + intime.epochSeconds
        end as visit_id

    merge (vis:Visit {id: visit_id})
        on match
        set vis.newCreated = false
        on create
        set vis.newCreated = true

    with row, ca, intime, outtime, vis,
        case when vis.newCreated then "CONTINUE" else "STOP" end as action
    remove vis.newCreated
    with row, ca, intime, outtime, vis
    where action = "CONTINUE" or (row.hadm_id is not null and row.hadm_id <> '')

    foreach (ignore in case when row.hadm_id is not null and row.hadm_id <> '' then [1] else [] end |
        set vis:INPATIENT
    )
    foreach (ignore in case when row.careunit = 'Emergency Department' then [1] else [] end |
        set vis:EMERGENCY
    )

    merge (ca)-[:HAS_VIS]->(vis)

    with row, vis, intime, outtime,
        case
            when row.careunit = 'Emergency Department' then 'ERStay'
            when row.careunit = 'Emergency Department Observation' then 'ERStay'
            when row.careunit contains 'ICU' then 'ICUStay'
            else 'WardStay'
        end as stay_type,
        case
            when row.eventtype = 'ED' then 'EDED'
            when row.eventtype = 'admit' then 'ADMITTED'
            when row.eventtype = 'transfer' then 'TRANSFERRED'
            when row.eventtype = 'discharge' then 'DISCHARGED'
            else 'UNKNOWN'
        end as event_type

    // create the stop
    create (stop:Stop {id: row.transfer_id})
    create (vis)-[:HAS_STOP]->(stop)

    with row, vis, intime, outtime, stop, stay_type, event_type
    call apoc.create.setLabels(stop, ["Stop", stay_type, event_type])
    yield node as ignore

    with row, vis, intime, outtime, stop
    // intime and outtime anchors for stop
    merge (intime_anchor:Anchor {id: row.subject_id + '-' + intime.epochSeconds})
        on create
        set intime_anchor.value = intime
    create (stop)-[:STOP_STARTED]->(intime_anchor)
    merge (outtime_anchor:Anchor {id: row.subject_id + '-' + outtime.epochSeconds})
        on create
        set outtime_anchor.value = outtime
    create (stop)-[:STOP_ENDED]->(outtime_anchor)

    // intime and outtime anchors for visit if it is not inpatient
    foreach( ignore in case when (row.hadm_id is null or row.hadm_id = '')  then [1] else [] end |
        merge (vis)-[:VIS_STARTED]->(intime_anchor)
        merge (vis)-[:VIS_ENDED]->(outtime_anchor)
    )

    // create care site local concept
    with row, stop
    match (rs:RefSet {id:1301})
    merge (lc:LocalConcept {id: rs.code + '|' + row.careunit})
        on create
        set lc:Item,
            lc.code = row.careunit,
            lc.name = row.careunit,
            lc.source = "custom::otmv/stopinf/careunit"
    merge (lc)-[:BELONGS_TO]->(rs)
    create (stopInf:StopInfo:CareUnit:EXISTENT)-[:OF_ITEM]->(lc)
    create (stop)-[:HAS_SINF]->(stopInf)
} in transactions of 5000 rows