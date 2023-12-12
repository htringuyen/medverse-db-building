:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/admissions.csv" AS row
call {
    with row
    match (vis:Visit {id: row.hadm_id})
    with row, vis
    match (vis)-[:HAS_STOP]->(stop),
          (stop)-[:STARTED]->(startAnc),
          (stop)-[:ENDED]->(endAnc)
    with row, vis, min(startAnc.value) as minTime, max(endAnc.value) as maxTime
    match (visStartAnc:Anchor {id:row.subject_id + "-" + minTime.epochSeconds})
    match (visEndAnc:Anchor {id:row.subject_id + "-" + maxTime.epochSeconds})
    merge (vis)-[:STARTED]->(visStartAnc)
    merge (vis)-[:ENDED]->(visEndAnc)


    // create discharge location for visit if any
    with row, vis
    match (rs:RefSet {id:1305})
    foreach(ignore in case when row.discharge_location is not null and row.discharge_location <> '' then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.discharge_location})
            on create
            set lc:Item,
                lc.code = row.discharge_location,
                lc.name = row.discharge_location,
                lc.source = "custom::otmv/visinf/discharge_location"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:DcLocation:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )

    // creat admitted location for visit if any
    with row, vis
    match (rs:RefSet {id:1304})
    foreach(ignore in case when row.admission_location is not null and row.admission_location <> '' then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.admission_location})
            on create
            set lc:Item,
                lc.code = row.admission_location,
                lc.name = row.admission_location,
                lc.source = "custom::otmv/visinf/admission_location"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:AdmLocation:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )

    // create admit type for visit if any
    with row, vis
    match (rs:RefSet {id:1303})
    foreach(ignore in case when row.admission_type is not null and row.admission_type <> '' then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.admission_type})
            on create
            set lc:Item,
                lc.code = row.admission_type,
                lc.name = row.admission_type,
                lc.source = "custom::otmv/visinf/admission_type"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:AdmType:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )

    // create admission order for visit if any
    /*with row, vis
    match (prov:Provider {id: row.admit_provider_id})
    create (order:Order {id:'ADM-' + row.hadm_id})
    create (order)-[:ORDERED_BY]->(prov)
    create (vis)-[:HAS_ORDER]->(order)
    create (order)-[:ORDER_FOR]->(admStop)*/

    // create order concept
    /*with row, vis, order
    match (rs:RefSet {id:1401})
    merge (lc:LocalConcept {id: rs.code + '|' + "Admission order"})
        on create
        set lc:Item,
            lc.code = "Admission order",
            lc.name = "Admission order",
            lc.source = "custom::otmv/visinf/admission_order"
    merge (lc)-[:BELONGS_TO]->(rs)
    create (order)-[:OF_ITEM]->(lc)*/

    // create insurance for visit
    with row, vis
    match (rs:RefSet {id:1204})
    foreach(ignore in case when row.insurance is not null and row.insurance <> '' then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.insurance})
            on create
            set lc:Item,
                lc.code = row.insurance,
                lc.name = row.insurance,
                lc.source = "custom::otmv/visinf/insurance"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:Insurance:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )

    // create language for visit
    with row, vis
    match (rs:RefSet {id:1203})
    foreach(ignore in case when row.language is not null and row.language <> '' then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.language})
            on create
            set lc:Item,
                lc.code = row.language,
                lc.name = row.language,
                lc.source = "custom::otmv/visinf/language"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:Language:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )

    // create marital status for visit
    with row, vis
    match (rs:RefSet {id:1202})
    foreach(ignore in case when row.marital_status is not null and row.marital_status <> '' then [1] else [] end |
        merge (lc:LocalConcept {id: rs.code + '|' + row.marital_status})
            on create
            set lc:Item,
                lc.code = row.marital_status,
                lc.name = row.marital_status,
                lc.source = "custom::otmv/visinf/marital_status"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:MaritalStatus:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )

    // create race for visit
    with row, vis
    match (rs:RefSet {id:1201})
    foreach(ignore in case when row.race is not null and row.race <> '' then [1] else [] end |
        merge (lc: LocalConcept {id: rs.code + '|' + row.race})
        on create
        set lc:Item,
            lc.code = row.race,
            lc.name = row.race,
            lc.source = "custom::otmv/visinf/race"
        merge (lc)-[:BELONGS_TO]->(rs)
        create (visInf:VisitInfo:Race:EXISTENT)-[:OF_ITEM]->(lc)
        create (vis)-[:HAS_VISINF]->(visInf)
    )
} in transactions of 5000 rows