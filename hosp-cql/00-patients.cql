:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/patients.csv" AS row
CALL {
    WITH row
    WITH row,
        toInteger(split(row.anchor_year_group, " - ")[0]) as p3,
        toInteger(row.anchor_year) as fVal

    with row, fVal, p3,
        p3 - 1 as p2,
        p3 - 2 as p1,
        p3 - 3 as p0

    WITH row,
        case
            when p3 % 4 = fVal % 4 then p3
            when p2 % 4 = fVal % 4 then p2
            when p1 % 4 = fVal % 4 then p1
            when p0 % 4 = fVal % 4 then p0
            else p3
        end as fixedYearVal

    WITH row,
        duration( { years: toInteger(row.anchor_year) - fixedYearVal } ) as shiftedYears,
        datetime( { year:fixedYearVal} ) as fixedYear

    WITH row, shiftedYears, fixedYear,
        fixedYear - duration({years:toInteger(row.anchor_age)}) as birthday,
        CASE
            WHEN row.dod IS NOT NULL AND row.dod <> ""
                THEN datetime(row.dod) - shiftedYears
            ELSE null END AS deathday

    MERGE (p:Patient:BIDMC {id:toInteger(row.subject_id)})
    SET p.ssn = "000-00-0000",
        p.firstName = "Pfirst",
        p.lastName = "Plast " + row.subject_id,
        p.shiftedYears = shiftedYears

    // MERGE (ca:Case {id:row.subject_id + '-01'})
    // MERGE (p)-[:HAS_CASE]->(ca)
    with row, p, deathday, birthday
    match (rs:RefSet {id:1102})
    match (dobItem:LocalConcept {id:rs.code + '|' + "DOB"})
    match (dodItem:LocalConcept {id:rs.code + '|' + "DOD"})

    // create birthday anchor
    MERGE (bac:Anchor {id:row.subject_id + "-" +  birthday.epochSeconds})
        on create
        set bac.value = birthday
    create (dob:PersInfo:BirthDate:EXISTENT)-[:OF_ITEM]->(dobItem)
    create (dob)-[:HAPPENED]->(bac)
    create (p)-[:HAS_PERSINF]->(dob)


    // create deathday anchor
    foreach (ignore in case when deathday is not null then [1] else [] END |
        MERGE (dac:Anchor {id:row.subject_id + "-" +  deathday.epochSeconds})
            on create
            set dac.value = deathday
        create (dod:PersInfo:DeathDate:EXISTENT)-[:OF_ITEM]->(dodItem)
        create (dod)-[:HAPPENED]->(dac)
        create (p)-[:HAS_PERSINF]->(dod)
    )

    // create gender concept
    WITH row, p
    MATCH (rs:RefSet {id:1101})

    WITH row, p, rs,
        row.gender as gender_code,
        CASE WHEN row.gender = "F" THEN "Female" ELSE "Male" END AS gender_name

    MERGE (lc:LocalConcept {id:rs.code + '|' + gender_code})
    SET lc:Item,
        lc.code = gender_code,
        lc.name = gender_name,
        lc.source = "custom::otmv/persinf/gender"
    WITH row, p, rs, lc
    MERGE (lc)-[:BELONGS_TO]->(rs)
    CREATE (persInf:PersInfo:Gender:EXISTENT)-[:OF_ITEM]->(lc)
    CREATE (p)-[:HAS_PERSINF]->(persInf)
} in transactions of 5000 rows
