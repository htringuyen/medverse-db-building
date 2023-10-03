:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/patients.csv" AS row
CALL {
    WITH row
    WITH row,
        toInteger(split(row.anchor_year_group, " - ")[1]) as pVal,
        toInteger(row.anchor_year) as fVal

    WITH row,
        case
            when fVal % 4 = 0 and fVal % 100 != 0 then pVal - (pVal % 4)
            when fVal % 400 = 0 then pVal - (pVal % 400)
            else pVal
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

    MERGE (ca:Case {id:row.subject_id + '-01'})
    MERGE (p)-[:HAS_CASE]->(ca)

    // create birthday anchor
    MERGE (bac:Anchor {id:row.subject_id + "-" +  birthday.epochSeconds})
        on create
        set bac.value = birthday
    MERGE (p)-[:BIRTHED]->(bac)

    // create deathday anchor
    foreach (ignore in case when deathday is not null then [1] else [] END |
        MERGE (dac:Anchor {id:row.subject_id + "-" +  deathday.epochSeconds})
            on create
            set dac.value = deathday
        MERGE (p)-[:DIED]->(dac)
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
    CREATE (p)-[:HAS_PINF]->(persInf)
} in transactions of 5000 rows







