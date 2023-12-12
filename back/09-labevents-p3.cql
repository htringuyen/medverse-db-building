:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/labevents.csv" AS row
// labevent-part02: ingest lab test
call {
    with row
    with row,
        row.valuenum as valuenum_first,
        apoc.text.regexGroups(row.value, "^(\<=|\>=|\>|\<|=)([-]?[\d]+[.]?[\d]*)")[0][2] as valuenum_comp,
        apoc.text.regexGroups(row.value, "^(\<=|\>=|\>|\<|=|)([-]?[\d]+[.]?[\d]*)\s*\D+")[0][2] as valuenum_unit
    // get param
    with row,
        case
            when valuenum_first is not null and valuenum_first <> "" then valuenum_first
            when valuenum_comp is not null and valuenum_comp <> "" then valuenum_comp
            when valuenum_unit is not null and valuenum_unit <> "" and row.valueuom is not null and row.valueuom <> "" then valuenum_unit
            else null
        end as valuenum,
        apoc.text.regexGroups(row.value, "^(\<=|\>=|\>|\<|=)([-]?[\d]+[.]?[\d]*)")[0][1] as comp_opr

    with row, valuenum, comp_opr

    // find text param if exists
    match (param_rs:RefSet {id:1705})
    match (opr_rs:RefSet {id:2301})
    match (unit_rs:RefSet {id:2201})
    match (flag_rs:RefSet {id:2401})

    with row, valuenum, comp_opr, param_rs, opr_rs, unit_rs, flag_rs

    optional match (param:LocalConcept {id:param_rs.code + '|' + row.value})

    // find operator if exists
    optional match (opr:LocalConcept {id:opr_rs.code + '|' + comp_opr})

    // find unit if exists
    optional match (unit:LocalConcept {id:unit_rs.code + '|' + row.valueuom})

    // find flag1, flag2 if exists
    optional match (flag1:LocalConcept {id:flag_rs.code + '|' + row.flag})
    optional match (flag2:LocalConcept {id:flag_rs.code + '|' + row.priority})

    // find comment if exists
    optional match (cmt:RefComment {idText:row.comments})


    with row, valuenum, comp_opr, param, opr, unit, flag1, flag2, cmt
    // create lab-testing measurement
    create (lt:LabTesting:Measurement)

    // set properties
    set lt.valueOpr = opr.value,
        lt.valueUnit = unit.value,
        lt.valueFlag = flag1.value,
        lt.value2ndFlag = flag2.value,
        lt.valueCmt = cmt.value

    // set valuenum if possible
    foreach (ignore in case when valuenum is not null then [1] else [] end |
        set lt:NUMERIC,
            lt.valueNum = toFloat(valuenum)
    )

    // if not numeric, set text param when possible
    foreach (ignore in case when valuenum is null and param is not null then [1] else [] end |
        set lt:TEXTUAL,
            lt.valueText = param.value
    )


    // link to item
    with row, lt
    match (item_rs:RefSet {id:1704})
    match (item:LocalConcept {id:item_rs.code + '|' + row.itemid})
    match (ls:LabSampling {specimenId:toInteger(row.specimen_id)})-[:MEAS_CHARTED]->(chartAnc)

    with row, lt, item, ls, chartAnc
    create (lt)-[:OF_ITEM]->(item)
    create (ls)-[:HAS_CONT]->(lt)
    create (lt)-[:CHARTED]->(chartAnc)
}





























