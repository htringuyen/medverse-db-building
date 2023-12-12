:auto
profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/labevents.csv" AS row
// labevent-part02: ingest lab test
with row
limit 20000
call {
    with row
    with row,
        row.valuenum as valuenum_first,
        apoc.text.regexGroups(row.value, "^(\<=|\>=|\>|\<|=)([-]?[\d]+[.]?[\d]*)")[0] as valuenum_comp_group,
        case when row.valuenum is null or row.valuenum = ''
                then apoc.text.regexGroups(row.value, "^(\<=|\>=|\>|\<|=|)([-]?[\d]+[.]?[\d]*)\s*\D+")[0][2]
                else null end as valuenum_unit
    // get param
    with row,
        case
            when valuenum_first is not null and valuenum_first <> "" then valuenum_first
            when valuenum_comp_group[2] is not null and valuenum_comp_group[2] <> "" then valuenum_comp_group[2]
            when valuenum_unit is not null and valuenum_unit <> "" and row.valueuom is not null and row.valueuom <> "" then valuenum_unit
            else null
        end as valuenum,
        valuenum_comp_group[1] as comp_opr

    with row, valuenum, comp_opr

    // find text param if exists
    match (param_rs:RefSet {id:1705})
    match (opr_rs:RefSet {id:2301})
    match (unit_rs:RefSet {id:2201})
    match (flag_rs:RefSet {id:2401})

    with row, valuenum, //comp_opr, param_rs, opr_rs, unit_rs, flag_rs,
        case when comp_opr is not null and comp_opr <> "" then opr_rs.code + '|' + comp_opr else "UNDEF|ALL" end as opr_id,
        case when trim(row.dose_due_unit) is not null and trim(row.dose_due_unit) <> "" then unit_rs.code + '|' + trim(row.dose_due_unit) else "UNDEF|ALL" end as unit_id,
        case when row.flag is not null and row.flag <> "" then flag_rs.code + '|' + row.flag else "UNDEF|ALL" end as flag_id,
        case when row.priority is not null and row.priority <> "" then flag_rs.code + '|' + row.priority else "UNDEF|ALL" end as flag_id_2nd,
        case when trim(row.comments) is not null and trim(row.comments) <> "" then trim(row.comments) else "UNDEF|ALL" end as cmt_id,
        case when row.value is not null and row.value <> "" and valuenum is null then param_rs.code + '|' + row.value else "UNDEF|ALL" end as param_id

    match (param:LocalConcept {id:param_id})
    match (opr:LocalConcept {id:opr_id})
    match (unit:LocalConcept {id:unit_id})
    match (flag1:LocalConcept {id:flag_id})
    match (flag2:LocalConcept {id:flag_id_2nd})
    match (cmt:RefComment {idText:cmt_id})

    with row, valuenum, //param, opr, unit, flag1, flag2, cmt
        case when param.value <> 0 then param.value else null end as param_value,
        case when opr.value <> 0 then opr.value else null end as opr_value,
        case when unit.value <> 0 then unit.value else null end as unit_value,
        case when flag1.value <> 0 then flag1.value else null end as flag_value,
        case when flag2.value <> 0 then flag2.value else null end as flag2_value,
        case when cmt.value <> 0 then cmt.value else null end as cmt_value
    // create lab-testing measurement
    create (lt:LabTesting:Measurement)

    // set properties
    set lt.valueOpr = opr_value,
        lt.valueUnit = unit_value,
        lt.valueFlag = flag_value,
        lt.value2ndFlag = flag2_value,
        lt.valueCmt = cmt_value

    // set valuenum if possible
    foreach (ignore in case when valuenum is not null then [1] else [] end |
        set lt:NUMERIC,
            lt.valueNum = toFloat(valuenum)
    )

    // if not numeric, set text param when possible
    foreach (ignore in case when valuenum is null and param_value is not null then [1] else [] end |
        set lt:TEXTUAL,
            lt.valueText = param_value
    )

    // link to item
    with row, lt
    match (item_rs:RefSet {id:1704})
    match (item:LocalConcept {id:item_rs.code + '|' + row.itemid})
    match (ls:LabSampling {specimenId:toInteger(row.specimen_id)})-[:CHARTED]->(chartAnc)

    with row, lt, item, ls, chartAnc
    create (lt)-[:OF_ITEM]->(item)
    create (ls)-[:HAS_CONT]->(lt)
    create (lt)-[:CHARTED]->(chartAnc)
} in transactions of 5000 rows


