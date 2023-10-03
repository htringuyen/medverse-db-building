PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/mimiciv/2.2/hosp/transfers.csv" AS row
call {
    with row
    with row
    where row.outtime is not null
    merge (s:Silly)
    set s.intime = datetime({epochseconds: apoc.date.parse(row.intime, 's', 'yyyy-MM-dd HH:mm:ss')})
    set s.outtime = datetime({epochseconds: apoc.date.parse(row.outtime, 's', 'yyyy-MM-dd HH:mm:ss')})
}