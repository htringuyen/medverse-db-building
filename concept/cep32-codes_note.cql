profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_codes_note.csv" AS row
call {
    with row
    match (rs:RefSet {id:2502})
    merge (lc:LocalConcept {id:rs.code + '|' + row.exam_code})
      on create
      set lc:Item,
          lc.code = row.exam_code,
          lc.name = row.exam_name,
          lc.source = "dataset::mimiciv/2.2/note/radiologyterms"
    merge (lc)-[:BELONGS_TO]->(rs)
}