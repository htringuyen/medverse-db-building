profile
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/concepts/d_type_note.csv" AS row
call {
    with row
    match (rs:RefSet {id:2501})
    merge (lc:LocalConcept {id: rs.code + '|' + row.code})
      on create
      set lc:Item,
          lc.code = row.code,
          lc.name = row.name,
          lc.source = "dataset::mimiciv/2.2/note/notetype"
    merge (lc)-[:BELONGS_TO]->(rs)
}