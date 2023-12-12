:auto
PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/CONCEPT_RELATIONSHIP.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    MATCH (c1:Concept {id:toInteger(row.concept_id_1)}),
          (c2:Concept {id:toInteger(row.concept_id_2)}),
          (refRel:Relationship {relId:row.relationship_id})
    WITH row, c1, c2, refRel,
            case when row.invalidReason is not null and row.invalidReason <> '' then row.invalidReason else null end as invalid_reason
    create (c1)-[r:__RELATES_TO]->(c2)
        set r.refId = refRel.id,
            r.validStartDate = date(row.valid_start_date),
            r.validEndDate = date(row.valid_end_date),
            r.invalidReason = invalid_reason
} IN TRANSACTIONS OF 5000 ROWS
