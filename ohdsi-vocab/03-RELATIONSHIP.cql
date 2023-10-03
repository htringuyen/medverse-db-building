PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/RELATIONSHIP.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    WITH row,
        CASE WHEN row.is_hierarchical = '1' THEN true ELSE false END AS is_hierarchical,
        CASE WHEN row.defines_ancestry = '1' THEN true ELSE false END AS defines_ancestry
    MERGE (r:Relationship {relId: row.relationship_id})
        ON CREATE
        SET r:Concept,
            r.cepId = toInteger(row.relationship_concept_id),
            r.relName = row.relationship_name,
            r.isHier = is_hierarchical,
            r.defAncestry = defines_ancestry,
            r.revRelId = row.reverse_relationship_id
}
