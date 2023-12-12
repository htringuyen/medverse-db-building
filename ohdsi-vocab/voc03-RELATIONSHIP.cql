PROFILE
LOAD CSV WITH HEADERS FROM "file:///var/lib/neo4j/import/vocab-clean/RELATIONSHIP.csv" AS row
FIELDTERMINATOR '\t'
CALL {
    WITH row
    WITH row,
        CASE WHEN row.is_hierarchical = '1' THEN true ELSE false END AS is_hierarchical,
        CASE WHEN row.defines_ancestry = '1' THEN true ELSE false END AS defines_ancestry
    MERGE (n:Relationship {relId: row.relationship_id})
        ON CREATE
        SET n:Concept,
            n.id = toInteger(row.relationship_concept_id),
            n.code = 'N/A',
            n.name = 'N/A',
            n.validStartDate = date(),
            n.validEndDate = date(),
            n.relName = row.relationship_name,
            n.isHierarchical = is_hierarchical,
            n.definesAncestry = defines_ancestry,
            n.reverseRelId = row.reverse_relationship_id
}
