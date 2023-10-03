// constraints for "Concept" label
CREATE CONSTRAINT concept_id_key IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepId) IS NODE KEY;
CREATE CONSTRAINT concept_id_type IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepId) IS ::INTEGER;

//CREATE CONSTRAINT concept_name_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepName) IS NOT NULL;
CREATE CONSTRAINT concept_name_type IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepName) IS ::STRING;

//CREATE CONSTRAINT concept_code_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepCode) IS NOT NULL;
CREATE CONSTRAINT concept_code_type IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepCode) IS ::STRING;

//CREATE CONSTRAINT concept_valid_start_date_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.validStartDate) IS NOT NULL;
CREATE CONSTRAINT concept_valid_start_date_type IF NOT EXISTS FOR (n:Concept) REQUIRE (n.validStartDate) IS ::DATE;

//CREATE CONSTRAINT concept_valid_end_date_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.validEndDate) IS NOT NULL;
CREATE CONSTRAINT concept_valid_end_date_type IF NOT EXISTS FOR (n:Concept) REQUIRE (n.validEndDate) IS ::DATE;

CREATE CONSTRAINT concept_invalid_reason_type IF NOT EXISTS FOR (n:Concept) REQUIRE (n.invalidReason) IS ::STRING;

// constraints for "VOCABULARY" label
CREATE CONSTRAINT vocabulary_id_key IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocId) IS NODE KEY;
CREATE CONSTRAINT vocabulary_id_type IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocId) IS ::STRING;

CREATE CONSTRAINT vocabulary_name_exists IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocName) IS NOT NULL;
CREATE CONSTRAINT vocabulary_name_type IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocName) IS ::STRING;

CREATE CONSTRAINT vocabulary_reference_exists IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocRef) IS NOT NULL;
CREATE CONSTRAINT vocabulary_reference_type IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocRef) IS ::STRING;

CREATE CONSTRAINT vocabulary_version_type IF NOT EXISTS FOR (n:Vocabulary) REQUIRE (n.vocVersion) IS ::STRING;

// constraints for "DOMAIN" label
CREATE CONSTRAINT domain_id_key IF NOT EXISTS FOR (n:Domain) REQUIRE (n.domainId) IS NODE KEY;
CREATE CONSTRAINT domain_id_type IF NOT EXISTS FOR (n:Domain) REQUIRE (n.domainId) IS ::STRING;

CREATE CONSTRAINT domain_name_exists IF NOT EXISTS FOR (n:Domain) REQUIRE (n.domainName) IS NOT NULL;
CREATE CONSTRAINT domain_name_type IF NOT EXISTS FOR (n:Domain) REQUIRE (n.domainName) IS ::STRING;

// constraints for "CONCEPT_CLASS" label
CREATE CONSTRAINT concept_class_id_key IF NOT EXISTS FOR (n:ConcepClass) REQUIRE (n.classId) IS NODE KEY;
CREATE CONSTRAINT concept_class_id_type IF NOT EXISTS FOR (n:ConcepClass) REQUIRE (n.classId) IS ::STRING;

CREATE CONSTRAINT concept_class_name_exsits IF NOT EXISTS FOR (n:ConcepClass) REQUIRE (n.className) IS NOT NULL;
CREATE CONSTRAINT concept_class_name_type IF NOT EXISTS FOR (n:ConcepClass) REQUIRE (n.className) IS ::STRING;

// constraints for "RELATIONSHIP" label
CREATE CONSTRAINT relationship_id_key IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.relId) IS NODE KEY;
CREATE CONSTRAINT relationship_id_type IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.relId) IS ::STRING;

CREATE CONSTRAINT relationship_name_exists IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.relName) IS NOT NULL;
CREATE CONSTRAINT relationship_name_type IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.relName) IS ::STRING;

CREATE CONSTRAINT relationship_is_hier_exists IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.isHier) IS NOT NULL;
CREATE CONSTRAINT relationship_is_hier_type IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.isHier) IS ::BOOLEAN;

CREATE CONSTRAINT relationship_def_ancestry_exists IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.defAncestry) IS NOT NULL;
CREATE CONSTRAINT relationship_def_ancestry_type IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.defAncestry) IS ::BOOLEAN;

CREATE CONSTRAINT reverse_relationship_id_exists IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.revRelId) IS NOT NULL;
CREATE CONSTRAINT reverse_relationship_id_type IF NOT EXISTS FOR (n:Relationship) REQUIRE (n.revRelId) IS ::STRING;


// constraint for "ConceptSynonym" label
CREATE CONSTRAINT concept_synonym_name_key IF NOT EXISTS FOR (n:Synonym) REQUIRE (n.synonymName) IS NODE KEY;
CREATE CONSTRAINT concept_synonym_name_type IF NOT EXISTS FOR (n:Synonym) REQUIRE (n.synonymName) IS ::STRING;

// constraint for "IngStrength" label
CREATE CONSTRAINT box_size_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.boxSize) IS ::INTEGER;

CREATE CONSTRAINT valid_start_date_exists IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.validStartDate) IS NOT NULL;
CREATE CONSTRAINT valid_start_date_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.validStartDate) IS ::DATE;

CREATE CONSTRAINT valid_end_date_exists IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.validEndDate) IS NOT NULL;
CREATE CONSTRAINT valid_end_date_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.validEndDate) IS ::DATE;

CREATE CONSTRAINT invalid_reason_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.invalidReason) IS ::STRING;

CREATE CONSTRAINT amout_value_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.amoutValue) IS ::FLOAT;

CREATE CONSTRAINT numerator_value_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.numerValue) IS ::FLOAT;

CREATE CONSTRAINT denominator_value_type IF NOT EXISTS FOR (n:IngStrength) REQUIRE (n.denomValue) IS ::FLOAT;
