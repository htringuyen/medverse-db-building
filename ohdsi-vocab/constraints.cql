/* for Concept */
create constraint concept_id_key if not exists for (n:Concept) require n.id is node key;
create constraint concept_id_type if not exists for (n:Concept) require n.id is ::integer;
create constraint concept_code_exists if not exists for (n:Concept) require n.code is not null;
create constraint concept_code_type if not exists for (n:Concept) require n.code is ::string;
create constraint concept_name_exists if not exists for (n:Concept) require n.name is not null;
create constraint concept_name_type if not exists for (n:Concept) require n.name is ::string;
create constraint concept_valid_start_date_exists if not exists for (n:Concept) require n.validStartDate is not null;
create constraint concept_valid_start_date_type if not exists for (n:Concept) require n.validStartDate is ::date;
create constraint concept_valid_end_date_exists if not exists for (n:Concept) require n.validEndDate is not null;
create constraint concept_valid_end_date_type if not exists for (n:Concept) require n.validEndDate is ::date;
create constraint concept_invalid_reason_type if not exists for (n:Concept) require n.invalidReason is ::string;

/* for Vocabulary */
create constraint vocabulary_vocab_id_key if not exists for (n:Vocabulary) require n.vocabId is node key;
create constraint vocabulary_vocab_id_type if not exists for (n:Vocabulary) require n.vocabId is ::string;
create constraint vocabulary_vocab_name_exists if not exists for (n:Vocabulary) require n.vocabName is node key;
create constraint vocabulary_vocab_name_type if not exists for (n:Vocabulary) require n.vocabName is ::string;
create constraint vocabulary_vocab_reference_exists if not exists for (n:Vocabulary) require n.vocabReference is not null;
create constraint vocabulary_vocab_reference_type if not exists for (n:Vocabulary) require n.vocabReference is ::string;
create constraint vocabulary_vocab_version_type if not exists for (n:Vocabulary) require n.vocabVersion is ::string;


/* for Domain */
create constraint domain_domain_id_key if not exists for (n:Domain) require n.domainId is node key;
create constraint domain_domain_id_type if not exists for (n:Domain) require n.domainId is ::string;
create constraint domain_domain_name_exists if not exists for (n:Domain) require n.domainName is not null;
create constraint domain_domain_name_type if not exists for (n:Domain) require n.domainName is ::string;

/* for Relationship */
create constraint relationship_id_key if not exists for (n:Relationship) require n.relId is node key;
create constraint relationship_id_type if not exists for (n:Relationship) require n.relId is ::string;
create constraint relationship_name_exists if not exists for (n:Relationship) require n.relName is not null;
create constraint relationship_name_type if not exists for (n:Relationship) require n.relName is ::string;
create constraint relationship_reverse_rel_id_exists if not exists for (n:Relationship) require n.reverseRelId is not null;
create constraint relationship_reverse_rel_id_type if not exists for (n:Relationship) require n.reverseRelId is ::string;
create constraint relationship_is_hierarchical_exists if not exists for (n:Relationship) require n.isHierarchical is not null;
create constraint relationship_is_hierarchical_type if not exists for (n:Relationship) require n.isHierarchical is ::boolean;
create constraint relationship_defines_ancestry_exists if not exists for (n:Relationship) require n.definesAncestry is not null;
create constraint relationship_defines_ancestry_type if not exists for (n:Relationship) require n.definesAncestry is ::boolean;

/* for ConceptClass */
create constraint concept_class_id_key if not exists for (n:ConceptClass) require n.classId is node key;
create constraint concept_class_id_type if not exists for (n:ConceptClass) require n.classId is ::string;
create constraint concept_class_name_exists if not exists for (n:ConceptClass) require n.className is not null;
create constraint concept_class_name_type if not exists for (n:ConceptClass) require n.className is ::string;


/* for ONTO_REL */
create constraint __relates_to_ref_id_exists if not exists for ()-[r:__RELATES_TO]->() require r.refId is not null;
create constraint __relates_to_ref_id_type if not exists for ()-[r:__RELATES_TO]->() require r.refId is ::integer;
create constraint __relates_to_valid_start_date_exists if not exists for ()-[r:__RELATES_TO]->() require r.validStartDate is not null;
create constraint __relates_to_valid_start_date_type if not exists for ()-[r:__RELATES_TO]->() require r.validStartDate is ::date;
create constraint __relates_to_valid_end_date_exists if not exists for ()-[r:__RELATES_TO]->() require r.validEndDate is not null;
create constraint __relates_to_valid_end_date_type if not exists for ()-[r:__RELATES_TO]->() require r.validEndDate is ::date;
create constraint __relates_to_invalid_reason_type if not exists for ()-[r:__RELATES_TO]->() require r.invalidReason is ::string;

/* for Synonym */
create constraint synonym_id_text_key if not exists for (n:Synonym) require n.idText is node key;
create constraint synonym_id_text_type if not exists for (n:Synonym) require n.idText is ::string;

/* for IngredientStrength */
create constraint ingredient_strength_amount_value_type if not exists for (n:IngredientStrength) require n.amountValue is ::float;
create constraint ingredient_strength_amount_unit_id_type if not exists for (n:IngredientStrength) require n.amountUnitId is ::integer;
create constraint ingredient_strength_denominator_value_type if not exists for (n:IngredientStrength) require n.denominatorValue is ::float;
create constraint ingredient_strength_denominator_unit_id_type if not exists for (n:IngredientStrength) require n.denominatorUnitId is ::integer;
create constraint ingredient_strength_numerator_value_type if not exists for (n:IngredientStrength) require n.numeratorValue is ::float;
create constraint ingredient_strength_numerator_unit_id_type if not exists for (n:IngredientStrength) require n.numeratorUnitId is ::integer;
create constraint ingredient_strength_valid_start_date_exists if not exists for (n:IngredientStrength) require n.validStartDate is not null;
create constraint ingredient_strength_valid_start_date_type if not exists for (n:IngredientStrength) require n.validStartDate is ::date;
create constraint ingredient_strength_valid_end_date_exists if not exists for (n:IngredientStrength) require n.validEndDate is not null;
create constraint ingredient_strength_valid_end_date_type if not exists for (n:IngredientStrength) require n.validEndDate is ::date;
create constraint ingredient_strength_invalid_reason_type if not exists for (n:IngredientStrength) require n.invalidReason is ::string;
create constraint ingredient_box_size_type if not exists for (n:IngredientStrength) require n.boxSize is ::integer;






























