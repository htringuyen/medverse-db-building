CREATE CONSTRAINT concept_name_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepName) IS NOT NULL;

CREATE CONSTRAINT concept_code_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.cepCode) IS NOT NULL;

CREATE CONSTRAINT concept_valid_start_date_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.validStartDate) IS NOT NULL;

CREATE CONSTRAINT concept_valid_end_date_exists IF NOT EXISTS FOR (n:Concept) REQUIRE (n.validEndDate) IS NOT NULL;