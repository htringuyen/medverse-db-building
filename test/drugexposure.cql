match (n:DrugExposure)
with count (n) as total
match (n:DrugExposure)-[:OF_DRUG]->(drug),
      (parent)-[:HAS_SEQ]->(n)
with total, n,
    case
      when count(drug) = 1 and count(parent) = 1 and ("MAIN" in labels(n) or "BASE" in labels(n) or "ADDITIVE" in labels(n))
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate