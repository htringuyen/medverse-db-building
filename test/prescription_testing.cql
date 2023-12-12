match (n:Prescription)
with count (n) as total
match (n:Prescription)-[:OF_DRUG]->(drug),
      (parent)-[:HAS_SEQ]->(n)
with total, n,
    case
      when count(drug) = 1 and count(parent) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate
