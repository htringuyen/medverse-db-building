match (n:Medication)
with count (n) as total
match (n:Medication)-[:OF_ITEM]->(item),
      (parent)-[:HAS_MED]->(n),
      (n)-[:STARTED]->(sa),
      (n)-[:ENDED]->(ea)
with total, n,
    case
      when count(item) = 1 and count(parent) = 1 and count(sa) = 1 and count(ea) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate
