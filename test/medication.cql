match (med:Medication)
with count (med) as total
match (med:Medication)-[:OF_ITEM]->(item),
      (stop)-[:HAS_MED]->(med),
      (med)-[:MED_STARTED]->(started),
      (med)-[:MED_ENDED]->(ended)
with total, med,
    case
      when count(item) = 1 and count(stop) = 1 and count(started) = 1 and count(ended) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / total as rate
