match (n:MicroSampling)
with count (n) as total
match (n:MicroSampling)-[:OF_ITEM]->(item),
      (parent)-[:HAS_MEAS]->(n),
      (n)-[:CHARTED]->(anc)
with total, n,
    case
      when count(item) = 1 and count(parent) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate
