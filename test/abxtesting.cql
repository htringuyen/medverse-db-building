match (n:AbxTesting)
with count (n) as total
match (n:AbxTesting)-[:OF_ITEM]->(item),
      (parent)-[:HAS_CONT]->(n),
      (n)-[:CHARTED]->(anc)
with total, n,
    case
      when count(item) = 1 and count(parent) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate