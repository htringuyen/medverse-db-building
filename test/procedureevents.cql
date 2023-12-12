match (n:Procedure:Miscellaneous:NUMERIC)
with count (n) as total
match (n:Procedure:Miscellaneous:NUMERIC)-[:OF_ITEM]->(lc),
      (parent)-[:HAS_PROC]->(n),
      (n)-[:STARTED]->(startAnc),
      (n)-[:ENDED]->(endAnc)
with total, n,
    case
      when count(lc) = 1 and count(parent) = 1 and count(startAnc) = 1 and count(endAnc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate