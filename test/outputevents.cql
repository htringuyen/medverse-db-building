match (n:Measurement:Miscellaneous)
with count (n) as total
match (n:Measurement:Miscellaneous)-[:OF_ITEM]->(lc),
      (parent)-[:HAS_MEAS]->(n),
      (n)-[:CHARTED]->(anc)
with total, n,
    case
      when count(lc) = 1 and count(parent) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate