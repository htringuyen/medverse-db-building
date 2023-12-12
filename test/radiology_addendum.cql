match (n:EventReport)
where n.id contains "AR"
with count (n) as total
match (n:EventReport)-[:OF_ITEM]->(lc),
      (parent)-[:HAS_SEQ]->(n),
      (n)-[:CHARTED]->(anc)
where n.id contains "AR"
with total, n,
    case
      when count(lc) = 1 and count(parent) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate