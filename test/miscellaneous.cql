match (n:Miscellaneous)
with count (n) as total
match (n:Miscellaneous)-[:OF_ITEM]->(lc),
      (parent:Stop)-[]->(n),
      (n)-[:CHARTED]->(anc)
using scan n:Miscellaneous
with total, n,
    case
      when count(lc) = 1 and count(parent) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate
