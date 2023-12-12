match (n:EraSummary)
with count (n) as total
match (n:EraSummary)-[:OF_ITEM]->(lc),
      (parent)-[:HAS_NOTE]->(n)
with total, n,
    case
      when count(lc) = 1 and count(parent) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate
