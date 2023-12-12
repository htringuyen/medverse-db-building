match (n:Stop)
with count(n) as total
with total
match (parent)-[:HAS_STOP]->(n:Stop),
      (n)-[:STARTED]->(sa)
optional match (n)-[:ENDED]->(ea)
with n, total,
     case
        when count(parent) = 1 and count(sa) = 1 and (count(ea) = 1 or ("DISCHARGED" in labels(n)))
        then 1
        else 0
       end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate

;