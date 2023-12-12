// test visit start and end time correctness
match (n:Visit)
with count(n) as total
with total
match (parent)-[:HAS_VISIT]->(n:Visit),
      (n)-[:STARTED]->(sa),
      (n)-[:ENDED]->(ea)
with total, n,
      case
        when count(parent) = 1 and count(sa) = 1 and count(ea) = 1
        then 1
        else 0
        end as success
return total, sum(success) as n_success, sum(success) / toFloat(total) as rate

// end test
;