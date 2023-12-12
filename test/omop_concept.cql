match (n:Concept)
with count(n) as total
match (n:Concept)-[:OF_VOCAB]->(v:Vocabulary),
      (n:Concept)-[:OF_DOMAIN]->(d:Domain),
      (n:Concept)-[:OF_CLASS]->(c:ConceptClass)
using scan n:Concept
with n, total,
      case when count(v) = 1 and count(d) = 1 and count(c) = 1 then 1 else 0 end as success
with total, sum(success) as success
return total, success, success / toFloat(total) as ratio
