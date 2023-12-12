match (n:Patient)-[:HAS_TIMELINE]->(t)
with n, t
skip 1000
limit 1
match (t)-[:HEAD]->(head), (t)-[:TAIL]->(tail)
match p = shortestPath((head)-[:NEXT*]->(tail))
with nodes(p) as anchors
unwind anchors as anc
match (thing)-[:CHARTED|STARTED|ENDED]->(anc)
optional match (thing)-[:OF_ITEM|OF_DRUG]->(lc)
with anc, collect([thing, lc]) as bundle
return anc, bundle