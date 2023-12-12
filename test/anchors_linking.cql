:auto
match (pa:Patient)-[:HAS_TIMELINE]->(t)
with pa
call {
    with pa
    match (anc:Anchor)
    where anc.id starts with pa.id + "-"
    with pa, anc
    order by anc.id
    with pa, collect(anc) as sortedAnchors

    with pa, sortedAnchors
    match (pa)-[:HAS_TIMELINE]->(t),
            (t)-[:TAIL]->(tail),
            (t)-[:HEAD]->(head)
    match p = shortestPath( (head)-[:NEXT*]->(tail) )

    with pa,
          case when sortedAnchors = nodes(p) then 1 else 0 end as success
    return count(pa) as n_pa, sum(success) as n_succ
} in transactions of 1000 rows
with sum(n_pa) as total, sum(n_succ) as success
return total, success, success / toFloat(total) as rate
