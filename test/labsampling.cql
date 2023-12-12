match (n:LabSampling)
with count (n) as total
match (n:LabSampling)-[:OF_ITEM]->(item),
      (stop)-[:HAS_MEAS]->(n),
      (n)-[:CHARTED]->(anc)
with total, n,
    case
      when count(item) = 1 and count(stop) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return total, sum(success) as success, sum(success) / toFloat(total) as rate
;

match (n:LabSampling)
with count (n) as total
match (n:LabSampling)-[:OF_ITEM]->(item),
      (stop)-[:HAS_MEAS]->(n),
      (n)-[:CHARTED]->(anc)
with total, n,
    case
      when count(item) = 1 and count(stop) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
with n
where success = 0
match (vis)-[:HAS_STOP]->(stop)-[:HAS_MEAS]->(n)-[:CHARTED]->(anc),
      (stop)-[:STARTED]->(startAnc),
      (stop)-[:ENDED]->(endAnc)
return vis.id, stop.id, n.specimenId, anc.value as charttime, startAnc.value as starttime, endAnc.value as endtime
order by n.specimenId
limit 20