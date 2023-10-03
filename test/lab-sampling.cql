match (ls:LabSampling)
with count (ls) as nLs
match (ls:LabSampling)-[:OF_ITEM]->(item),
      (stop)-[:HAS_MEAS]->(ls),
      (ls)-[:MEAS_CHARTED]->(anc)
with nLs, ls,
    case
      when count(item) = 1 and count(stop) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return nLs as total, sum(success) as success, sum(success) / nLs as rate
