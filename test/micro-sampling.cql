match (meas:MicroSampling)
with count (meas) as nMeas
match (meas:MicroSampling)-[:OF_ITEM]->(item),
      (stop)-[:HAS_MEAS]->(meas),
      (meas)-[:MEAS_CHARTED]->(anc)
with nMeas, meas,
    case
      when count(item) = 1 and count(stop) = 1 and count(anc) = 1
      then 1
      else 0
    end as success
return nMeas as total, sum(success) as success, sum(success) / nMeas as rate
