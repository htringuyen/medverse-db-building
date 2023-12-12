call apoc.periodic.iterate("match ()-[r]-() return r", "delete r", {batchSize:5000, parallel:true, concurrency:10});
call apoc.periodic.iterate("match (n) return n", "delete n", {batchSize:5000, parallel:true, concurrency:10});
call apoc.schema.assert({},{});


unwind range(1, 10) as i
with i
call apoc.periodic.iterate("match ()-[r]-() return r", "delete r", {batchSize:5000, parallel:true, concurrency:10})
yield total
return total
;