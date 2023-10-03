call apoc.periodic.iterate("match ()-[r]-() return r", "delete r", {batchSize:5000, parallel:true, concurrency:10});
call apoc.periodic.iterate("match (n) return n", "delete n", {batchSize:5000, parallel:true, concurrency:10});
call apoc.schema.assert({},{});