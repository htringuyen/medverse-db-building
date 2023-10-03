match (vis)-[:HAS_STOP]->(stop:Stop),
      (stop)-[:STOP_STARTED]->(startAnc),
      (stop)-[:STOP_ENDED]->(endAnc)
with stop,
     case
        when count(vis) = 1 and count(startAnc) = 1 and (count(endAnc) = 1 or "DISCHARGED" in labels(stop))
        then "PASSED"
        else "FAILED"
        end as testResult
return testResult, count(testResult) as count
