// test visit start and end time correctness
match (n:Visit)
with count(n) as numVis
match ()-[r:VIS_STARTED]->()
with numVis, count(r) as numStartAnc
match ()-[r:VIS_ENDED]->()
with numVis, numStartAnc, count(r) as numEndAnc
return "Visit timing: " + case when numVis = numStartAnc and numVis = numEndAnc then "PASS" else "FAILED" end
// end test
;





