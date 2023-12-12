:auto
profile
match (ls:LabSampling)-[:CHARTED]->(chartedAnc)
where not exists( ()-[:HAS_MEAS]->(ls) )
call {
  with ls, chartedAnc
  match (pa:Patient {id:ls.subjectId})

  with ls, chartedAnc, pa
  optional match (pa)-[:HAS_VISIT]->()-[:HAS_STOP]->(stop)-[:STARTED]->(startAnc),
                  (stop)-[:ENDED]->(endAnc)
  where chartedAnc.value >= startAnc.value and chartedAnc.value < endAnc.value

  with ls, chartedAnc, stop

  limit 1 // check point

  foreach (ignore in case when stop is not null then [1] else [] end |
    create (stop)-[:HAS_MEAS]->(ls)
  )

  with ls, chartedAnc
  where stop is null
  match (pa:Patient {id:ls.subjectId})

  with ls, chartedAnc, pa
  // create visit and stop
  merge (vis:Visit {id:ls.subjectId + "-" + chartedAnc.value.epochSeconds})
      on create
      set vis:OUTPATIENT,
          vis.newCreated = true
  merge (stop:Stop {id:ls.subjectId + "-" + chartedAnc.value.epochSeconds})
      on create
      set stop:NonStay

  // link to meas
  create (stop)-[:HAS_MEAS]->(ls)

  with ls, chartedAnc, vis, stop, pa
  where vis.newCreated is not null
  // create start and end anchors
  match (startAnc:Anchor {id:ls.subjectId + "-" + chartedAnc.value.epochSeconds})

  merge (endAnc:Anchor {id:ls.subjectId + "-" + (chartedAnc.value.epochSeconds + 60)})
      on create
      set endAnc.value = chartedAnc.value + duration({seconds: 60})

  with ls, vis, stop, startAnc, endAnc, pa
  remove vis.newCreated
  create (pa)-[:HAS_VISIT]->(vis)
  create (vis)-[:HAS_STOP]->(stop)
  create (vis)-[:STARTED]->(startAnc)
  create (vis)-[:ENDED]->(endAnc)
  create (stop)-[:STARTED]->(startAnc)
  create (stop)-[:ENDED]->(endAnc)
} in transactions of 5000 rows

