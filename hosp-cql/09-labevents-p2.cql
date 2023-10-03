:auto
profile
match (ls:LabSampling)-[:MEAS_CHARTED]->(chartedAnc)
call {
  with ls, chartedAnc
  match (pa:Patient {id:ls.subjectId})

  with ls, chartedAnc, pa
  optional match (pa)-[:HAS_CASE]->(ca)-[:HAS_VIS]->()-[:HAS_STOP]->(stop)-[:STOP_STARTED]->(startAnc),
                  (stop)-[:STOP_ENDED]->(endAnc)
  where chartedAnc.value >= startAnc.value and chartedAnc.value < endAnc.value

  with ls, chartedAnc, stop

  limit 1 // check point

  foreach (ignore in case when stop is not null then [1] else [] end |
    create (stop)-[:HAS_MEAS]->(ls)
  )

  with ls, chartedAnc
  where stop is null
  match (pa:Patient {id:ls.subjectId})-[:HAS_CASE]->(ca)

  with ls, chartedAnc, ca
  // create visit and stop
  merge (vis:Visit:OUTPATIENT {id:ls.subjectId + "-" + chartedAnc.value.epochSeconds})
      on create
      set vis.newCreated = true
  merge (stop:Stop:NonStay {id:ls.subjectId + "-" + chartedAnc.value.epochSeconds})

  // link to meas
  create (stop)-[:HAS_MEAS]->(ls)

  foreach (ignore in case when vis.newCreated is not null then [1] else [] end |
      remove vis.newCreated
      create (ca)-[:HAS_VIS]->(vis)
      create (vis)-[:HAS_STOP]->(stop)

      // create start and end anchors
      merge (startAnc:Anchor {id:ls.subjectId + "-" + chartedAnc.value.epochSeconds})
          on create
          set startAnc.value = chartedAnc.value
      merge (endAnc:Anchor {id:ls.subjectId + "-" + (chartedAnc.value.epochSeconds + 60)})
          on create
          set endAnc.value = chartedAnc.value + duration( {seconds: 60} )
      create (vis)-[:VIS_STARTED]->(startAnc)
      create (vis)-[:VIS_ENDED]->(endAnc)
      create (stop)-[:STOP_STARTED]->(startAnc)
      create (stop)-[:STOP_ENDED]->(endAnc)
  )
} in transactions of 5000 rows

