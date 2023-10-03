:auto
profile
match (ls:LabSampling)-[:MEAS_CHARTED]->(chartedAnc)
call {
  with ls, chartedAnc
  merge (vis:Visit {id:ls.visitId})
    on create
    set vis:OUTPATIENT,
        ls.visNewCreated = true

  foreach (ignore in case when ls.visNewCreated is not null then [1] else [] end |
    // create vis-stop-meas
    merge (ca:Case {id: split(chartedAnc.id, '-')[0] + '-01'})
    merge (ca)-[:HAS_VIS]->(vis)
    merge (stop:Stop {id: ls.visitId})
      on create
      set stop:NonStay
    merge (vis)-[:HAS_STOP]->(stop)
    merge (stop)-[:HAS_MEAS]->(ls)

    // create anchors
    merge (startAnc:Anchor {id: ls.visitId})
      on create
      set startAnc.value = datetime({epochseconds: toInteger(split(ls.visitId, '-')[1])})

    merge (endAnc: Anchor {id: split(ls.visitId, '-')[0] + '-' + (startAnc.value.epochSeconds + 1800)})
      on create
      set endAnc.value = datetime({epochseconds: startAnc.value.epochSeconds + 1800})

    merge (stop)-[:STOP_STARTED]->(startAnc)
    merge (stop)-[:STOP_ENDED]->(endAnc)

    merge (vis)-[:VIS_STARTED]->(startAnc)
    merge (vis)-[:VIS_ENDED]->(endAnc)
  )

  with ls, vis, chartedAnc,
      case when ls.visNewCreated is null then true else false end as continue
  remove ls.visNewCreated
  with ls, vis, chartedAnc
  where continue

  match (vis)-[:HAS_STOP]->(stop),
        (stop)-[:STOP_STARTED]->(startAnc),
        (stop)-[:STOP_ENDED]->(endAnc)

  with ls, stop
  where chartedAnc.value >= startAnc.value and chartedAnc.value < endAnc.value
  merge (stop)-[:HAS_MEAS]->(ls)
} in transactions of 5000 rows

