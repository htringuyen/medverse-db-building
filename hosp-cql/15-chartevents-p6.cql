match (item:LocalConcept)
where item.id starts with "CLEAN|"
with item,
  case
      when item.chartDomain contains "Meas" then 1702
      when item.chartDomain = "Observation" then 1602
      when item.chartDomain = "Procedure" then 1902
      when item.chartDomain = "Device" then 1802
      when item.chartDomain = "Condition" then 1504
  end as param_rs_id
match (rs:RefSet {id: param_rs_id})
with item, rs
set item.id = replace(item.id, "CLEAN|", rs.code + "|")
merge (item)-[:BELONGS_TO]->(rs)
