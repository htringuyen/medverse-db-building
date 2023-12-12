profile
match (param:LocalConcept {id:"PARAM-ORGTEST|" + 80002})<-[:ASSOC_PARAM]-(item)
with param.value as lookup_value, item
match (item)<-[:OF_ITEM]-(meas)
where meas.valueText = lookup_value
return count(meas)
;

profile
match (param:LocalConcept {id:"PARAM-ORGTEST|" + 80002})
match (n:OrgTesting) where n.valueText = param.value
return count (n)
;

