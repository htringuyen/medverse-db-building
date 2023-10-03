merge (n:LookupValueSupplier {id:'otmv'})
    on create
    set n.value = 1

;

merge (upAnc:Anchor {id:"UNDEFINED_UPPER"})
    on create
    set upAnc.value = datetime("9999-01-01T00:00:00.000Z")
merge (lowAnc:Anchor {id:"UNDEFINED_LOWER"})
    on create
    set lowAnc.value = datetime("0001-01-01T00:00:00.000Z")

;

merge (lc:LocalConcept {id:"UNDEF|ITEM"})
    on create
    set lc:Item,
        lc.code = "UNDEFINED_ITEM",
        lc.name = "UNDEFINED_ITEM",
        lc.source = "custom::otmv/metadata/undefined"
