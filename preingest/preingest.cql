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
;

merge (lc:LocalConcept:Item:Param:Flag:Unit:Route:ProdStrength {id:"UNDEF|ALL"})
    on create
    set lc:Lookup:RefComment,
        lc.idText = "UNDEF|ALL",
        lc.value = 0,
        lc.flagDescription = "UNDEFINED_ALL",
        lc.code = "UNDEFINED_ALL",
        lc.name = "UNDEFINED_ALL",
        lc.source = "custom::otmv/metadata/undefined"

;
