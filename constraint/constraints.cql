/* for IngestMetadata */
create constraint ingest_metadata_id_key if not exists for (n:IngestMetadata) require n.id is node key;

/* for Patient */
CREATE CONSTRAINT patient_id_key IF NOT EXISTS FOR (n:Patient) REQUIRE n.id is node key;
CREATE CONSTRAINT patient_id_type IF NOT EXISTS FOR (n:Patient) REQUIRE n.id is ::integer;
create constraint patient_ssn_exists if not exists for (n:Patient) require n.ssn is not null;
create constraint patient_ssn_type if not exists for (n:Patient) require n.ssn is ::string;
create constraint patient_first_name_exists if not exists for (n:Patient) require n.firstName is not null;
create constraint patient_first_name_type if not exists for (n:Patient) require n.firstName is ::string;
create constraint patient_last_name_exists if not exists for (n:Patient) require n.lastName is not null;
create constraint patient_last_name_type if not exists for (n:Patient) require n.lastName is ::string;
create constraint patient_shifted_years_exists if not exists for (n:Patient) require n.shiftedYears is not null;
create constraint patient_shifted_years_type if not exists for (n:Patient) require n.shiftedYears is ::duration;

/* for Timeline */
create constraint timeline_id_key if not exists for (n:Timeline) require n.id is node key;
create constraint timeline_id_type if not exists for (n:Timeline) require n.id is ::string;

/* for RefSet */
CREATE CONSTRAINT ref_set_id_key if not exists for (n:RefSet) require n.id is node key;
create constraint ref_set_id_type if not exists for (n:RefSet) require n.id is ::integer;
create constraint ref_set_code_key if not exists for (n:RefSet) require n.code is node key;
create constraint ref_set_code_type if not exists for (n:RefSet) require n.code is ::string;
create  constraint ref_set_name_exists if not exists for (n:RefSet) require n.name is not null;
create constraint ref_set_name_type if not exists for (n:RefSet) require n.name is ::string;
create constraint ref_set_reference_exists if not exists for (n:RefSet) require n.reference is not null;
create constraint ref_set_reference_type if not exists for (n:RefSet) require n.reference is ::string;

/* for LocalDomain */
create constraint local_domain_id_key if not exists for (n:LocalDomain) require n.id is node key;
create constraint local_domain_id_type if not exists for (n:LocalDomain) require n.id is ::integer;
create constraint local_domain_code_exists if not exists for (n:LocalDomain) require n.code is not null;
create constraint local_domain_code_type if not exists for (n:LocalDomain) require n.code is ::string;
create constraint local_domain_name_exists if not exists for (n:LocalDomain) require n.name is not null;
create constraint local_domain_name_type if not exists for (n:LocalDomain) require n.name is ::string;
create constraint local_domain_reference_exists if not exists for (n:LocalDomain) require n.reference is not null;
create constraint local_domain_reference_type if not exists for (n:LocalDomain) require n.reference is ::string;

/* for LocalClass */
create constraint local_class_id_key if not exists for (n:LocalClass) require n.id is node key;
create constraint local_class_id_type if not exists for (n:LocalClass) require n.id is ::integer;
create constraint local_class_code_exists if not exists for (n:LocalClass) require n.code is not null;
create constraint local_class_code_type if not exists for (n:LocalClass) require n.code is ::string;
create constraint local_class_name_exists if not exists for (n:LocalClass) require n.name is not null;
create constraint local_class_name_type if not exists for (n:LocalClass) require n.name is ::string;
create constraint local_class_reference_exists if not exists for (n:LocalClass) require n.reference is not null;
create constraint local_class_reference_type if not exists for (n:LocalClass) require n.reference is ::string;

/* for LocalConcept */
create constraint local_concept_id_key if not exists for (n:LocalConcept) require n.id is node key;
create constraint local_concept_id_type if not exists for (n:LocalConcept) require n.id is ::string;
create constraint local_concept_code_exists if not exists for (n:LocalConcept) require n.code is not null;
create constraint local_concept_code_type if not exists for (n:LocalConcept) require n.code is ::string;
create constraint local_concept_name_exists if not exists for (n:LocalConcept) require n.name is not null;
create constraint local_concept_name_type if not exists for (n:LocalConcept) require n.name is ::string;
create constraint local_concept_source_exists if not exists for (n:LocalConcept) require n.source is not null;
create constraint local_concept_source_type if not exists for (n:LocalConcept) require n.source is ::string;

/* for EXISTENT */
  // TODO
create constraint existent_value_comment if not exists for (n:EXISTENT) require n.valueComment is ::integer;
create constraint existent_value_flag1_type if not exists for (n:EXISTENT) require n.valueFlag is ::integer;
create constraint existent_value_flag2_type if not exists for (n:EXISTENT) require n.value2ndFlag is ::integer;

/* for TEXTUAL */
create index textual_value_text_index if not exists for (n:TEXTUAL) on (n.valueText);
create constraint textual_value_text_exists if not exists for (n:TEXTUAL) require n.valueText is not null;
create constraint textual_value_text_type if not exists for (n:TEXTUAL) require n.valueText is ::integer;
create constraint textual_value_comment if not exists for (n:TEXTUAL) require n.valueComment is ::integer;
create constraint textual_value_flag1_type if not exists for (n:TEXTUAL) require n.valueFlag is ::integer;
create constraint textual_value_flag2_type if not exists for (n:TEXTUAL) require n.value2ndFlag is ::integer;


/* for NUMERIC */
create constraint numeric_value_num_exists if not exists for (n:NUMERIC) require n.valueNum is not null;
create constraint numeric_value_num_type if not exists for (n:NUMERIC) require n.valueNum is ::float;
create constraint numeric_value_unit_type if not exists for (n:NUMERIC) require n.valueUnit is ::integer;
create constraint numeric_value_opr_type if not exists for (n:NUMERIC) require n.valueOpr is ::integer;
create constraint numeric_value_cmt_type if not exists for (n:NUMERIC) require n.valueCmt is ::integer;
create constraint numeric_value_flag1_type if not exists for (n:NUMERIC) require n.valueFlag is ::integer;
create constraint numeric_value_flag2_type if not exists for (n:NUMERIC) require n.value2ndFlag is ::integer;

/* for DATETIME */
create constraint temporal_value_date_exists if not exists for (n:DATETIME) require n.valueTime is not null;
create constraint temporal_value_date_type if not exists for (n:DATETIME) require n.valueTime is ::ZONED DATETIME;
create constraint temporal_value_unit_type if not exists for (n:DATETIME) require n.valueUnit is ::integer;
create constraint textual_value_flag1_type if not exists for (n:DATETIME) require n.valueFlag is ::integer;
create constraint textual_value_flag2_type if not exists for (n:DATETIME) require n.value2ndFlag is ::integer;

/* for Item */
create constraint item_desc_numeric if not exists for (n:Item) require n.descNumeric is ::string;
create constraint item_desc_parametric if not exists for (n:Item) require n.descParametric is ::string;
create constraint item_desc_existent if not exists for (n:Item) require n.descExistent is ::string;
create constraint item_ref_range_lower if not exists for (n:Item) require n.refRangeLower is ::float;
create constraint item_ref_range_upper if not exists for (n:Item) require n.refRangeUpper is ::float;

/* for Lookup */
create constraint lookup_id_key if not exists for (n:Lookup) require n.value is node key;
create constraint lookup_id_type if not exists for (n:Lookup) require n.value is ::integer;

/* for flag */
create constraint flag_description_exists if not exists for (n:Flag) require n.flagDescription is not null;
create constraint flag_description_type if not exists for (n:Flag) require n.flagDescription is ::string;

/* for LookupValueSupplier */
create constraint lookup_value_suplier_id_key if not exists for (n:LookupValueSupplier) require n.id is node key;
create constraint lookup_value_suplier_id_type if not exists for (n:LookupValueSupplier) require n.id is ::string;
create constraint lookup_value_suplier_value_exists if not exists for (n:LookupValueSupplier) require n.value is not null;
create constraint lookup_value_suplier_value_type if not exists for (n:LookupValueSupplier) require n.value is ::integer;

/* for LabSampling */
create constraint lab_sampling_id_key if not exists for (n:LabSampling) require n.specimenId is node key;
create constraint lab_sampling_id_type if not exists for (n:LabSampling) require n.specimenId is ::integer;

/* for MicroSampling */
create constraint micro_sampling_id_key if not exists for (n:MicroSampling) require n.specimenId is node key;
create constraint micro_sampling_id_type if not exists for (n:MicroSampling) require n.specimenId is ::integer;

/* for OrgTesting */
create constraint org_testing_id_key if not exists for (n:OrgTesting) require n.testId is node key;
create constraint org_testing_id_type if not exists for (n:OrgTesting) require n.testId is ::string;
create constraint org_testing_sample_seq_exists if not exists for (n:OrgTesting) require n.sampleSeq is not null;
create constraint org_testing_sample_seq if not exists for (n:OrgTesting) require n.sampleSeq is ::integer;

/* for AbxTesting */
create constraint abx_testing_isolate_num_type if not exists for (n:AbxTesting) require n.isolateNum is ::integer;
create constraint abx_testing_quantity_type if not exists for (n:AbxTesting) require n.quantity is ::string;

/* for RefComment */
create constraint ref_comment_id_key if not exists for (n:RefComment) require n.idText is node key;
create constraint ref_comment_id_type if not exists for (n:RefComment) require n.idText is ::string;

/* for CATEGORIZED */
create constraint categorized_category_exists if not exists for (n:CATEGORIZED) require n.category is not null;
create constraint categorized_category_type if not exists for (n:CATEGORIZED) require n.category is ::string;

/* for Anchor */
create constraint anchor_id_key if not exists for (n:Anchor) require n.id is node key;
create constraint anchor_id_type if not exists for (n:Anchor) require n.id is ::string;
create constraint anchor_value_exists if not exists for (n:Anchor) require n.value is not null;
create constraint anchor_value_type if not exists for (n:Anchor) require n.value is ::ZONED DATETIME;

/* for Case */
create constraint case_id_key if not exists for (n:Case) require n.id is node key;
create constraint case_id_type if not exists for (n:Case) require n.id is ::string;

/* for Visit */
create constraint visit_id_key if not exists for (n:Visit) require n.id is node key;
create constraint visit_id_type if not exists for (n:Visit) require n.id is ::string;

/* for Stop */
create constraint stop_id_key if not exists for (n:Stop) require n.id is node key;
create constraint stop_id_type if not exists for (n:Stop) require n.id is ::string;

/* for AdmReason */
create constraint adm_reason_serverity_type if not exists for (n:AdmReason) require n.severity is ::integer;
create constraint adm_reason_mortality_type if not exists for (n:AdmReason) require n.mortality is ::integer;

/* for HospDx */
create constraint hospdx_priority_exists if not exists for (n:HospDx) require n.priority is not null;
create constraint hospdx_priority_type if not exists for (n:HospDx) require n.priority is ::integer;

/* for EmergDx */
create constraint emergdx_priority_exists if not exists for (n:EmergDx) require n.priority is not null;
create constraint emergdx_priority_type if not exists for (n:EmergDx) require n.priority is ::integer;

/* for ProcedureEra */
create constraint procedure_era_id_key if not exists for (n:ProcedureEra) require n.id is node key;
create constraint procedure_era_id_type if not exists for (n:ProcedureEra) require n.id is ::string;


/* for BilledProc */
create constraint billed_proc_priority_exists if not exists for (n:BilledProc) require n.priority is not null;
create constraint billed_proc_priority_type if not exists for (n:BilledProc) require n.priority is ::integer;

/* for provider */
create constraint provider_id_key if not exists for (n:Provider) require n.id is node key;
create constraint provider_id_type if not exists for (n:Provider) require n.id is ::string;

/* for Drug */
create constraint drug_ndc_code_type if not exists for (n:Drug) require n.ndcCode is ::string;
create constraint drug_gsn_code_type if not exists for (n:Drug) require n.gsnCode is ::string;

/* for Medication */

/*
    create (med:Medication {id:toInteger(row.pharmacy_id)})
        set med.hasMainRx = hasMainRx,
            med.frequency = frequency,
            med.dispSched = disp_sched,
            med.infusionType = infusion_type,
            med.slidingScale = sliding_scale,
            med.lockoutInterval = lockout_interval,
            med.basalRate = basal_rate,
            med.oneHrMax = one_hr_max,
            med.dosesPer24Hrs = doses_per_24_hrs,
            med.duration = duration,
            med.durationInterval = duration_interval,
            med.expirationValue = expiration_value,
            med.expirationUnit = expiration_unit
            */
// set type constraints for above properties
create constraint medication_id_key if not exists for (n:Medication) require n.id is node key;
create constraint medication_id_type if not exists for (n:Medication) require n.id is ::integer;
create constraint medication_has_main_rx_type if not exists for (n:Medication) require n.hasMainRx is ::boolean;
create constraint medication_frequency_type if not exists for (n:Medication) require n.frequency is ::string;
create constraint medication_disp_sched_type if not exists for (n:Medication) require n.dispSched is ::string;
create constraint medication_infusion_type_type if not exists for (n:Medication) require n.infusionType is ::string;
create constraint medication_sliding_scale_type if not exists for (n:Medication) require n.slidingScale is ::string;
create constraint medication_lockout_interval_type if not exists for (n:Medication) require n.lockoutInterval is ::string;
create constraint medication_basal_rate_type if not exists for (n:Medication) require n.basalRate is ::float;
create constraint medication_one_hr_max_type if not exists for (n:Medication) require n.oneHrMax is ::string;
create constraint medication_doses_per_24_hrs_type if not exists for (n:Medication) require n.dosesPer24Hrs is ::float;
create constraint medication_duration_type if not exists for (n:Medication) require n.duration is ::float;
create constraint medication_duration_interval_type if not exists for (n:Medication) require n.durationInterval is ::string;
create constraint medication_expiration_value_type if not exists for (n:Medication) require n.expirationValue is ::integer;
create constraint medication_expiration_unit_type if not exists for (n:Medication) require n.expirationUnit is ::string;
// additional contraints
create constraint medication_entertime_type if not exists for (n:Medication) require n.enterTime is ::ZONED DATETIME;
create constraint medication_verified_time if not exists for (n:Medication) require n.verifiedTime is ::ZONED DATETIME;
create constraint medication_expiration_date if not exists for (n:Medication) require n.expirationDate is ::ZONED DATETIME;
create constraint medication_dispensation_source if not exists for (n:Medication) require n.dispensationSource is ::string;
create constraint medication_fill_quantity if not exists for (n:Medication) require n.fillQuantity is ::string;


/* for Prescription */
create constraint prescription_value_dose_type if not exists for (n:Prescription) require n.valueDose is ::float;
create constraint prescription_value_dose_upto_type if not exists for (n:Prescription) require n.valueDoseUpto is ::float;
create constraint prescription_value_form_type if not exists for (n:Prescription) require n.valueForm is ::float;
create constraint prescription_value_form_upto_type if not exists for (n:Prescription) require n.valueFormUpto is ::float;
create constraint prescription_value_prodStr_type if not exists for (n:Prescription) require n.valuePStr is ::integer;
create constraint prescription_value_dose_unit if not exists for (n:Prescription) require n.valueDUnit is ::integer;
create constraint prescription_value_dose_form if not exists for (n:Prescription) require n.valueFUnit is ::integer;
create constraint prescription_value_route if not exists for (n:Prescription) require n.valueRoute is ::integer;
create constraint prescription_value_doses_per_24_hrs if not exists for (n:Prescription) require n.dosesPer24Hrs is ::float;

/* for RxAdmin */
create constraint rx_admin_id_key if not exists for (n:RxAdmin) require n.id is node key;
create constraint rx_admin_id_type if not exists for (n:RxAdmin) require n.id is ::string;
create constraint rx_admin_patient_weight_type if not exists for (n:RxAdmin) require n.patientWeight is ::float;
create constraint rx_admin_value_flag_type if not exists for (n:RxAdmin) require n.valueFlag is ::integer;


/* for DrugExposure */
create constraint drug_exposure_value_dose_type if not exists for (n:DrugExposure) require n.valueDose is ::float;
create constraint drug_exposure_value_dose_upto_type if not exists for (n:DrugExposure) require n.valueDoseUpto is ::float;
create constraint drug_exposure_value_dose_unit if not exists for (n:DrugExposure) require n.valueDUnit is ::integer;


/* for INFUSION */
create constraint infusion_inf_amount_type if not exists for (n:INFUSION) require n.infAmount is ::float;
create constraint infusion_total_inf_amount_type if not exists for (n:INFUSION) require n.totalInfAmount is ::float;
create constraint infusion_prior_inf_rate_type if not exists for (n:INFUSION) require n.priorInfRate is ::float;
create constraint infusion_inf_prior_inf_rate_upto_type if not exists for (n:INFUSION) require n.priorInfRateUpto is ::float;
create constraint infusion_inf_amount_unit_type if not exists for (n:INFUSION) require n.valueInfAmountUnit is ::integer;
create constraint infusion_from_open_bag_type if not exists for (n:INFUSION) require n.fromOpenBag is ::boolean;
create constraint infusion_original_inf_rate_type if not exists for (n:INFUSION) require n.originalInfRate is ::float;
create constraint infusion_original_inf_amount_type if not exists for (n:INFUSION) require n.originalInfAmount is ::float;

/* for DrugEra*/
create constraint drug_era_id_key if not exists for (n:DrugEra) require n.id is node key;
create constraint drug_era_id_type if not exists for (n:DrugEra) require n.id is ::string;

/* for Note */
create constraint note_id_key if not exists for (n:Note) require n.id is node key;
create constraint note_id_type if not exists for (n:Note) require n.id is ::string;
create constraint note_chart_time_exists if not exists for (n:Note) require n.chartTime is not null;
create constraint note_time_type if not exists for (n:Note) require n.chartTime is ::ZONED DATETIME;
create constraint note_exists if not exists for (n:Note) require n.text is not null;
create constraint note_type if not exists for (n:Note) require n.text is ::string;

/* for EraSummary */

/* for EventReport */

/* for ExtractedTerm */
create constraint extraction_id_key if not exists for (n:Extraction) require n.id is node key;
create constraint extraction_id_type if not exists for (n:Extraction) require n.id is ::string;
create constraint extraction_mapping_code_type if not exists for (n:Extraction) require n.mappingCode is ::string;
create constraint extraction_mapping_code_system_type if not exists for (n:Extraction) require n.mappingCodeSystem is ::string;





