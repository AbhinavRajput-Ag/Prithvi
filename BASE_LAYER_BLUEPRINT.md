# Prithvi Base Layer Blueprint

## Purpose
This document defines the base data layer for Prithvi so the platform can grow without repeatedly changing table meaning or rewriting core APIs.

The base layer should capture:
- who the farmer is
- who in the family operates the farm
- what land parcels exist
- what crop cycles run on each parcel
- what it costs to cultivate
- how labour, harvest, sales, and payments move
- what assets, risks, finance, and agronomy context surround the farm

This blueprint is intentionally wider than the current MVP UI. Some fields can remain optional in the first implementation.

## Design Principles
- Treat land as a first-class entity.
- Treat crop cycles as parcel-linked, not just farmer-linked.
- Preserve transaction history rather than overwriting values.
- Keep evidence-bearing records like documents, payments, soil tests, and advisories.
- Separate raw passive ingestion from verified operational records.
- Allow optional/null fields where real field data may be incomplete.
- Add `created_at`, `updated_at`, `created_by`, `status`, and `notes` wherever useful.

## Core Entity Map

### 1. Farmer
Represents the primary farming household or operator.

Recommended fields:
- `id`
- `name`
- `phone`
- `alternate_phone`
- `village`
- `gram_panchayat`
- `tehsil`
- `district`
- `state`
- `postal_code`
- `language`
- `gender`
- `date_of_birth`
- `education_level`
- `farming_experience_years`
- `land_acres`
- `bank_name`
- `bank_account_last4`
- `ifsc_code`
- `upi_id`
- `kyc_status`
- `consent_status`
- `created_at`
- `updated_at`

### 2. Farmer Member
Family members or household members involved in agriculture.

Recommended fields:
- `id`
- `farmer_id`
- `name`
- `relation`
- `age`
- `gender`
- `phone`
- `education_level`
- `role_in_agriculture`
- `primary_operator`
- `decision_maker`
- `financial_participant`
- `available_for_labour`
- `skill_notes`
- `created_at`
- `updated_at`

### 3. Land Parcel
Individual plot or parcel owned, leased, or operated by the farmer.

Recommended fields:
- `id`
- `farmer_id`
- `plot_name`
- `parcel_code`
- `survey_number`
- `ownership_type`
- `owner_name`
- `lease_start_date`
- `lease_end_date`
- `area_acres`
- `cultivable_area_acres`
- `irrigated_area_acres`
- `location_description`
- `village`
- `tehsil`
- `district`
- `state`
- `latitude`
- `longitude`
- `geojson_boundary`
- `elevation_meters`
- `slope_category`
- `soil_type`
- `soil_texture`
- `water_availability`
- `irrigation_source`
- `borewell_count`
- `road_access`
- `power_access`
- `fencing_status`
- `storage_nearby`
- `last_crop`
- `crop_history_notes`
- `risk_flags`
- `created_at`
- `updated_at`

### 4. Irrigation Asset
Water infrastructure linked to one or more parcels.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `source_type`
- `source_name`
- `borewell_depth_ft`
- `pump_hp`
- `distribution_method`
- `water_quality_notes`
- `seasonal_availability`
- `working_status`
- `last_service_date`
- `created_at`
- `updated_at`

### 5. Crop Cycle
One crop season on one parcel.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_type`
- `variety`
- `season`
- `year`
- `seed_source`
- `seed_brand`
- `seed_lot_number`
- `sowing_method`
- `area_acres`
- `sowing_date`
- `transplant_date`
- `expected_harvest`
- `expected_yield_quintal`
- `yield_estimate_revision_count`
- `current_stage`
- `crop_status`
- `adoption_notes`
- `created_at`
- `updated_at`

### 6. Crop History
Historical record of what was grown previously on a parcel.

Recommended fields:
- `id`
- `parcel_id`
- `crop_type`
- `variety`
- `season`
- `year`
- `yield_quintal`
- `residue_management`
- `soil_impact_notes`
- `pest_notes`
- `created_at`

### 7. Input Cost
Material and service costs attached to a crop cycle.

Recommended fields:
- `id`
- `crop_id`
- `stage`
- `category`
- `item_name`
- `supplier_name`
- `supplier_type`
- `invoice_number`
- `quantity`
- `unit`
- `amount`
- `transaction_mode`
- `transaction_reference`
- `transaction_date`
- `subsidized`
- `notes`
- `entry_date`
- `created_at`

### 8. Labour Entry
Structured labour details for a crop cycle.

Recommended fields:
- `id`
- `crop_id`
- `stage`
- `activity_name`
- `labour_origin`
- `labour_type`
- `labour_count`
- `male_count`
- `female_count`
- `start_date`
- `end_date`
- `wage_type`
- `wage_rate`
- `total_amount`
- `contractor_name`
- `payment_mode`
- `transaction_reference`
- `notes`
- `created_at`

### 9. Harvest
Harvest event for a crop cycle.

Recommended fields:
- `id`
- `crop_id`
- `harvest_date`
- `harvest_window_start`
- `harvest_window_end`
- `yield_quintal`
- `yield_rejected_quintal`
- `selling_price`
- `quality_grade`
- `moisture_percent`
- `bags_count`
- `buyer`
- `storage_location`
- `revenue`
- `notes`
- `created_at`

### 10. Deal
Commercial sale commitment for harvested produce.

Recommended fields:
- `id`
- `crop_id`
- `buyer_id`
- `buyer_name`
- `buyer_type`
- `sale_date`
- `quantity_quintal`
- `price_per_quintal`
- `gross_amount`
- `deductions_amount`
- `transport_cost`
- `mandi_fee`
- `net_amount`
- `payment_terms`
- `due_date`
- `payment_status`
- `notes`
- `created_at`

### 11. Payment Transaction
Separate transaction history for a deal.

Recommended fields:
- `id`
- `deal_id`
- `transaction_date`
- `amount`
- `mode`
- `reference_number`
- `received_by`
- `status`
- `notes`
- `created_at`

### 12. Farmer Document
Documents for land, identity, crop, finance, or compliance.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `document_type`
- `document_number`
- `issued_by`
- `issue_date`
- `expiry_date`
- `verification_status`
- `file_url`
- `notes`
- `created_at`

### 13. Advisory Alert
Operational or agronomy advice and risk alerts.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `alert_type`
- `source`
- `severity`
- `title`
- `message`
- `recommendation`
- `due_date`
- `resolved_status`
- `resolved_at`
- `created_at`

### 14. Soil Test
Lab or field soil analysis tied to a parcel.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `sample_date`
- `lab_name`
- `report_number`
- `ph`
- `ec`
- `organic_carbon`
- `nitrogen`
- `phosphorus`
- `potassium`
- `sulphur`
- `zinc`
- `boron`
- `iron`
- `manganese`
- `copper`
- `texture`
- `recommendation_summary`
- `file_url`
- `created_at`

### 15. Weather Risk Snapshot
Periodic weather and operational risk state.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `snapshot_date`
- `forecast_window_days`
- `rainfall_mm`
- `temperature_min`
- `temperature_max`
- `humidity_percent`
- `wind_speed_kmph`
- `heat_risk`
- `flood_risk`
- `drought_risk`
- `hail_risk`
- `pest_risk`
- `risk_notes`
- `created_at`

### 16. Asset Equipment
Farm equipment and productive assets.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `asset_type`
- `asset_name`
- `ownership_type`
- `brand`
- `capacity`
- `condition_status`
- `purchase_date`
- `purchase_cost`
- `finance_linked`
- `service_due_date`
- `notes`
- `created_at`

### 17. Loan Credit
Credit lines and debt obligations.

Recommended fields:
- `id`
- `farmer_id`
- `crop_id`
- `lender_name`
- `lender_type`
- `product_type`
- `principal_amount`
- `interest_rate`
- `sanction_date`
- `disbursement_date`
- `due_date`
- `emi_amount`
- `outstanding_balance`
- `collateral_type`
- `purpose`
- `status`
- `notes`
- `created_at`

### 18. Bank Account / Payment Profile
Payout and collection identity for the farmer.

Recommended fields:
- `id`
- `farmer_id`
- `account_holder_name`
- `bank_name`
- `branch_name`
- `account_number_masked`
- `ifsc_code`
- `upi_id`
- `preferred_payout_mode`
- `verification_status`
- `primary_account`
- `created_at`

### 19. Buyer Registry
Standardized buyer master for deals.

Recommended fields:
- `id`
- `name`
- `buyer_type`
- `contact_person`
- `phone`
- `location`
- `district`
- `state`
- `payment_reliability_score`
- `notes`
- `created_at`

### 20. Input Supplier
Supplier registry for seeds, fertilizer, pesticide, and service vendors.

Recommended fields:
- `id`
- `name`
- `supplier_type`
- `contact_person`
- `phone`
- `location`
- `district`
- `state`
- `gst_number`
- `notes`
- `created_at`

### 21. Warehouse / Storage Location
Storage points used before sale or aggregation.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `name`
- `location`
- `storage_type`
- `capacity_quintal`
- `ownership_type`
- `condition_status`
- `notes`
- `created_at`

### 22. Insurance Policy
Crop or asset insurance coverage.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `provider`
- `policy_number`
- `policy_type`
- `coverage_amount`
- `premium_amount`
- `start_date`
- `end_date`
- `claim_status`
- `claim_amount`
- `notes`
- `created_at`

### 23. Subsidy Scheme
Master list of subsidy schemes the system recognizes.

Recommended fields:
- `id`
- `scheme_name`
- `scheme_code`
- `provider`
- `category`
- `benefit_type`
- `description`
- `active_status`
- `created_at`

### 24. Government Benefit
Farmer-level enrollment and benefits received.

Recommended fields:
- `id`
- `farmer_id`
- `scheme_id`
- `application_number`
- `status`
- `applied_date`
- `approved_date`
- `benefit_amount`
- `benefit_type`
- `notes`
- `created_at`

### 25. Farm Visit / Extension Visit
Field visits by agronomists, agents, or extension workers.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `visitor_name`
- `visitor_role`
- `organization`
- `visit_date`
- `visit_type`
- `summary`
- `recommendations`
- `next_visit_due`
- `created_at`

### 26. Task / Activity Log
Operational task history for the farm.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `task_type`
- `title`
- `description`
- `scheduled_date`
- `completed_date`
- `assigned_to`
- `status`
- `priority`
- `source`
- `created_at`

### 27. Disease / Pest Incident
Field-level agronomy incident log.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `incident_date`
- `incident_type`
- `crop_stage`
- `severity`
- `affected_area_acres`
- `symptoms`
- `suspected_cause`
- `treatment_advised`
- `treatment_done`
- `resolution_status`
- `created_at`

### 28. Seed Source
Seed traceability reference for each crop cycle.

Recommended fields:
- `id`
- `crop_id`
- `supplier_id`
- `brand_name`
- `seed_type`
- `variety`
- `lot_number`
- `quantity`
- `unit`
- `purchase_date`
- `germination_claim`
- `notes`
- `created_at`

### 29. Water Test
Quality testing for irrigation water sources.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `sample_date`
- `source_type`
- `ph`
- `ec`
- `salinity`
- `hardness`
- `contaminant_notes`
- `recommendation_summary`
- `created_at`

### 30. Yield Estimate Revision
Tracks changes in expected yield over time.

Recommended fields:
- `id`
- `crop_id`
- `revision_date`
- `previous_estimate_quintal`
- `new_estimate_quintal`
- `revision_reason`
- `source`
- `notes`
- `created_at`

### 31. Expense Receipt / Voucher
Evidence layer for financial entries.

Recommended fields:
- `id`
- `farmer_id`
- `crop_id`
- `input_cost_id`
- `supplier_id`
- `receipt_number`
- `receipt_date`
- `amount`
- `payment_mode`
- `file_url`
- `verification_status`
- `notes`
- `created_at`

### 32. Consent / Data Sharing Record
Consent and data usage audit trail.

Recommended fields:
- `id`
- `farmer_id`
- `consent_type`
- `consent_version`
- `granted`
- `granted_at`
- `revoked_at`
- `purpose`
- `channel`
- `witness_name`
- `notes`
- `created_at`

### 33. Device / Sensor Source
Optional foundation for IoT or external telemetry.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `device_type`
- `device_name`
- `provider`
- `serial_number`
- `installation_date`
- `status`
- `data_source_url`
- `last_seen_at`
- `notes`
- `created_at`

## Passive Data Layer
These entities support low-touch data collection from feeds, uploads, telemetry, field traces, and derived system logic.

### Passive design rules
- Keep raw-source records append-only where possible.
- Never overwrite verified business records directly from OCR, feeds, or inference.
- Record provenance for every passive row: source, ingestion time, confidence, and review status.
- Allow later review to link passive records into active master tables like `expense_receipts`, `farmer_documents`, `soil_tests`, `advisory_alerts`, and `deals`.

### 34. Weather Snapshot Feed
Periodic weather observations and forecast slices tied to a farmer, parcel, or crop.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `source_name`
- `snapshot_time`
- `forecast_window_hours`
- `rainfall_mm`
- `temperature_min_c`
- `temperature_max_c`
- `humidity_percent`
- `wind_speed_kmph`
- `solar_radiation`
- `heat_risk`
- `flood_risk`
- `drought_risk`
- `hail_risk`
- `lightning_risk`
- `raw_payload`
- `created_at`

### 35. Mandi Price Snapshot
Price intelligence collected automatically from mandis or market feeds.

Recommended fields:
- `id`
- `crop_type`
- `variety`
- `market_name`
- `district`
- `state`
- `snapshot_date`
- `min_price`
- `modal_price`
- `max_price`
- `arrival_quantity`
- `source_name`
- `raw_payload`
- `created_at`

### 36. Satellite Crop Health Observation
Remote-sensing indicators tied to parcel or crop cycles.

Recommended fields:
- `id`
- `parcel_id`
- `crop_id`
- `observation_date`
- `provider`
- `imagery_source`
- `ndvi`
- `ndmi`
- `vegetation_health_score`
- `canopy_stress_score`
- `cloud_cover_percent`
- `anomaly_flag`
- `observation_summary`
- `raw_payload`
- `created_at`

### 37. Government Land Record Reference
External reference to official land record sources.

Recommended fields:
- `id`
- `parcel_id`
- `source_system`
- `reference_number`
- `owner_name`
- `record_status`
- `verified_match`
- `last_checked_at`
- `raw_payload`
- `notes`
- `created_at`

### 38. Ingested Document
Raw inbound file or media item from WhatsApp, upload, scan, or shared image.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `source_channel`
- `source_reference`
- `file_type`
- `file_name`
- `file_url`
- `captured_at`
- `ingestion_status`
- `review_status`
- `linked_record_type`
- `linked_record_id`
- `notes`
- `created_at`

### 39. OCR Extraction
Machine-read extraction from receipts, invoices, reports, or shared media.

Recommended fields:
- `id`
- `ingested_document_id`
- `extraction_type`
- `provider`
- `extracted_text`
- `parsed_fields_json`
- `confidence_score`
- `review_status`
- `reviewed_by`
- `reviewed_at`
- `created_at`

### 40. Soil Lab Report Import
Passive lab-report ingestion before normalization into `soil_tests`.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `ingested_document_id`
- `lab_name`
- `report_number`
- `sample_date`
- `parsed_json`
- `confidence_score`
- `review_status`
- `linked_soil_test_id`
- `created_at`

### 41. Bank Statement Import
Imported payout, debit, or settlement records from bank feeds or statements.

Recommended fields:
- `id`
- `farmer_id`
- `payment_profile_id`
- `statement_period_start`
- `statement_period_end`
- `source_name`
- `file_url`
- `import_status`
- `raw_payload`
- `created_at`

### 42. Payment Reconciliation Event
System-generated or imported match between expected and observed payments.

Recommended fields:
- `id`
- `farmer_id`
- `deal_id`
- `statement_import_id`
- `matched_amount`
- `expected_amount`
- `difference_amount`
- `match_status`
- `match_confidence`
- `reconciliation_notes`
- `created_at`

### 43. Device Telemetry
Sensor or device stream entries for irrigation, climate, or equipment state.

Recommended fields:
- `id`
- `device_source_id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `observed_at`
- `metric_name`
- `metric_value`
- `metric_unit`
- `quality_flag`
- `raw_payload`
- `created_at`

### 44. Field Visit Event
Call center or field officer interaction synced later from offline tools.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `visit_date`
- `visit_mode`
- `visitor_name`
- `visitor_role`
- `organization`
- `summary`
- `recommendations`
- `follow_up_needed`
- `source_channel`
- `created_at`

### 45. Geo Visit Log
Geolocation trace from field visits or farm operations.

Recommended fields:
- `id`
- `field_visit_id`
- `farmer_id`
- `parcel_id`
- `captured_at`
- `latitude`
- `longitude`
- `accuracy_meters`
- `capture_source`
- `notes`
- `created_at`

### 46. Crop Stage Inference
System-inferred crop stage derived from activities, visits, or signals.

Recommended fields:
- `id`
- `crop_id`
- `inferred_stage`
- `inference_source`
- `inference_date`
- `confidence_score`
- `basis_summary`
- `accepted`
- `accepted_by`
- `accepted_at`
- `created_at`

### 47. Risk Alert Event
System-generated risk detection from pattern rules or passive signals.

Recommended fields:
- `id`
- `farmer_id`
- `parcel_id`
- `crop_id`
- `alert_type`
- `severity`
- `detected_at`
- `source_type`
- `confidence_score`
- `title`
- `message`
- `recommended_action`
- `status`
- `resolved_at`
- `created_at`

## Supporting Registries
These may stay simple initially but are worth planning.

### Buyer Registry
Use `buyer_registry` as the source of truth for deals.

### Supplier Registry
Use `input_supplier` as the source of truth for procurement and seed traceability.

### Storage Location
Use `warehouse / storage_location` as the source of truth for post-harvest inventory points.

### Insurance Policy
Use `insurance_policy` for crop or asset coverage records.

## Priority Relationships
- one `farmer` has many `farmer_members`
- one `farmer` has many `land_parcels`
- one `land_parcel` has many `crop_cycles`
- one `crop_cycle` has many `input_costs`
- one `crop_cycle` has many `labour_entries`
- one `crop_cycle` has many `harvests`
- one `crop_cycle` has many `deals`
- one `deal` has many `payment_transactions`
- one `land_parcel` has many `soil_tests`
- one `farmer` can have many `documents`, `assets`, and `loans`
- one `land_parcel` can have many `weather_snapshots`, `satellite_observations`, and `geo_visit_logs`
- one `ingested_document` can have many `ocr_extractions`
- one `bank_statement_import` can have many `payment_reconciliation_events`
- one `crop_cycle` can have many `crop_stage_inferences` and `risk_alert_events`

## Required vs Optional for Phase 1

### Required in phase 1
- farmer core identity
- farmer member name/relation/role
- land parcel plot name/area/ownership/location
- crop cycle parcel linkage
- input cost basic transaction fields
- labour count/date/amount
- harvest yield and date
- deal gross amount and payment status
- soil test summary values
- asset type and status
- loan lender/principal/due/outstanding

### Optional in phase 1
- geojson boundary
- micronutrient full panel
- insurance and subsidy
- storage and warehouse modeling
- sensor/device integration
- scoring models

## Suggested SQL Rollout Order

### Phase A: land and family base
- `farmer_members`
- expand `land_parcels`
- `irrigation_assets`
- `farmer_documents`

### Phase B: crop operations base
- rename or stabilize on `crop_cycles` semantics
- `labour_entries`
- enrich `input_costs`
- `crop_history`
- `buyer_registry`
- `input_supplier`
- `seed_source`
- `yield_estimate_revision`
- `expense_receipts`

### Phase C: agronomy and risk
- `soil_tests`
- `weather_risk_snapshots`
- `advisory_alerts`
- `farm_visit_extension_visit`
- `task_activity_log`
- `disease_pest_incidents`
- `water_tests`

### Phase D: asset and finance
- `asset_equipment`
- `loan_credit`
- `payment_transactions`
- `bank_account_payment_profile`
- `insurance_policy`
- `subsidy_scheme`
- `government_benefit`
- `consent_data_sharing_record`
- `warehouse_storage_location`
- `device_sensor_source`

### Phase E: passive ingestion and intelligence
- `weather_snapshot_feeds`
- `mandi_price_snapshots`
- `satellite_crop_health_observations`
- `government_land_record_references`
- `ingested_documents`
- `ocr_extractions`
- `soil_lab_report_imports`
- `bank_statement_imports`
- `payment_reconciliation_events`
- `device_telemetry`
- `field_visit_events`
- `geo_visit_logs`
- `crop_stage_inferences`
- `risk_alert_events`

## Recommended Immediate Implementation
If building incrementally, implement these next:
1. `farmer_members`
2. expand `land_parcels`
3. `labour_entries`
4. `soil_tests`
5. `farmer_documents`
6. `loan_credit`
7. `asset_equipment`
8. `buyer_registry`
9. `input_supplier`
10. `yield_estimate_revision`

## Recommended Passive Rollout
Once the active ledger is stable, add passive capture in this order:
1. `weather_snapshot_feeds`
2. `mandi_price_snapshots`
3. `risk_alert_events`
4. `ingested_documents`
5. `ocr_extractions`
6. `field_visit_events`
7. `geo_visit_logs`
8. `bank_statement_imports`
9. `payment_reconciliation_events`
10. `crop_stage_inferences`
11. `satellite_crop_health_observations`
12. `government_land_record_references`

## Notes for API Design
- prefer `GET /farmers/{id}` style internally even if name-based endpoints remain for demos
- use parcel-linked crop routes wherever possible
- keep separate payment transaction history instead of editing a single total field only
- allow nulls for slow-changing field data gathered over time
- add auditability for money, ownership, and evidence-bearing tables
- for passive ingestion, expose review and link endpoints instead of writing directly into verified ledger tables
