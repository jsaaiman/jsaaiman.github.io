-- Use this query to restate historical scenarios if seed tables or bucketing change. Note that this clears out the master forecast table so you will need to rerun function1 first 
-- and function4 afteward.

DELETE FROM fpa.live_forecast_staging WHERE live_forecast_staging.child IS NULL; -- Clears staging table
DELETE FROM fpa.live_forecast_staging WHERE live_forecast_staging.child IS NOT NULL; -- Clears staging table

DELETE FROM fpa.forecasts_master_tbl WHERE forecasts_master_tbl.child IS NULL; -- Clears master table
DELETE FROM fpa.forecasts_master_tbl WHERE forecasts_master_tbl.child IS NOT NULL; -- Clears master table

-- Below grabs data from historical_scenarios_bigquery_import table and stages into staging table. You can sub out with any cvs import from bigquery
INSERT INTO fpa.live_forecast_staging(
  	tran_date,
	posting_period,
	accounting_period_end_date,
	subsidiary_id,
	subsidiary,
	department_id,
	department_child_1,
	department_child_2,
	department_child_3,
	department_child_4,
	division,
	location_id,
	standard_location,
	nation_state,
	class_id,
	class_name,
	account_id,
	child,
	parent_id,
	parent,
	expense_type,
	account_type,
	fs,
	fsli,
	bucket1,
	bucket2,
	sub_category,
	ops_category,
	ops_sub_category,
	workplace_category,
	po_number,
	po_line_number,
	vendor,
	document_number,
	commodity,
	project_code,
	project_group,
	project_category,
	memo,
	item,
	currency,
	amount,
	debit_amount,
	debit_fx_amount,
	credit_amount,
	credit_fx_amount,
	scenario,
	team,
	contact,
	contact_email,
	project
)
SELECT
  	CAST(NULL AS DATE) AS tran_date,
	CAST(h.date AS CHARACTER VARYING(256)) AS posting_period,
	CAST(h.period AS DATE) AS accounting_period_end_date,
	CAST(1 AS INTEGER) AS subsidiary_id,
	CAST(h.subsidiary_name AS CHARACTER VARYING(256)) AS subsidiary,
	CAST(c.department_id AS INTEGER) AS department_id,
	CAST(h.department_name AS CHARACTER VARYING(128)) AS department_child_1,
	CAST(c.department_child_2 AS CHARACTER VARYING(128)) AS department_child_2,
	CAST(c.department_child_3 AS CHARACTER VARYING(128)) AS department_child_3,
	CAST(c.department_child_4 AS CHARACTER VARYING(128)) AS department_child_4,
	CAST(c.division AS CHARACTER VARYING(128)) AS division,
	CAST(l.location_id AS INTEGER) AS location_id,
	CAST(l.simplified_location AS CHARACTER VARYING(256)) AS standard_location,
	CAST(h.state AS CHARACTER VARYING(256)) AS nation_state,
	CAST(1 AS INTEGER) AS class_id,
	CAST(h.class_name AS CHARACTER VARYING(128)) AS class_name,
	CAST(LEFT(h.child,4) AS CHARACTER VARYING(128)) AS account_id,
	CAST(g.child_gl_account AS CHARACTER VARYING(256)) AS child,
	CAST(LEFT(h.parent,4) AS CHARACTER VARYING(4)) AS parent_id,
	CAST(h.parent AS CHARACTER VARYING(64)) AS parent,
	CAST(h.type AS CHARACTER VARYING(128)) AS expense_type,
	CAST(NULL AS CHARACTER VARYING(128)) AS account_type,
	CAST(h.fs AS CHARACTER VARYING(13)) AS fs,
	CAST(h.fsli AS CHARACTER VARYING(10)) AS fsli,
	CAST(				
		CASE 
			WHEN LEFT(h.child,4) = '1695' 
				AND c.division IN('Workplace', 'Corporate IT')  THEN
				'Office & Terminals'
			WHEN LEFT(h.child,4) = '1695' 
				AND c.division NOT IN('Workplace', 'Corporate IT') THEN
				'Other Capex'
			WHEN c.division = 'Workplace'
				AND g.mgmt_bucket NOT IN('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation','Vehicle Leases','Allocations')
				AND g.fs NOT IN('Capex') THEN
				'Workplace'
				WHEN c.division = 'Operations'
				AND g.mgmt_bucket NOT IN('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation','Vehicle Leases','Allocations')
				AND g.fs NOT IN('Capex') THEN
				'Operations'
			WHEN c.division = 'Vehicle Operations'
				AND g.fs NOT IN('Capex')
				AND g.mgmt_bucket NOT IN('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation','Vehicle Leases','Allocations') THEN
				'Operations'
			WHEN c.division = 'Hardware'
				AND g.fs NOT IN('Capex')
				AND g.mgmt_bucket = 'HW Builds - OPEX' THEN
				'HW Builds - OPEX'
			WHEN c.division = 'Hardware'
				AND g.fs NOT IN('Capex')
				AND g.mgmt_bucket = 'HW Programs' THEN
				'HW Programs'
			ELSE
				g.mgmt_bucket
			END AS CHARACTER VARYING (27)) AS bucket1,	
	CAST(h.bucket2 AS CHARACTER VARYING(27)) AS bucket2,
	CAST(g.sub_category AS CHARACTER VARYING (128)) AS sub_category,
	CAST(g.ops_category AS CHARACTER VARYING (128)) AS ops_category,
	CAST(g.ops_sub_category AS CHARACTER VARYING (128)) AS ops_sub_category,
	CAST(g.workplace_category AS CHARACTER VARYING (128)) AS workplace_category,
	CAST(h.po_number AS CHARACTER VARYING(128)) AS po_number,
	CAST(h.po_line_number AS CHARACTER VARYING(4)) AS po_line_number,
	CAST(h.vendor AS CHARACTER VARYING(128)) AS vendor,
	CAST(h.document_number AS CHARACTER VARYING(256)) AS document_number,
	CAST(h.commodity AS CHARACTER VARYING(256)) AS commodity,
	CAST(h.project_code AS CHARACTER VARYING(128)) AS project_code,
	CAST(p.project_group AS CHARACTER VARYING(256)) AS project_group,	
	CAST(p.project_category AS CHARACTER VARYING(256)) AS project_category,	
	CAST(h.memo AS CHARACTER VARYING(4096)) AS memo,
	CAST(NULL AS CHARACTER VARYING(256)) AS item,
	CAST('USD' AS CHARACTER VARYING(3)) AS currency,
	CAST(h.amount_net_ AS DOUBLE PRECISION) AS amount,
	CAST(h.debit AS DOUBLE PRECISION) AS debit_amount,
	CAST(0 AS DOUBLE PRECISION) AS debit_fx_amount,
	CAST(h.credit AS DOUBLE PRECISION) AS credit_amount,
	CAST(0 AS DOUBLE PRECISION) AS credit_fx_amount,
	CAST(h.scenario AS CHARACTER VARYING(128)) AS scenario,
	CAST(h.team AS CHARACTER VARYING(128)) AS team,
	CAST(h.contact AS CHARACTER VARYING(128)) AS contact,
	CAST('format' AS CHARACTER VARYING(128)) AS contact_email,
	CAST(h.project AS CHARACTER VARYING(128)) AS project
FROM
  	fpa.historical_scenarios_bigquery_import h -- UPDATE table reference if needed
LEFT JOIN fpa.fpa_glaccounts g ON LEFT(h.child,4) = g.account_id
LEFT JOIN fpa.fpa_cost_centers c ON h.department_name = c.department_child_1
LEFT JOIN fpa.fpa_location_codes l ON h.location1 = l.original_location
LEFT JOIN fpa.fpa_project_codes p ON h.project_code = p.project_code_child
;

-- Updates subcategory for project based management buckets
UPDATE fpa.live_forecast_staging
SET sub_category = CASE 
                      WHEN bucket1 IN('HW Programs', 'HW Builds - CAPEX', 'Other Capex')
                           AND LEN(project_group)>= 1 THEN project_group
                      WHEN bucket1 IN('HW Programs', 'HW Builds - CAPEX', 'Other Capex')
                           AND (project_group IS NULL OR LEN(project_group) < 1) THEN 'Other Projects'
                      WHEN bucket1 IN('Office & Terminals','Placeholder') THEN standard_location
                      ELSE sub_category
                    END
FROM fpa.live_forecast_staging;


--Takes staged data and appends to master
INSERT INTO fpa.forecasts_master_tbl
SELECT
	*
FROM
	fpa.live_forecast_staging;

UPDATE fpa.forecasts_master_tbl
SET scenario = 'Budget 24v1'
WHERE scenario = 'Budget 24v1_rev2';
