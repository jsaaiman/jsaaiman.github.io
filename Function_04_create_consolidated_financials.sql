--Use this query to unify fpa.fpa_actuals and fpa.forecast_master_tbl. The resulting table fpa.unified_fpa_financials. An update query then flips contra R&D and non-cash lease payments to a positive number. A second table is created called unified_fpa_financials_simplified that takes only essential columns and aggregates amounts for dashboard performance.

DROP TABLE fpa.unified_fpa_financials;

CREATE TABLE fpa.unified_fpa_financials AS (
	SELECT
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
		account_id,
		child,
		parent_id,
		parent,
		fs,
		fsli,
		bucket1,
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
		amount/1000 AS amount,
		scenario,
		team,
		contact,
		contact_email,
		project,
		CASE 
			WHEN bucket1 IN('Office & Terminals', 'HW Builds - CAPEX', 'Other Capex')
				AND child = '1670 - placeholder' THEN
				'Non-Cash Financials'
			WHEN bucket1 = 'Revenue/Commercial Receipts' THEN
				'Revenue'
			WHEN bucket1 IN('Personnel - Base', 'Personnel - Other') THEN
				'Personnel'
			WHEN bucket1 IN('Office & Terminals', 'HW Builds - CAPEX', 'Other Capex') THEN
				'Capex'
			WHEN bucket1 = 'Depreciation' THEN
				'Depreciation'
			WHEN bucket1 = 'Headcount' THEN
				'Headcount'
			WHEN bucket1 = 'Vehicle Leases' THEN
				'Omitted accounts'
			WHEN bucket1 = 'Omit' THEN
				'Omitted accounts'
			WHEN bucket1 = 'Allocations' THEN 
				'Allocations'
			ELSE
				'Other Opex'
		END AS category,
		CASE 
			WHEN category = 'Revenue' THEN
				'1. Revenue'
			WHEN category = 'Personnel'
				OR category = 'Other Opex' THEN
				'2. OPEX'
			WHEN category = 'Capex' THEN
				'3. CAPEX'
			WHEN category = 'Allocations' THEN
				'4. Allocations'
			ELSE
				'Other'
		END AS p_l_location
	FROM
		fpa.fpa_actuals
	WHERE bucket1 <> 'Omit'
UNION ALL
SELECT
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
  account_id,
  child,
  parent_id,
  parent,
  fs,
  fsli,
  bucket1,
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
  amount/1000 AS amount,
  scenario,
  team,
  contact,
  contact_email,
  project,
  CASE 
			WHEN bucket1 IN('Office & Terminals', 'HW Builds - CAPEX', 'Other Capex')
				AND child = '1670 - placeholder' THEN
				'Non-Cash Financials'
			WHEN bucket1 = 'Revenue/Commercial Receipts' THEN
				'Revenue'
			WHEN bucket1 IN('Personnel - Base', 'Personnel - Other') THEN
				'Personnel'
			WHEN bucket1 IN('Office & Terminals', 'HW Builds - CAPEX', 'Other Capex') THEN
				'Capex'
			WHEN bucket1 = 'Depreciation' THEN
				'Depreciation'
			WHEN bucket1 = 'Headcount' THEN
				'Headcount'
			WHEN bucket1 = 'Vehicle Leases' THEN
				'Omitted accounts'
			WHEN bucket1 = 'Omit' THEN
				'Omitted accounts'
			WHEN bucket1 = 'Allocations' THEN 
				'Allocations'
			ELSE
				'Other Opex'
		END AS category,
		CASE 
			WHEN category = 'Revenue' THEN
				'1. Revenue'
			WHEN category = 'Personnel'
				OR category = 'Other Opex' THEN
				'2. OPEX'
			WHEN category = 'Capex' THEN
				'3. CAPEX'
			WHEN category = 'Allocations' THEN
				'4. Allocations'
			ELSE
				'Other'
		END AS p_l_location
	FROM fpa.forecasts_master_tbl
WHERE bucket1 <> 'Omit'
UNION ALL
SELECT
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
  account_id,
  child,
  parent_id,
  parent,
  fs,
  fsli,
  bucket1,
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
  scenario,
  team,
  contact,
  contact_email,
  project,
  'Loads And Miles' AS category,
  'Loads And Miles' AS p_l_location
 FROM fpa.fpa_miles_loads_archive
);

UPDATE fpa.unified_fpa_financials
SET sub_category = 'Disposal',bucket1 = 'Disposals (Non-Cash)',p_l_location='3. CAPEX',category='Capex',ops_category='Disposals (Non-Cash)',ops_sub_category='Disposals (Non-Cash)'
WHERE memo LIKE '%Asset Write%' AND child LIKE '16%';



--This query created the simplified financials
DROP TABLE fpa.unified_fpa_financials_simplified;

CREATE TABLE fpa.unified_fpa_financials_simplified AS (
	SELECT 
	accounting_period_end_date,
	department_child_1,
	division,
	standard_location,
	vendor,
	child,
	parent,
	fs,
	fsli,
	bucket1,
	sub_category,
	category,
	p_l_location,
	project_group,
	scenario,
	SUM(amount) AS amount
	FROM 
	fpa.unified_fpa_financials
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);