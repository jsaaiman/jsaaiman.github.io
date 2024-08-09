-- Use this query to restate historical scenarios if seed tables or bucketing change.
-- Note that this clears out the master forecast table so you will need to rerun function1 first 
-- and function4 afterward.

-- Clear staging and master tables
TRUNCATE TABLE fpa.live_forecast_staging;
TRUNCATE TABLE fpa.forecasts_master_tbl;

-- Insert data from historical_scenarios_bigquery_import into staging table
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
    NULL AS tran_date,
    h.date AS posting_period,
    h.period AS accounting_period_end_date,
    1 AS subsidiary_id,
    h.subsidiary_name AS subsidiary,
    c.department_id AS department_id,
    h.department_name AS department_child_1,
    c.department_child_2 AS department_child_2,
    c.department_child_3 AS department_child_3,
    c.department_child_4 AS department_child_4,
    c.division AS division,
    l.location_id AS location_id,
    l.simplified_location AS standard_location,
    h.state AS nation_state,
    1 AS class_id,
    h.class_name AS class_name,
    LEFT(h.child, 4) AS account_id,
    g.child_gl_account AS child,
    LEFT(h.parent, 4) AS parent_id,
    h.parent AS parent,
    h.type AS expense_type,
    NULL AS account_type,
    h.fs AS fs,
    h.fsli AS fsli,
    CASE 
        WHEN LEFT(h.child, 4) = '1695' AND c.division IN('Workplace', 'Corporate IT') THEN 'Office & Terminals'
        WHEN LEFT(h.child, 4) = '1695' AND c.division NOT IN('Workplace', 'Corporate IT') THEN 'Other Capex'
        WHEN c.division = 'Workplace' AND g.mgmt_bucket NOT IN('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation', 'Vehicle Leases', 'Allocations') AND g.fs NOT IN('Capex') THEN 'Workplace'
        WHEN c.division IN('Operations', 'Vehicle Operations') AND g.fs NOT IN('Capex') AND g.mgmt_bucket NOT IN('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation', 'Vehicle Leases', 'Allocations') THEN 'Operations'
        WHEN c.division = 'Hardware' AND g.fs NOT IN('Capex') AND g.mgmt_bucket = 'HW Builds - OPEX' THEN 'HW Builds - OPEX'
        WHEN c.division = 'Hardware' AND g.fs NOT IN('Capex') AND g.mgmt_bucket = 'HW Programs' THEN 'HW Programs'
        ELSE g.mgmt_bucket
    END AS bucket1,
    h.bucket2 AS bucket2,
    g.sub_category AS sub_category,
    g.ops_category AS ops_category,
    g.ops_sub_category AS ops_sub_category,
    g.workplace_category AS workplace_category,
    h.po_number AS po_number,
    h.po_line_number AS po_line_number,
    h.vendor AS vendor,
    h.document_number AS document_number,
    h.commodity AS commodity,
    h.project_code AS project_code,
    p.project_group AS project_group,
    p.project_category AS project_category,
    h.memo AS memo,
    NULL AS item,
    'USD' AS currency,
    h.amount_net_ AS amount,
    h.debit AS debit_amount,
    0 AS debit_fx_amount,
    h.credit AS credit_amount,
    0 AS credit_fx_amount,
    h.scenario AS scenario,
    h.team AS team,
    h.contact AS contact,
    'format' AS contact_email,
    h.project AS project
FROM
    fpa.historical_scenarios_bigquery_import h
LEFT JOIN fpa.fpa_glaccounts g ON LEFT(h.child, 4) = g.account_id
LEFT JOIN fpa.fpa_cost_centers c ON h.department_name = c.department_child_1
LEFT JOIN fpa.fpa_location_codes l ON h.location1 = l.original_location
LEFT JOIN fpa.fpa_project_codes p ON h.project_code = p.project_code_child;

-- Update subcategory for project-based management buckets
UPDATE fpa.live_forecast_staging
SET sub_category = CASE 
    WHEN bucket1 IN('HW Programs', 'HW Builds - CAPEX', 'Other Capex') AND LEN(project_group) >= 1 THEN project_group
    WHEN bucket1 IN('HW Programs', 'HW Builds - CAPEX', 'Other Capex') AND (project_group IS NULL OR LEN(project_group) < 1) THEN 'Other Projects'
    WHEN bucket1 IN('Office & Terminals', 'Placeholder') THEN standard_location
    ELSE sub_category
END;

-- Append staged data to master table
INSERT INTO fpa.forecasts_master_tbl
SELECT * FROM fpa.live_forecast_staging;

-- Update specific scenario in master table
UPDATE fpa.forecasts_master_tbl
SET scenario = 'Budget 24v1'
WHERE scenario = 'Budget 24v1_rev2';
