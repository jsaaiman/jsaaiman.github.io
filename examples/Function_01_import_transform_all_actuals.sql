--Use this function to pull the entirety of actuals from the netsuite_etl. If you only want to update a portion of the data (say 1 month). This might be needed if model or seed 
--tables have changed and you need to restate historicals. If you just want to update a single month, use function_2. Make sure to update the WHERE clause at the bottom.

DROP TABLE IF EXISTS fpa.fpa_actuals;

CREATE TABLE fpa.fpa_actuals AS
SELECT
    -- Date and Time Information
    f.tran_date,
    f.posting_period,
    f.accounting_period_end_date,
    -- Accounting Details
    CAST(f.subsidiary_no_hierarchy_id AS INTEGER) AS subsidiary_id,
    CAST(f.subsidiary_no_hierarchy AS VARCHAR(256)) AS subsidiary,
    CAST(f.department_no_hierarchy_id AS INTEGER) AS department_id,
    CAST(c.department_child_1 AS VARCHAR(128)) AS department_child_1,
    CAST(c.department_child_2 AS VARCHAR(128)) AS department_child_2,
    CAST(c.department_child_3 AS VARCHAR(128)) AS department_child_3,
    CAST(c.department_child_4 AS VARCHAR(128)) AS department_child_4,
    CAST(c.division AS VARCHAR(128)) AS division,
    CAST(f.location_no_hierarchy_id AS INTEGER) AS location_id,
    l.simplified_location AS standard_location,
    l.state AS nation_state,
    CAST(f.class_no_hierarchy_id AS INTEGER) AS class_id,
    CAST(f.class_no_hierarchy AS VARCHAR(128)) AS class_name,
    CAST(f.account_number AS VARCHAR(128)) AS account_id,
    g.child_gl_account AS child,
    CAST(g.parent_id AS VARCHAR(4)) AS parent_id,
    g.parent_gl_account AS parent,
    CAST(f.type AS VARCHAR(128)) AS expense_type,
    CAST(f.account_type AS VARCHAR(128)) AS account_type,
    CAST(g.fs AS VARCHAR(13)) AS fs,
    CAST(c.fsli AS VARCHAR(10)) AS fsli,
    CAST(
        CASE 
            WHEN f.account_number = 1695 AND c.division IN ('Workplace', 'Corporate IT') THEN 'Office & Terminals'
            WHEN f.account_number = 1695 AND c.division NOT IN ('Workplace', 'Corporate IT') THEN 'Other Capex'
            WHEN c.division = 'Workplace' AND g.mgmt_bucket NOT IN ('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation','Vehicle Leases','Allocations') AND g.fs NOT IN ('Capex') THEN 'Workplace'
            WHEN c.division = 'Operations' AND g.mgmt_bucket NOT IN ('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation','Vehicle Leases','Allocations') AND g.fs NOT IN ('Capex') THEN 'Operations'
            WHEN c.division = 'Vehicle Operations' AND g.fs NOT IN ('Capex') AND g.mgmt_bucket NOT IN ('Headcount', 'T&E', 'SW Licenses', 'Personnel - Base', 'Personnel - Other', 'Omit', 'Revenue/Commercial Receipts', 'COGS', 'Depreciation','Vehicle Leases','Allocations') THEN 'Operations'
            WHEN c.division = 'Hardware' AND g.fs NOT IN ('Capex') AND g.mgmt_bucket = 'HW Builds - OPEX' THEN 'HW Builds - OPEX'
            WHEN c.division = 'Hardware' AND g.fs NOT IN ('Capex') AND g.mgmt_bucket = 'HW Programs' THEN 'HW Programs'
            ELSE g.mgmt_bucket
        END AS VARCHAR(27)
    ) AS bucket1,
    CAST(NULL AS VARCHAR(27)) AS bucket2,
    CAST(g.sub_category AS VARCHAR(128)) AS sub_category,
    CAST(g.ops_category AS VARCHAR(128)) AS ops_category,
    CAST(g.ops_sub_category AS VARCHAR(128)) AS ops_sub_category,
    CAST(g.workplace_category AS VARCHAR(128)) AS workplace_category,
    f.custcol_coupa_po_number AS po_number,
    CAST(f.custcol_ai_po_line_num AS VARCHAR(4)) AS po_line_number,
    COALESCE(NULLIF(f.entity, ''), COALESCE(f.entity_name, f.type || ' - ' || f.tranid)) AS vendor,
    f.tranid AS document_number,
    f.custcol_ai_transaction_commodity AS commodity,
    TRIM(f.line_cseg_ai_project_gl) AS project_code,
    CAST(p.project_group AS VARCHAR(256)) AS project_group,
    CAST(p.project_category AS VARCHAR(256)) AS project_category,
    f.memo,
    CAST(f.item AS VARCHAR(256)) AS item,
    -- Financial Amounts
    CAST(f.currency AS VARCHAR(3)) AS currency,
    CAST(f.amount AS DOUBLE PRECISION) AS amount,
    CAST(f.debit_amount AS DOUBLE PRECISION) AS debit_amount,
    CAST(f.debit_fx_amount AS DOUBLE PRECISION) AS debit_fx_amount,
    CAST(f.credit_amount AS DOUBLE PRECISION) AS credit_amount,
    CAST(f.credit_fx_amount AS DOUBLE PRECISION) AS credit_fx_amount,
    -- Additional Information
    CAST('Actuals' AS VARCHAR(128)) AS scenario,
    CAST(NULL AS VARCHAR(128)) AS team,
    CAST(coup.created_by_fullname AS VARCHAR(128)) AS contact,
    CAST(coup.created_by_email AS VARCHAR(128)) AS contact_email,
    CAST(NULL AS VARCHAR(128)) AS project
FROM
    fpa.financials f
LEFT JOIN fpa.fpa_glaccounts g ON f.account_number = g.account_id
LEFT JOIN fpa.fpa_cost_centers c ON f.department_no_hierarchy_id = c.department_id
LEFT JOIN fpa.fpa_location_codes l ON f.location_no_hierarchy_id = l.location_id
LEFT JOIN fq_hma_prod.coupa_po coup ON f.custcol_coupa_po_number = coup.po_id
LEFT JOIN fpa.fpa_project_codes p ON TRIM(f.line_cseg_ai_project_gl) = p.netsuite_description
WHERE
    f.accounting_period_end_date < '2024-08-01'; -- Update the month here

UPDATE fpa.fpa_actuals
SET sub_category = CASE 
                      WHEN bucket1 IN('HW Programs', 'HW Builds - CAPEX', 'Other Capex') AND LENGTH(project_group) >= 1 THEN project_group
                      WHEN bucket1 IN('HW Programs', 'HW Builds - CAPEX', 'Other Capex') AND (project_group IS NULL OR LENGTH(project_group) < 1) THEN 'Other Projects'
                      WHEN bucket1 IN('Office & Terminals','Placeholder') THEN standard_location
                      ELSE sub_category
                    END;

UPDATE fpa.fpa_actuals
SET amount = -amount
WHERE account_id IN ('4001','4002','4003','4004','4005','4006','7040') AND scenario = 'Actuals';
