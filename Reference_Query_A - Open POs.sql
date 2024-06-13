 WITH purchase_orders AS (  SELECT 
	-- PO Header info
    po.po_number,
    po.status as po_status,
    po.created_at,
    po.ship_to_user_fullname AS created_by,
    po.updated_by_fullname AS updated_by,
    po.supplier_display_name,
    CAST(LISTAGG(po_line.description, '; ') WITHIN GROUP (ORDER BY po_line.line_num) OVER (PARTITION BY po.po_number) AS CHARACTER VARYING (128)) AS po_description,
    -- PO Line info
    po_line.line_num,
    po_line.status AS line_status,
    po_line.description,
    po_line.currency_code,
    -- Seed Table Pull In
    g.account_id,
    g.child_gl_account,
    g.parent_id,
    g.parent_gl_account,
    g.mgmt_bucket,
    g.fs,
    c.department_child_1,
    c.division,
    l.simplified_location,
    -- Financial Data
    ROUND(po_line.accounting_total, 2) AS total_USD,
    po_line.price,
    po_line.quantity,
    ROUND(po_line.total,2) AS total_LCL,
    po_line.received,
    po_line.invoiced,
    CASE WHEN po_line.quantity IS NOT NULL THEN 
    ROUND(ROUND(po_line.invoiced, 2) * ROUND(po_line.price, 2), 2) ELSE po_line.invoiced END AS total_billed,
    ROUND(po_line.total, 2) - total_billed AS remaining
FROM fq_hma_prod.coupa_po_lines po_line
--Joins
LEFT JOIN fpa.fpa_glaccounts g ON SPLIT_PART(po_line.account_code, '-', 4) = g.account_id
LEFT JOIN fpa.fpa_cost_centers c ON SPLIT_PART(po_line.account_code, '-', 2) = c.department_id
LEFT JOIN fpa.fpa_location_codes l ON SPLIT_PART(po_line.account_code, '-', 3) = l.location_id
JOIN fq_hma_prod.coupa_po po ON po_line.order_header_number = po.po_number
)
, flag_prepaid_pos AS (
	-- CTE for prepaid flag
  SELECT purchase_orders.*,
    CASE 
        WHEN SUM(CASE WHEN child_gl_account = '1500 - Prepaids - Automated' THEN 1 ELSE 0 END) OVER (PARTITION BY po_number) > 0 
        THEN 'Prepaids' 
        ELSE NULL 
    END AS prepaid_tag
  FROM purchase_orders
)
SELECT * FROM flag_prepaid_pos