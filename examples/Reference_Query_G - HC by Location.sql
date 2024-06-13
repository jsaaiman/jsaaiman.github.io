WITH peep_loc AS (SELECT 
w.*,
b.location_id,
l.simplified_location AS standard_location,
c.division
FROM squad.hris_workday w
LEFT JOIN fpa.fpa_buildings b ON w.locationid = b.building_code
JOIN fpa.fpa_location_codes l ON b.location_id = l.location_id
JOIN fpa.fpa_cost_centers c ON w.costcenterid = c.department_id
)
SELECT 
division,
standard_location,
COUNT(employeeid)
FROM peep_loc
WHERE LEFT(employeeid, 1) <> 'C' 
GROUP BY 1,2;
