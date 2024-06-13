WITH ActualMiles AS (
  SELECT
    LAST_DAY(start_date) AS period,
  	'Actuals' AS scenario,
  	'Miles' AS bucket1,
    ROUND(SUM(logged_miles + est_unlogged_miles),2) AS miles
  FROM 
    dbt_prod.fleet_mileage
  GROUP BY 1,2,3
),
ForecastMiles AS (
SELECT
    LAST_DAY(accounting_period_end_date) AS period,
  	scenario,
	bucket1,
	ROUND(SUM(amount),2) AS miles
FROM
	fpa.unified_fpa_financials
WHERE bucket1 IN ('Miles')
GROUP BY  
	1,2,3
),
Dollars AS (
SELECT
    LAST_DAY(accounting_period_end_date) AS period,
  	scenario,
	bucket1,
	ROUND(SUM(amount)*1000,2) AS amount
FROM
	fpa.unified_fpa_financials
WHERE bucket1 NOT IN ('Miles')
GROUP BY  
	1,2,3
),
ConsolidatedMiles AS (
SELECT * FROM ActualMiles
UNION ALL 
SELECT * FROM ForecastMiles
)
SELECT 
d.*,
m.miles,
ROUND(d.amount/m.miles,2) AS dollar_per_mile
FROM Dollars d
JOIN ConsolidatedMiles m ON d.period = m.period AND d.scenario = m.scenario
WHERE d.scenario = 'Actuals'
;
