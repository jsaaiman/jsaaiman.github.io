	
	-- Clears out staging table
DELETE FROM fpa.live_forecast_staging WHERE team IS NULL; 
DELETE FROM fpa.live_forecast_staging WHERE team IS NOT NULL; 


-- Takes actuals from fpa_financials_dbt and adds them to staging table per the filters
INSERT INTO fpa.live_forecast_staging
SELECT
	*
FROM
	fpa.fpa_actuals
WHERE
	EXTRACT(MONTH FROM fpa_actuals.accounting_period_end_date) <= 1 --Update month as needed
	AND EXTRACT(YEAR FROM fpa_actuals.accounting_period_end_date) = 2024; --Update year as needed

UPDATE
	fpa.live_forecast_staging
SET
	scenario = 'Live'; -- Sets scenario to live in staging table


-- Appendings data from staging to master and revises scenario as needed for all lines coded to live.
INSERT INTO fpa.forecasts_master_tbl
SELECT
	*
FROM
	fpa.live_forecast_staging;

UPDATE
	fpa.forecasts_master_tbl
SET
	scenario = '2024 1+11' --Update scenario as needed or create a new one
WHERE
	scenario = 'Live';
