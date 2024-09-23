-- Using accountadmin is often suggested for quickstarts, but any role with sufficient privledges can work
USE ROLE ACCOUNTADMIN;

-- Use appropriate resources: 
USE DATABASE quickstart;
USE SCHEMA ml_functions;
USE WAREHOUSE quickstart_wh;
--
-- FOR APP
--

--create table for sales one year before
CREATE OR REPLACE TABLE sales_previous_years AS (
    SELECT
        to_timestamp_ntz(date) AS timestamp,
        primary_city,
        menu_item_name,
        total_sold
    FROM
        tasty_byte_sales
    WHERE
        date <= (SELECT max(date) - interval '1 year' FROM tasty_byte_sales)
    GROUP BY
        all
        );

--create view for lobster rasles one year before
CREATE OR REPLACE VIEW lobster_sales_previous_year AS (
    SELECT
        timestamp,
        total_sold
    FROM
        sales_previous_years
    WHERE
        menu_item_name LIKE 'Lobster Mac & Cheese'
        );

--create forecast, coud take 2 minutes         
CREATE OR REPLACE forecast lobstermac_forecast_last_year (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'lobster_sales_previous_year'),
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD'
);

--forecast sales for one year ahead
CALL lobstermac_forecast_last_year!FORECAST(FORECASTING_PERIODS => 365);

--store forecast results
CREATE OR REPLACE TABLE lobster_sales_predictions AS (
    SELECT
        *
    FROM
        TABLE(RESULT_SCAN(-1))
);

create or replace view lobster_sales_forecast as
    select ts as timestamp, trunc(forecast) as forecast from lobster_sales_predictions order by 1;

--procedure to calculate forecast for next days
CREATE OR REPLACE PROCEDURE calculate_forecast(forecast_days INTEGER)
RETURNS TABLE ()
LANGUAGE sql 
AS
BEGIN    
  CALL lobstermac_forecast!FORECAST(FORECASTING_PERIODS => :forecast_days);  
DECLARE res RESULTSET DEFAULT (
    SELECT ts as TIMESTAMP, trunc(forecast) as FORECAST
    FROM TABLE(result_scan(-1)));
BEGIN 
    RETURN table(res);
END;
END;


call calculate_forecast(forecast_days=>1);