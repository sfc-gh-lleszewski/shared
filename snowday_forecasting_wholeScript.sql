-- Using accountadmin is often suggested for quickstarts, but any role with sufficient privledges can work
USE ROLE SYSADMIN;

-- Create development database, schema for our work: 
CREATE OR REPLACE DATABASE quickstart;
CREATE OR REPLACE SCHEMA ml_functions;

-- Use appropriate resources: 
USE DATABASE quickstart;
USE SCHEMA ml_functions;

-- Create warehouse to work with: 
CREATE OR REPLACE WAREHOUSE quickstart_wh;
USE WAREHOUSE quickstart_wh;

-- Set search path for ML Functions:
-- ref: https://docs.snowflake.com/en/user-guide/ml-powered-forecasting#preparing-for-forecasting
ALTER ACCOUNT
SET SEARCH_PATH = '$current, $public, SNOWFLAKE.ML';

-- Create a csv file format to be used to ingest from the stage: 
CREATE OR REPLACE FILE FORMAT quickstart.ml_functions.csv_ff
    type = 'csv'
    SKIP_HEADER = 1,
    COMPRESSION = AUTO;

-- Create an external stage pointing to AWS S3 for loading sales data: 
CREATE OR REPLACE STAGE s3load 
    COMMENT = 'Quickstart S3 Stage Connection'
    url = 's3://sfquickstarts/frostbyte_tastybytes/mlpf_quickstart/'
    file_format = quickstart.ml_functions.csv_ff;

-- Define Tasty Byte Sales table
CREATE OR REPLACE TABLE quickstart.ml_functions.tasty_byte_sales(
  	DATE DATE,
	PRIMARY_CITY VARCHAR(16777216),
	MENU_ITEM_NAME VARCHAR(16777216),
	TOTAL_SOLD NUMBER(17,0)
);

-- Ingest data from S3 into our table
COPY INTO quickstart.ml_functions.tasty_byte_sales 
    FROM @s3load/ml_functions_quickstart.csv;

-- View a sample of the ingested data: 
SELECT * FROM quickstart.ml_functions.tasty_byte_sales LIMIT 100;


-- Note, your database and schema context for this table was set on page 2 step 2
-- query a sample of the ingested data
SELECT *
    FROM tasty_byte_sales
    WHERE menu_item_name LIKE 'Lobster Mac & Cheese';


-- Create table containing the latest years worth of sales data: 
CREATE OR REPLACE TABLE vancouver_sales AS (
    SELECT
        to_timestamp_ntz(date) AS timestamp,
        primary_city,
        menu_item_name,
        total_sold
    FROM
        tasty_byte_sales
    WHERE
        date > (SELECT max(date) - interval '1 year' FROM tasty_byte_sales)
    GROUP BY
        all
        );

 SELECT
        timestamp,
        total_sold
    FROM
        vancouver_sales
    WHERE
        menu_item_name LIKE 'Lobster Mac & Cheese';

-- Create view for lobster sales
CREATE OR REPLACE VIEW lobster_sales AS (
    SELECT
        timestamp,
        total_sold
    FROM
        vancouver_sales
    WHERE
        menu_item_name LIKE 'Lobster Mac & Cheese'
);

-- Build Forecasting model; this could take ~15-25 secs; please be patient
CREATE OR REPLACE forecast lobstermac_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'lobster_sales'),
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD'
);

-- Show models to confirm training has completed
SHOW forecast;


-- Create predictions, and save results to a table:  
CALL lobstermac_forecast!FORECAST(FORECASTING_PERIODS => 10);

-- Run immediately after the above call to store results!
CREATE OR REPLACE TABLE macncheese_predictions AS (
    SELECT
        *
    FROM
        TABLE(RESULT_SCAN(-1))
);

-- Visualize the results, overlaid on top of one another: 
SELECT
    timestamp,
    total_sold,
    NULL AS forecast
FROM
    lobster_sales
WHERE
    timestamp > '2023-03-01'
UNION
SELECT
    TS AS timestamp,
    NULL AS total_sold,
    forecast
FROM
    macncheese_predictions
ORDER BY
    timestamp asc;


-- Run the forecast; this could take <15 secs
CALL lobstermac_forecast!FORECAST(FORECASTING_PERIODS => 10, CONFIG_OBJECT => {'prediction_interval': .9});    



-- Create a view for the holidays in Vancouver, which is located in British Columbia (BC) in Cananda (CA)
CREATE OR REPLACE VIEW canadian_holidays AS (
    SELECT
        date,
        holiday_name,
        is_financial
    FROM
        frostbyte_cs_public.cybersyn.public_holiday_calendar
    WHERE
        ISO_ALPHA2 LIKE 'CA'
        AND date > '2022-01-01'
        AND (
            subdivision IS null
            OR subdivision LIKE 'BC'
        )
    ORDER BY
        date ASC
);

-- Create a view for our training data, including the holidays for all items sold
CREATE OR REPLACE VIEW allitems_vancouver AS (
    SELECT
        vs.timestamp,
        vs.menu_item_name,
        vs.total_sold,
        ch.holiday_name
    FROM 
        vancouver_sales vs
        LEFT JOIN canadian_holidays ch ON vs.timestamp = ch.date
    WHERE MENU_ITEM_NAME IN ('Mothers Favorite', 'Bottled Soda', 'Ice Tea')
);

-- Train Model; this could take ~15-25 secs; please be patient
CREATE OR REPLACE forecast vancouver_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'allitems_vancouver'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD'
);

-- show it
SHOW forecast;


-- Retrieve the latest date from our input dataset, which is 05/28/2023: 
SELECT MAX(timestamp) FROM vancouver_sales;

-- Create view for inference data
CREATE OR REPLACE VIEW vancouver_forecast_data AS (
    WITH future_dates AS (
        SELECT
            '2023-05-28' ::DATE + row_number() OVER (
                ORDER BY
                    0
            ) AS timestamp
        FROM
            TABLE(generator(rowcount => 10))
    ),
    food_items AS (
        SELECT
            DISTINCT menu_item_name
        FROM
            allitems_vancouver
    ),
    joined_menu_items AS (
        SELECT
            *
        FROM
            food_items
            CROSS JOIN future_dates
        ORDER BY
            menu_item_name ASC,
            timestamp ASC
    )
    SELECT
        jmi.menu_item_name,
        to_timestamp_ntz(jmi.timestamp) AS timestamp,
        ch.holiday_name
    FROM
        joined_menu_items AS jmi
        LEFT JOIN canadian_holidays ch ON jmi.timestamp = ch.date
    ORDER BY
        jmi.menu_item_name ASC,
        jmi.timestamp ASC
);
select * from vancouver_forecast_data;
-- Call the model on the forecast data to produce predictions: 
CALL vancouver_forecast!forecast(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_forecast_data'),
        SERIES_COLNAME => 'menu_item_name',
        TIMESTAMP_COLNAME => 'timestamp'
    );

-- Store results into a table: 
CREATE OR REPLACE TABLE vancouver_predictions AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

select * from vancouver_predictions;
CALL VANCOUVER_FORECAST!explain_feature_importance();


-- Evaluate model performance:
CALL VANCOUVER_FORECAST!show_evaluation_metrics();


-- Create a view containing our training data
CREATE OR REPLACE VIEW vancouver_anomaly_training_set AS (
    SELECT *
    FROM vancouver_sales
    WHERE timestamp < (SELECT MAX(timestamp) FROM vancouver_sales) - interval '1 Month'
);

-- Create a view containing the data we want to make inferences on
CREATE OR REPLACE VIEW vancouver_anomaly_analysis_set AS (
    SELECT *
    FROM vancouver_sales
    WHERE timestamp > (SELECT MAX(timestamp) FROM vancouver_anomaly_training_set)
);

-- Create the model: UNSUPERVISED method, however can pass labels as well; this could take ~15-25 secs; please be patient 
CREATE OR REPLACE snowflake.ml.anomaly_detection vancouver_anomaly_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_training_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    LABEL_COLNAME => ''
); 

-- Call the model and store the results into table; this could take ~10-20 secs; please be patient
CALL vancouver_anomaly_model!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_analysis_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

-- Create a table from the results
CREATE OR REPLACE TABLE vancouver_anomalies AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

-- Review the results
SELECT series,ts,forecast,* FROM vancouver_anomalies;


-- Query to identify trends
SELECT series, is_anomaly, count(is_anomaly) AS num_records
FROM vancouver_anomalies
WHERE is_anomaly =1
GROUP BY ALL
ORDER BY num_records DESC
LIMIT 5;

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