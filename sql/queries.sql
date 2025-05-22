CREATE DATABASE WALRUS_ECOMERSE_DB;

USE WALRUS_ECOMERSE_DB;

CREATE SCHEMA STAGE_EXTERNAL;

USE SCHEMA STAGE_EXTERNAL;

CREATE OR REPLACE STAGE ecommerce_stage
URL='s3://walrus-snowflake'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

CREATE OR REPLACE TABLE RAW_ECOMMERSE_DATA (
    Order_ID INT,
    Customer_ID STRING,
    Customer_Name STRING,
    Order_Date STRING,
    Product STRING,
    Quantity INT,
    Price INT,
    Discount FLOAT,
    Total_Amount FLOAT,
    Payment_Method STRING,
    Shipping_Address STRING,
    Status STRING
);

COPY INTO RAW_ECOMMERSE_DATA
FROM @STAGE_EXTERNAL.ECOMMERSE_STAGE/ecommerce_orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'CONTINUE';

COPY INTO raw_orders
FROM @ext_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE td_for_review LIKE raw_orders;
CREATE OR REPLACE TABLE td_suspisios_records LIKE raw_orders;
CREATE OR REPLACE TABLE td_invalid_date_format LIKE raw_orders;
CREATE OR REPLACE TABLE td_invalid_qty_price LIKE raw_orders;
CREATE OR REPLACE TABLE td_clean_records LIKE raw_orders;

INSERT INTO td_for_review
SELECT * FROM raw_orders
WHERE delivery_address IS NULL AND LOWER(order_status) = 'delivered';

INSERT INTO td_suspisios_records
SELECT * FROM raw_orders
WHERE customer_id IS NULL;

UPDATE raw_orders
SET payment_method = 'Unknown'
WHERE payment_method IS NULL;

INSERT INTO td_invalid_date_format
SELECT * FROM raw_orders
WHERE TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NULL;

UPDATE raw_orders
SET order_date = TO_DATE(order_date, 'MM/DD/YYYY')
WHERE TRY_TO_DATE(order_date, 'MM/DD/YYYY') IS NOT NULL;

INSERT INTO td_invalid_qty_price
SELECT * FROM raw_orders
WHERE quantity <= 0 OR unit_price <= 0;

DELETE FROM raw_orders
WHERE quantity <= 0 OR unit_price <= 0;

UPDATE raw_orders
SET discount = 0
WHERE discount < 0;

UPDATE raw_orders
SET discount = 50
WHERE discount > 50;

UPDATE raw_orders
SET final_price = quantity * unit_price * (1 - discount / 100);

UPDATE raw_orders
SET order_status = 'Pending'
WHERE delivery_address IS NULL AND LOWER(order_status) = 'delivered';

DELETE FROM raw_orders
WHERE ROWID NOT IN (
  SELECT MIN(ROWID)
  FROM raw_orders
  GROUP BY *
);

INSERT INTO td_clean_records
SELECT * FROM raw_orders
WHERE order_id NOT IN (
  SELECT order_id FROM td_for_review
  UNION
  SELECT order_id FROM td_suspisios_records
  UNION
  SELECT order_id FROM td_invalid_date_format
  UNION
  SELECT order_id FROM td_invalid_qty_price
);


