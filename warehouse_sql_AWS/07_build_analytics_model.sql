-- build the analytics star schema in Redshift
-- this file creates all dimension and fact tables and loads them from staging
-- the structure mirrors the local PostgreSQL version exactly
-- Redshift-specific adjustments are noted inline

-- step 1: create the analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;


-- step 2: create dimension tables

-- dim_date stores one row per calendar date with attributes used for time-based analysis
-- BIGINT IDENTITY is the Redshift equivalent of PostgreSQL BIGSERIAL
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key     INTEGER NOT NULL,
    full_date    DATE NOT NULL,
    day          INTEGER NOT NULL,
    day_of_week  INTEGER NOT NULL,
    day_name     VARCHAR(10) NOT NULL,
    week_of_year INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   VARCHAR(10) NOT NULL,
    year         INTEGER NOT NULL,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);

-- DISTSTYLE ALL broadcasts dim_date to all nodes
-- this avoids network shuffling when joining to fact_sales

-- dim_country normalizes country names and assigns a surrogate key
CREATE TABLE IF NOT EXISTS analytics.dim_country (
    country_id  BIGINT IDENTITY(1,1),
    country     VARCHAR(100) NOT NULL,
    PRIMARY KEY (country_id)
)
DISTSTYLE ALL;

-- dim_product stores one row per product with its most recent description
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    stockcode    VARCHAR(20) NOT NULL,
    product_name VARCHAR(500),
    PRIMARY KEY (stockcode)
)
DISTSTYLE ALL;

-- dim_customer stores one row per known customer with first and last seen dates
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id     INTEGER NOT NULL,
    first_seen_date DATE,
    last_seen_date  DATE,
    PRIMARY KEY (customer_id)
)
DISTSTYLE ALL;


-- step 3: create the fact table
-- fact_sales stores one row per invoice line linked to all four dimensions
-- DISTSTYLE KEY on date_key distributes rows evenly across Redshift nodes
-- foreign key constraints are defined but not enforced in Redshift (noted below)
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    fact_id     BIGINT IDENTITY(1,1),
    invoice     VARCHAR(20)    NOT NULL,
    stockcode   VARCHAR(20)    NOT NULL,
    customer_id INTEGER,
    country_id  BIGINT         NOT NULL,
    date_key    INTEGER        NOT NULL,
    quantity    INTEGER        NOT NULL,
    price       DECIMAL(12,2)  NOT NULL,
    total_price DECIMAL(14,2)  NOT NULL,
    invoice_ts  TIMESTAMP      NOT NULL,
    PRIMARY KEY (fact_id),
    -- Redshift accepts FK syntax but does not enforce referential integrity
    -- the constraints are declared here to document the intended relationships
    FOREIGN KEY (date_key)    REFERENCES analytics.dim_date(date_key),
    FOREIGN KEY (country_id)  REFERENCES analytics.dim_country(country_id),
    FOREIGN KEY (stockcode)   REFERENCES analytics.dim_product(stockcode),
    FOREIGN KEY (customer_id) REFERENCES analytics.dim_customer(customer_id)
)
DISTSTYLE KEY
DISTKEY (date_key)
SORTKEY (date_key, invoice_ts);

-- DISTKEY on date_key collocates fact rows with dim_date for efficient joins
-- SORTKEY on date_key and invoice_ts optimizes time-range scan performance


-- step 4: load all analytics tables from staging
-- truncate all tables together before reloading to avoid constraint issues
-- this makes the load fully rebuildable and idempotent
TRUNCATE TABLE analytics.fact_sales;
TRUNCATE TABLE analytics.dim_country;
TRUNCATE TABLE analytics.dim_product;
TRUNCATE TABLE analytics.dim_customer;
TRUNCATE TABLE analytics.dim_date;


-- load dim_date from distinct invoice dates in staging
INSERT INTO analytics.dim_date (
    date_key, full_date, day, day_of_week, day_name,
    week_of_year, quarter, month, month_name, year
)
SELECT DISTINCT
    CAST(TO_CHAR(invoicedate, 'YYYYMMDD') AS INTEGER)    AS date_key,
    CAST(invoicedate AS DATE)                             AS full_date,
    EXTRACT(DAY FROM invoicedate)::INTEGER                AS day,
    EXTRACT(DOW FROM invoicedate)::INTEGER + 1            AS day_of_week,
    TRIM(TO_CHAR(invoicedate, 'Day'))                     AS day_name,
    EXTRACT(WEEK FROM invoicedate)::INTEGER               AS week_of_year,
    EXTRACT(QUARTER FROM invoicedate)::INTEGER            AS quarter,
    EXTRACT(MONTH FROM invoicedate)::INTEGER              AS month,
    TRIM(TO_CHAR(invoicedate, 'Month'))                   AS month_name,
    EXTRACT(YEAR FROM invoicedate)::INTEGER               AS year
FROM staging.online_retail_clean
WHERE invoicedate IS NOT NULL;


-- load dim_country from distinct country values in staging
INSERT INTO analytics.dim_country (country)
SELECT DISTINCT TRIM(country) AS country
FROM staging.online_retail_clean
WHERE NULLIF(TRIM(country), '') IS NOT NULL
ORDER BY 1;


-- load dim_product with the most descriptive product name per stockcode
INSERT INTO analytics.dim_product (stockcode, product_name)
SELECT
    TRIM(stockcode) AS stockcode,
    MAX(NULLIF(description, '')) AS product_name
FROM staging.online_retail_clean
WHERE NULLIF(TRIM(stockcode), '') IS NOT NULL
GROUP BY TRIM(stockcode);


-- load dim_customer with first and last seen dates per customer
INSERT INTO analytics.dim_customer (customer_id, first_seen_date, last_seen_date)
SELECT
    customer_id,
    MIN(CAST(invoicedate AS DATE)) AS first_seen_date,
    MAX(CAST(invoicedate AS DATE)) AS last_seen_date
FROM staging.online_retail_clean
WHERE customer_id IS NOT NULL
GROUP BY customer_id;


-- load fact_sales by joining staging to the dimension tables
INSERT INTO analytics.fact_sales (
    invoice, stockcode, customer_id, country_id,
    date_key, quantity, price, total_price, invoice_ts
)
SELECT
    s.invoice,
    TRIM(s.stockcode)                                         AS stockcode,
    s.customer_id,
    c.country_id,
    CAST(TO_CHAR(s.invoicedate, 'YYYYMMDD') AS INTEGER)       AS date_key,
    s.quantity,
    s.price,
    s.total_price,
    s.invoicedate                                             AS invoice_ts
FROM staging.online_retail_clean s
JOIN analytics.dim_country c ON c.country = TRIM(s.country)
WHERE s.invoice IS NOT NULL
  AND NULLIF(TRIM(s.stockcode), '') IS NOT NULL
  AND s.invoicedate IS NOT NULL
  AND NULLIF(TRIM(s.country), '') IS NOT NULL;


-- step 5: post-load validation checks

-- compare staging rows to fact rows to confirm all eligible rows loaded
SELECT
    (SELECT COUNT(*) FROM staging.online_retail_clean) AS staging_rows,
    (SELECT COUNT(*) FROM analytics.fact_sales)        AS fact_rows;

-- dimension row counts
SELECT
    (SELECT COUNT(*) FROM analytics.dim_date)     AS dim_date_rows,
    (SELECT COUNT(*) FROM analytics.dim_country)  AS dim_country_rows,
    (SELECT COUNT(*) FROM analytics.dim_product)  AS dim_product_rows,
    (SELECT COUNT(*) FROM analytics.dim_customer) AS dim_customer_rows;

-- revenue sanity check: net revenue includes returns so it will be lower than gross
SELECT ROUND(SUM(total_price), 2) AS net_revenue
FROM analytics.fact_sales;
