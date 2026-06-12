-- raw data profiling checks for Redshift
-- run these queries after loading raw.online_retail to validate the incoming data
-- the checks and logic are identical to the local PostgreSQL version
-- Redshift supports all standard SQL used here

-- total row count
SELECT COUNT(*) AS total_rows
FROM raw.online_retail;

-- NULL profiling on critical fields
-- customer_id nulls are expected and preserved by design
SELECT
    COUNT(*) - COUNT(invoice)      AS null_invoice,
    COUNT(*) - COUNT(stockcode)    AS null_stockcode,
    COUNT(*) - COUNT(description)  AS null_description,
    COUNT(*) - COUNT(quantity)     AS null_quantity,
    COUNT(*) - COUNT(price)        AS null_price,
    COUNT(*) - COUNT(customer_id)  AS null_customer_id
FROM raw.online_retail;

-- description spacing issues (leading or trailing whitespace)
SELECT COUNT(*) AS rows_with_spacing_issues
FROM raw.online_retail
WHERE description LIKE ' %'
   OR description LIKE '% ';

-- negative and zero value checks
-- negative quantities indicate returns and are intentionally kept
-- negative prices are excluded during staging
SELECT
    COUNT(CASE WHEN quantity < 0 THEN 1 END) AS negative_quantity,
    COUNT(CASE WHEN quantity = 0 THEN 1 END) AS zero_quantity,
    COUNT(CASE WHEN price < 0    THEN 1 END) AS negative_price,
    COUNT(CASE WHEN price = 0    THEN 1 END) AS zero_price
FROM raw.online_retail;

-- cancelled invoices (invoice starts with C)
SELECT COUNT(*) AS cancelled_invoices
FROM raw.online_retail
WHERE invoice LIKE 'C%';

-- invoice date range to verify both yearly files loaded correctly
SELECT
    MIN(invoicedate) AS min_invoicedate,
    MAX(invoicedate) AS max_invoicedate
FROM raw.online_retail;

-- country distribution to spot unexpected values
SELECT country, COUNT(*) AS cnt
FROM raw.online_retail
GROUP BY country
ORDER BY cnt DESC;

-- duplicate business key check (same invoice, stockcode, invoicedate)
SELECT invoice, stockcode, invoicedate, COUNT(*) AS cnt
FROM raw.online_retail
GROUP BY invoice, stockcode, invoicedate
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;

-- price range to detect outliers
SELECT
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM raw.online_retail;

-- quantity range to detect outliers
SELECT
    MIN(quantity) AS min_quantity,
    MAX(quantity) AS max_quantity
FROM raw.online_retail;

-- top 10 rows by absolute quantity to inspect large orders and large returns
SELECT invoice, stockcode, quantity, price
FROM raw.online_retail
ORDER BY ABS(quantity) DESC
LIMIT 10;

-- top 10 rows by highest price to identify premium or data quality issues
SELECT invoice, stockcode, quantity, price
FROM raw.online_retail
ORDER BY price DESC
LIMIT 10;

-- rows with negative prices (should be excluded during staging)
SELECT invoice, stockcode, quantity, price
FROM raw.online_retail
WHERE price < 0
ORDER BY price
LIMIT 50;

-- non-product and financial adjustment rows that will be excluded in staging
SELECT stockcode, description, COUNT(*) AS cnt
FROM raw.online_retail
WHERE stockcode = 'M'
   OR description IN ('BANK CHARGES', 'AMAZONFEE')
GROUP BY stockcode, description
ORDER BY cnt DESC;
