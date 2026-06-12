-- staging validation checks for Redshift
-- run these after the Glue job rebuilds staging.online_retail_clean
-- confirms that all data quality rules were applied correctly

-- row count in staging
SELECT COUNT(*) AS staging_row_count
FROM staging.online_retail_clean;

-- confirm no negative prices exist (excluded during staging transformation)
SELECT COUNT(*) AS negative_price_rows
FROM staging.online_retail_clean
WHERE price < 0;

-- confirm non-product rows were excluded
SELECT COUNT(*) AS non_product_rows
FROM staging.online_retail_clean
WHERE stockcode = 'M'
   OR description IN ('BANK CHARGES', 'AMAZONFEE');

-- confirm no leading or trailing whitespace remains in descriptions
SELECT COUNT(*) AS spacing_issues_remaining
FROM staging.online_retail_clean
WHERE description <> TRIM(description);

-- validate total_price calculation matches quantity times price
SELECT COUNT(*) AS incorrect_total_price_rows
FROM staging.online_retail_clean
WHERE ROUND(total_price, 2) <> ROUND(CAST(quantity AS DECIMAL(12,2)) * price, 2);

-- expected staging row count calculated directly from raw using the same filter rules
SELECT COUNT(*) AS expected_staging_rows
FROM raw.online_retail
WHERE quantity IS NOT NULL
  AND price IS NOT NULL
  AND price >= 0
  AND stockcode <> 'M'
  AND COALESCE(description, '') NOT IN ('BANK CHARGES', 'AMAZONFEE');

-- confirm returns (negative quantities) are preserved in staging
SELECT COUNT(*) AS negative_quantity_rows
FROM staging.online_retail_clean
WHERE quantity < 0;

-- confirm cancelled invoices are preserved in staging
SELECT COUNT(*) AS cancelled_invoice_rows
FROM staging.online_retail_clean
WHERE invoice LIKE 'C%';

-- compare staging row count against expected to quickly detect mismatches
SELECT
    (SELECT COUNT(*) FROM staging.online_retail_clean)                    AS staging_rows,
    (SELECT COUNT(*) FROM raw.online_retail
     WHERE quantity IS NOT NULL AND price IS NOT NULL AND price >= 0
       AND stockcode <> 'M'
       AND COALESCE(description, '') NOT IN ('BANK CHARGES', 'AMAZONFEE')) AS expected_rows;
