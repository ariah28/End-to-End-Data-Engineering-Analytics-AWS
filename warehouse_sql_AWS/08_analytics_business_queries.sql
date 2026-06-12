-- analytics business queries for Redshift
-- these queries answer key business questions using the analytics star schema
-- the logic is identical to the local PostgreSQL version
-- all standard SQL used here is fully supported in Redshift

-- total revenue across the full dataset
SELECT ROUND(SUM(total_price), 2) AS total_revenue
FROM analytics.fact_sales;


-- revenue by year and month to identify trends and seasonality
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.total_price), 2) AS monthly_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- top 10 products by revenue to identify best-selling items
SELECT
    p.stockcode,
    p.product_name,
    ROUND(SUM(f.total_price), 2) AS product_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_product p ON p.stockcode = f.stockcode
GROUP BY p.stockcode, p.product_name
ORDER BY product_revenue DESC
LIMIT 10;


-- top 10 customers by revenue to identify high-value customers
SELECT
    c.customer_id,
    ROUND(SUM(f.total_price), 2) AS customer_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.customer_id
ORDER BY customer_revenue DESC
LIMIT 10;


-- revenue by country to analyze geographic performance
SELECT
    co.country,
    ROUND(SUM(f.total_price), 2) AS country_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_country co ON co.country_id = f.country_id
GROUP BY co.country
ORDER BY country_revenue DESC;


-- average order value calculated as total revenue divided by distinct invoice count
SELECT
    ROUND(SUM(total_price) / COUNT(DISTINCT invoice), 2) AS avg_order_value
FROM analytics.fact_sales;


-- return revenue impact to quantify revenue loss from returns
SELECT
    ROUND(SUM(total_price), 2) AS return_revenue_impact
FROM analytics.fact_sales
WHERE quantity < 0;


-- weekly revenue trend to detect short-term seasonality patterns
SELECT
    d.year,
    d.week_of_year,
    ROUND(SUM(f.total_price), 2) AS weekly_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.week_of_year
ORDER BY d.year, d.week_of_year;
