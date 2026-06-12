-- create the raw table that receives data loaded from S3
-- BIGINT IDENTITY is the Redshift equivalent of PostgreSQL BIGSERIAL
-- VARCHAR with explicit lengths is required in Redshift (TEXT is allowed but VARCHAR is preferred)
-- this table is append-only; rows are never updated or deleted here
CREATE TABLE IF NOT EXISTS raw.online_retail (
    id          BIGINT IDENTITY(1,1),
    invoice     VARCHAR(20),
    stockcode   VARCHAR(20),
    description VARCHAR(500),
    quantity    INTEGER,
    invoicedate TIMESTAMP,
    price       DECIMAL(10,2),
    customer_id INTEGER,
    country     VARCHAR(100)
)
DISTSTYLE AUTO
SORTKEY (invoicedate);

-- DISTSTYLE AUTO lets Redshift choose the best distribution strategy based on data size
-- SORTKEY on invoicedate improves performance for time-range queries
