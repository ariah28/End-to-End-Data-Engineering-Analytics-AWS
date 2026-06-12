-- create the three schemas that define the layered architecture
-- raw holds data exactly as it arrives from S3
-- staging holds cleaned and validated data ready for transformation
-- analytics holds the final star schema used for reporting and analysis
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
