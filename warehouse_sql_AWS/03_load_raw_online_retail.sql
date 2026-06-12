-- load both yearly CSV files from S3 into raw.online_retail
-- COPY is the recommended way to load data into Redshift at scale
-- it runs in parallel across all Redshift nodes and is much faster than INSERT
-- the IAM role must have s3:GetObject permission on the source bucket
-- replace the placeholder values with your actual S3 bucket name and IAM role ARN

-- load 2009 to 2010 dataset
COPY raw.online_retail (
    invoice,
    stockcode,
    description,
    quantity,
    invoicedate,
    price,
    customer_id,
    country
)
FROM 's3://YOUR-BUCKET-NAME/raw/Year_2009-2010_online_retail_II.csv'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT-ID:role/YOUR-REDSHIFT-ROLE'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'MM/DD/YYYY HH:MI'
EMPTYASNULL
BLANKSASNULL;

-- load 2010 to 2011 dataset
COPY raw.online_retail (
    invoice,
    stockcode,
    description,
    quantity,
    invoicedate,
    price,
    customer_id,
    country
)
FROM 's3://YOUR-BUCKET-NAME/raw/Year_2010-2011_online_retail_II.csv'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT-ID:role/YOUR-REDSHIFT-ROLE'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'MM/DD/YYYY HH:MI'
EMPTYASNULL
BLANKSASNULL;
