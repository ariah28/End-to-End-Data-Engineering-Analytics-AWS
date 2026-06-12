import sys
import os
import io
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pandas as pd

# initialize the Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# configuration loaded from environment variables
# Glue passes these in via job parameters defined in Terraform
# -------------------------------------------------------------------
S3_BUCKET          = os.environ["S3_BUCKET"]
S3_OUTPUTS_PREFIX  = os.environ.get("S3_OUTPUTS_PREFIX", "outputs/tableau/")
REDSHIFT_URL       = os.environ["REDSHIFT_JDBC_URL"]
REDSHIFT_USER      = os.environ["REDSHIFT_USER"]
REDSHIFT_PASS      = os.environ["REDSHIFT_PASSWORD"]


def read_redshift_query(sql):
    # run a SQL query against Redshift and return the result as a Spark DataFrame
    return spark.read \
        .format("jdbc") \
        .option("url",      REDSHIFT_URL) \
        .option("dbtable",  f"({sql}) tmp") \
        .option("user",     REDSHIFT_USER) \
        .option("password", REDSHIFT_PASS) \
        .option("driver",   "com.amazon.redshift.jdbc42.Driver") \
        .load()


def fetch_tableau_kpis():
    # query the daily KPI dataset from the analytics star schema
    # this dataset is denormalized and ready for direct use in Tableau
    # Tableau connects to this CSV file in S3 rather than directly to Redshift
    sql = """
        SELECT
            d.full_date                                                     AS full_date,
            d.year                                                          AS year,
            d.month                                                         AS month,
            d.month_name                                                    AS month_name,
            c.country                                                       AS country,
            COUNT(DISTINCT f.invoice)                                       AS daily_orders,
            SUM(CASE WHEN f.total_price > 0 THEN f.total_price ELSE 0 END) AS gross_revenue,
            SUM(CASE WHEN f.total_price < 0 THEN f.total_price ELSE 0 END) AS returns_revenue,
            SUM(f.total_price)                                              AS net_revenue,
            CASE WHEN d.quarter = 4 THEN 1 ELSE 0 END                      AS is_q4
        FROM analytics.fact_sales f
        JOIN analytics.dim_date d    ON f.date_key   = d.date_key
        JOIN analytics.dim_country c ON f.country_id = c.country_id
        GROUP BY d.full_date, d.year, d.month, d.month_name, d.quarter, c.country
        ORDER BY d.full_date, c.country
    """
    df_spark = read_redshift_query(sql)
    return df_spark.toPandas()


def upload_to_s3(df):
    # convert the dataset to CSV and upload to the outputs/tableau folder in S3
    s3_client  = boto3.client("s3")
    s3_key     = f"{S3_OUTPUTS_PREFIX}tableau_daily_kpis.csv"
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"Wrote {len(df):,} rows to s3://{S3_BUCKET}/{s3_key}")


def main():
    df = fetch_tableau_kpis()
    upload_to_s3(df)


main()
job.commit()
