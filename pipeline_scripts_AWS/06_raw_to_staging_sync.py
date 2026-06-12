import sys
import os
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, DecimalType
)

# initialize the Glue and Spark contexts
# these are required entry points for every AWS Glue job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# configuration loaded from environment variables
# Glue passes these in via the job parameters defined in Terraform
# -------------------------------------------------------------------
S3_BUCKET       = os.environ["S3_BUCKET"]
S3_RAW_PREFIX   = os.environ.get("S3_RAW_PREFIX", "raw/")
REDSHIFT_URL    = os.environ["REDSHIFT_JDBC_URL"]
REDSHIFT_USER   = os.environ["REDSHIFT_USER"]
REDSHIFT_PASS   = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_TMPDIR = os.environ["REDSHIFT_TMPDIR_S3"]

# paths to both source CSV files stored in S3
# the pipeline reads both yearly datasets and combines them before processing
RAW_SOURCE_FILES = [
    f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}Year_2009-2010_online_retail_II.csv",
    f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}Year_2010-2011_online_retail_II.csv",
]

# explicit schema enforced on read
# this prevents Spark from inferring wrong types from the CSV
CSV_SCHEMA = StructType([
    StructField("invoice",      StringType(),      True),
    StructField("stockcode",    StringType(),      True),
    StructField("description",  StringType(),      True),
    StructField("quantity",     IntegerType(),     True),
    StructField("invoicedate",  TimestampType(),   True),
    StructField("price",        DecimalType(12,2), True),
    StructField("customer_id",  IntegerType(),     True),
    StructField("country",      StringType(),      True),
])

# JDBC connection options reused across all Redshift read and write calls
REDSHIFT_OPTS = {
    "url":      REDSHIFT_URL,
    "user":     REDSHIFT_USER,
    "password": REDSHIFT_PASS,
    "driver":   "com.amazon.redshift.jdbc42.Driver",
    "tempdir":  REDSHIFT_TMPDIR,
}


def read_csv_from_s3(file_path):
    # read a single CSV file from S3 into a Spark DataFrame
    # header=true uses the first row as column names
    # enforceSchema applies the explicit schema defined above
    return spark.read \
        .option("header", "true") \
        .option("timestampFormat", "M/d/yyyy H:mm") \
        .schema(CSV_SCHEMA) \
        .csv(file_path)


def read_redshift_table(table_name):
    # read an existing Redshift table into a Spark DataFrame via JDBC
    return spark.read \
        .format("jdbc") \
        .option("url",      REDSHIFT_OPTS["url"]) \
        .option("dbtable",  table_name) \
        .option("user",     REDSHIFT_OPTS["user"]) \
        .option("password", REDSHIFT_OPTS["password"]) \
        .option("driver",   REDSHIFT_OPTS["driver"]) \
        .load()


def write_to_redshift(df, table_name, mode="append"):
    # write a Spark DataFrame to a Redshift table
    # mode append adds new rows without touching existing ones
    # mode overwrite truncates the table and reloads it (used for staging)
    # tempdir is an S3 path Glue uses as a staging area during the COPY operation
    df.write \
        .format("io.github.spark_redshift_utils.spark_redshift") \
        .option("url",         REDSHIFT_OPTS["url"]) \
        .option("dbtable",     table_name) \
        .option("user",        REDSHIFT_OPTS["user"]) \
        .option("password",    REDSHIFT_OPTS["password"]) \
        .option("driver",      REDSHIFT_OPTS["driver"]) \
        .option("tempdir",     REDSHIFT_OPTS["tempdir"]) \
        .option("tempformat",  "CSV") \
        .mode(mode) \
        .save()


def sync_raw_table():
    # read both yearly CSV files from S3 and combine them into one DataFrame
    # unionByName aligns columns by name regardless of order
    print("Reading CSV files from S3")
    df_list = [read_csv_from_s3(f) for f in RAW_SOURCE_FILES]
    df_incoming = df_list[0].unionByName(df_list[1])

    # read what is already loaded in the raw table
    # this is used to detect new rows and prevent duplicate ingestion
    print("Reading existing raw table from Redshift")
    df_existing = read_redshift_table("raw.online_retail")

    # identify new rows by finding incoming rows that do not exist in raw
    # the composite key matches the same deduplication logic used in the local pipeline
    df_new = df_incoming.join(
        df_existing.select(
            "invoice", "stockcode", "invoicedate",
            "quantity", "price", "country"
        ),
        on=["invoice", "stockcode", "invoicedate", "quantity", "price", "country"],
        how="left_anti"
    )

    new_row_count = df_new.count()
    print(f"New rows to insert into raw: {new_row_count}")

    if new_row_count > 0:
        # append only the new rows to raw.online_retail
        write_to_redshift(df_new, "raw.online_retail", mode="append")
        print("Raw sync complete")
    else:
        print("No new rows detected. Raw table is up to date.")


def rebuild_staging_table():
    # read the full raw table after the sync is complete
    print("Reading raw table to rebuild staging")
    df_raw = read_redshift_table("raw.online_retail")

    # apply the same data quality rules as the local pipeline:
    # remove null quantities and prices, remove negative prices,
    # exclude non-product rows (M, BANK CHARGES, AMAZONFEE),
    # trim whitespace from descriptions,
    # compute total_price as quantity multiplied by price
    df_staging = df_raw \
        .filter(F.col("quantity").isNotNull()) \
        .filter(F.col("price").isNotNull()) \
        .filter(F.col("price") >= 0) \
        .filter(F.col("stockcode") != "M") \
        .filter(~F.col("description").isin(["BANK CHARGES", "AMAZONFEE"])) \
        .withColumn("description", F.trim(F.col("description"))) \
        .withColumn("total_price", (F.col("quantity") * F.col("price")).cast(DecimalType(14,2)))

    print(f"Staging rows after filtering: {df_staging.count()}")

    # overwrite the staging table so it always reflects the latest raw data
    # staging is designed to be fully rebuildable from raw at any time
    write_to_redshift(df_staging, "staging.online_retail_clean", mode="overwrite")
    print("Staging rebuild complete")


def main():
    print("Starting raw to staging sync")
    sync_raw_table()
    rebuild_staging_table()
    print("Pipeline step complete")


main()
job.commit()
