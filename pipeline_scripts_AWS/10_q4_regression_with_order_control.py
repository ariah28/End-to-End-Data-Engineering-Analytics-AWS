import sys
import os
import math
import boto3
import io

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pandas as pd
import numpy as np

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
S3_BUCKET         = os.environ["S3_BUCKET"]
S3_OUTPUTS_PREFIX = os.environ.get("S3_OUTPUTS_PREFIX", "outputs/modeling/")
REDSHIFT_URL      = os.environ["REDSHIFT_JDBC_URL"]
REDSHIFT_USER     = os.environ["REDSHIFT_USER"]
REDSHIFT_PASS     = os.environ["REDSHIFT_PASSWORD"]


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


def fetch_daily_dataset():
    # query the daily dataset from the analytics star schema
    # the model uses daily_revenue as the dependent variable
    # and is_q4 plus daily_orders as independent variables
    sql = """
        SELECT
            d.full_date AS order_date,
            SUM(f.total_price) AS daily_revenue,
            COUNT(DISTINCT f.invoice) AS daily_orders,
            CASE WHEN EXTRACT(MONTH FROM d.full_date) IN (10, 11, 12) THEN 1 ELSE 0 END AS is_q4
        FROM analytics.fact_sales f
        JOIN analytics.dim_date d ON d.date_key = f.date_key
        GROUP BY d.full_date
        ORDER BY d.full_date
    """
    df_spark = read_redshift_query(sql)
    df = df_spark.toPandas()
    df["order_date"]    = pd.to_datetime(df["order_date"])
    df["daily_revenue"] = pd.to_numeric(df["daily_revenue"], errors="coerce").fillna(0.0)
    df["daily_orders"]  = pd.to_numeric(df["daily_orders"],  errors="coerce").fillna(0).astype(int)
    df["is_q4"]         = pd.to_numeric(df["is_q4"],         errors="coerce").fillna(0).astype(int)
    return df


def run_regression(df):
    # OLS regression using NumPy matrix operations, no statsmodels or SciPy
    # model: daily_revenue ~ 1 + is_q4 + daily_orders
    df = df.dropna(subset=["daily_revenue", "is_q4", "daily_orders"]).copy()

    y = df["daily_revenue"].astype(float).to_numpy().reshape(-1, 1)
    X = np.hstack([
        np.ones((len(df), 1)),
        df[["is_q4"]].astype(float).to_numpy(),
        df[["daily_orders"]].astype(float).to_numpy(),
    ])

    n, k = X.shape
    if n <= k:
        raise ValueError(f"Not enough observations: n={n}, k={k}")

    XtX     = X.T @ X
    XtX_inv = np.linalg.inv(XtX)
    beta    = XtX_inv @ (X.T @ y)
    y_hat   = X @ beta
    resid   = y - y_hat

    df_resid = n - k
    sse      = float((resid.T @ resid)[0, 0])
    y_mean   = float(y.mean())
    sst      = float(((y - y_mean).T @ (y - y_mean))[0, 0])
    r2       = 1.0 - (sse / sst) if sst > 0 else float("nan")
    adj_r2   = 1.0 - (1.0 - r2) * (n - 1) / df_resid if np.isfinite(r2) else float("nan")

    sigma2   = sse / df_resid
    se_beta  = np.sqrt(np.diag(sigma2 * XtX_inv)).reshape(-1, 1)
    t_stats  = beta / se_beta

    def normal_cdf(z):
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

    def pval(t):
        return 2.0 * (1.0 - normal_cdf(abs(float(t)))) if math.isfinite(float(t)) else float("nan")

    p_vals = np.array([pval(t) for t in t_stats.flatten()]).reshape(-1, 1)
    terms  = ["const", "is_q4", "daily_orders"]

    print("\nQ4 Regression (Redshift data | NumPy OLS)")
    print("Model: daily_revenue ~ is_q4 + daily_orders")
    print(f"Observations: {n}")
    print(f"R-squared: {r2:.4f}")
    print(f"Adj. R-squared: {adj_r2:.4f}")
    print(f"\n{'term':<14}{'coef':>14}{'std err':>14}{'t':>12}{'p>|t|':>12}")
    for i, term in enumerate(terms):
        print(f"{term:<14}{float(beta[i,0]):>14,.4f}{float(se_beta[i,0]):>14,.4f}"
              f"{float(t_stats[i,0]):>12.4f}{float(p_vals[i,0]):>12.6g}")


def upload_dataset_to_s3(df):
    # convert the regression dataset to CSV and upload to S3
    # this file can be downloaded and used for further analysis or visualization
    s3_client = boto3.client("s3")
    s3_key    = f"{S3_OUTPUTS_PREFIX}q4_regression_dataset_daily.csv"
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"Dataset saved to s3://{S3_BUCKET}/{s3_key}")


def main():
    df = fetch_daily_dataset()
    upload_dataset_to_s3(df)
    run_regression(df)


main()
job.commit()
