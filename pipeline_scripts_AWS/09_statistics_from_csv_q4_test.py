import sys
import os
import math
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
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
S3_BUCKET        = os.environ["S3_BUCKET"]
S3_OUTPUTS_PREFIX = os.environ.get("S3_OUTPUTS_PREFIX", "outputs/modeling/")
REDSHIFT_URL     = os.environ["REDSHIFT_JDBC_URL"]
REDSHIFT_USER    = os.environ["REDSHIFT_USER"]
REDSHIFT_PASS    = os.environ["REDSHIFT_PASSWORD"]

ALPHA      = 0.05
CONF_LEVEL = 0.95


def read_redshift_query(sql):
    # run a SQL query against Redshift and return the result as a Spark DataFrame
    # the query is wrapped in a subquery alias so JDBC can read it as a table
    return spark.read \
        .format("jdbc") \
        .option("url",      REDSHIFT_URL) \
        .option("dbtable",  f"({sql}) tmp") \
        .option("user",     REDSHIFT_USER) \
        .option("password", REDSHIFT_PASS) \
        .option("driver",   "com.amazon.redshift.jdbc42.Driver") \
        .load()


def normal_cdf(z):
    # standard normal CDF using erf, no SciPy dependency
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def z_critical(conf_level):
    # critical z value for common confidence levels
    common = {0.90: 1.6449, 0.95: 1.9600, 0.99: 2.5758}
    return common.get(conf_level, 1.9600)


def welch_df(x, y):
    # Welch-Satterthwaite effective degrees of freedom
    nx, ny = x.size, y.size
    vx, vy = x.var(ddof=1), y.var(ddof=1)
    num = (vx / nx + vy / ny) ** 2
    den = (vx**2) / (nx**2 * (nx - 1)) + (vy**2) / (ny**2 * (ny - 1))
    return float(num / den)


def diff_ci(x, y, conf_level=0.95):
    # confidence interval for the mean difference using normal approximation
    nx, ny = x.size, y.size
    mx, my = x.mean(), y.mean()
    vx, vy = x.var(ddof=1), y.var(ddof=1)
    diff = mx - my
    se = math.sqrt(vx / nx + vy / ny)
    zcrit = z_critical(conf_level)
    return float(diff - zcrit * se), float(diff + zcrit * se)


def split_q4_nonq4(dates, values):
    # split a series into Q4 (Oct, Nov, Dec) and non-Q4 groups
    dts = pd.to_datetime(dates)
    months = dts.dt.month
    q4     = values[months.isin([10, 11, 12])].to_numpy(dtype=float)
    non_q4 = values[~months.isin([10, 11, 12])].to_numpy(dtype=float)
    return q4, non_q4


def welch_test_block(name, q4, non_q4, decimals=2):
    # run a Welch t-test between the Q4 and non-Q4 groups
    # uses normal approximation for p-values, no SciPy required
    if q4.size < 2 or non_q4.size < 2:
        raise ValueError(f"{name}: need at least 2 observations per group")

    n1, n2 = q4.size, non_q4.size
    m1, m2 = float(q4.mean()), float(non_q4.mean())
    v1, v2 = float(q4.var(ddof=1)), float(non_q4.var(ddof=1))

    diff   = m1 - m2
    se     = math.sqrt(v1 / n1 + v2 / n2)
    t_stat = diff / se if se > 0 else float("nan")
    p_val  = 2.0 * (1.0 - normal_cdf(abs(t_stat))) if math.isfinite(t_stat) else float("nan")
    df_eff = welch_df(q4, non_q4)
    ci_lo, ci_hi = diff_ci(q4, non_q4, CONF_LEVEL)

    fmt = f"{{:,.{decimals}f}}"
    lines = [
        f"=== {name} ===",
        f"n_q4={n1} | n_non_q4={n2}",
        f"mean_q4: {fmt.format(m1)}",
        f"mean_non_q4: {fmt.format(m2)}",
        f"diff (q4 - non_q4): {fmt.format(diff)}",
        f"t_stat (Welch): {t_stat:.4f}",
        f"p_value (Normal approx): {p_val:.6g}",
        f"significant (alpha={ALPHA}): {bool(p_val < ALPHA) if math.isfinite(p_val) else 'NA'}",
        f"df_eff (Welch-Satterthwaite): {df_eff:.2f}",
        f"{int(CONF_LEVEL*100)}% CI (diff, Normal approx): [{fmt.format(ci_lo)}, {fmt.format(ci_hi)}]",
    ]
    return "\n".join(lines)


def fetch_daily_revenue():
    # query daily revenue from the analytics star schema in Redshift
    sql = """
        SELECT
            d.full_date AS order_date,
            SUM(f.total_price) AS revenue
        FROM analytics.fact_sales f
        JOIN analytics.dim_date d ON d.date_key = f.date_key
        GROUP BY d.full_date
        ORDER BY d.full_date
    """
    df_spark = read_redshift_query(sql)
    df = df_spark.toPandas()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["revenue"]    = pd.to_numeric(df["revenue"], errors="coerce").fillna(0.0)
    return df


def fetch_daily_return_rate():
    # query daily return rate from the analytics star schema in Redshift
    # return rate is calculated as returned revenue divided by gross revenue per day
    sql = """
        WITH daily AS (
            SELECT
                d.full_date AS order_date,
                SUM(CASE WHEN f.quantity > 0 THEN f.total_price ELSE 0 END) AS gross_revenue,
                ABS(SUM(CASE WHEN f.quantity < 0 THEN f.total_price ELSE 0 END)) AS returned_revenue
            FROM analytics.fact_sales f
            JOIN analytics.dim_date d ON d.date_key = f.date_key
            GROUP BY d.full_date
        )
        SELECT
            order_date,
            CASE WHEN gross_revenue > 0 THEN (returned_revenue / gross_revenue) ELSE NULL END AS return_rate
        FROM daily
        WHERE gross_revenue > 0
        ORDER BY order_date
    """
    df_spark = read_redshift_query(sql)
    df = df_spark.toPandas()
    df["order_date"]  = pd.to_datetime(df["order_date"])
    df["return_rate"] = pd.to_numeric(df["return_rate"], errors="coerce")
    df = df.dropna(subset=["return_rate"]).copy()
    return df


def upload_report_to_s3(report_text):
    # write the stats report text to S3 so it persists outside the Glue job
    s3_client = boto3.client("s3")
    s3_key    = f"{S3_OUTPUTS_PREFIX}q4_stats_tests_report.txt"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=report_text.encode("utf-8"),
        ContentType="text/plain",
    )
    print(f"Report saved to s3://{S3_BUCKET}/{s3_key}")


def main():
    daily_rev = fetch_daily_revenue()
    q4_rev, non_q4_rev = split_q4_nonq4(daily_rev["order_date"], daily_rev["revenue"])

    daily_rr = fetch_daily_return_rate()
    q4_rr, non_q4_rr = split_q4_nonq4(daily_rr["order_date"], daily_rr["return_rate"])

    block1 = welch_test_block("TEST 1: Daily Revenue (Q4 vs Non-Q4)", q4_rev, non_q4_rev, decimals=2)
    block2 = welch_test_block("TEST 2: Daily Return Rate (Q4 vs Non-Q4)", q4_rr, non_q4_rr, decimals=4)

    report = "\n\n".join([block1, block2]) + "\n"
    print("\n" + report)
    upload_report_to_s3(report)


main()
job.commit()
