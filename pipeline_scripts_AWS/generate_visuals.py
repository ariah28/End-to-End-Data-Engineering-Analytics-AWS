import sys
import os
import io
import json
import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

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
S3_BUCKET           = os.environ["S3_BUCKET"]
S3_FIGURES_PREFIX   = os.environ.get("S3_FIGURES_PREFIX", "outputs/figures/")
REDSHIFT_URL        = os.environ["REDSHIFT_JDBC_URL"]
REDSHIFT_USER       = os.environ["REDSHIFT_USER"]
REDSHIFT_PASS       = os.environ["REDSHIFT_PASSWORD"]

s3_client = boto3.client("s3")
STATE_KEY = f"{S3_FIGURES_PREFIX}_visuals_state.json"


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


def qdf(sql):
    # convenience wrapper to run a query and return a pandas DataFrame
    return read_redshift_query(sql).toPandas()


def compute_data_signature():
    # compute a lightweight fingerprint of the current analytics data
    # used to detect whether new data has been loaded since the last run
    sql = """
        SELECT MAX(d.full_date) AS max_date, COUNT(*) AS n_rows
        FROM analytics.fact_sales f
        JOIN analytics.dim_date d ON d.date_key = f.date_key
    """
    row = qdf(sql).iloc[0]
    return {"max_date": str(row["max_date"]), "n_rows": int(row["n_rows"])}


def load_state():
    # read the last known data signature from S3
    # if the file does not exist this is the first run
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=STATE_KEY)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        return {}
    except Exception:
        return {}


def save_state(signature, version):
    # write the current data signature and version number to S3
    # this file is checked at the start of every run to skip unchanged data
    from datetime import datetime
    state = {
        "data_signature": signature,
        "version": version,
        "last_run_utc": datetime.utcnow().isoformat(),
    }
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=STATE_KEY,
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def save_figure_to_s3(fig_name, version):
    # save the current matplotlib figure to both a versioned path and a latest path in S3
    # versioned path retains history of every run
    # latest path always points to the most recent output
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    img_bytes = buf.read()

    versioned_key = f"{S3_FIGURES_PREFIX}versioned/{fig_name}_v{version}.png"
    latest_key    = f"{S3_FIGURES_PREFIX}latest/{fig_name}_latest.png"

    for key in [versioned_key, latest_key]:
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=img_bytes, ContentType="image/png")
        print(f"Saved: s3://{S3_BUCKET}/{key}")


def plot_daily_revenue(version):
    sql = """
        SELECT d.full_date AS date, ROUND(SUM(f.total_price), 2) AS revenue
        FROM analytics.fact_sales f
        JOIN analytics.dim_date d ON d.date_key = f.date_key
        GROUP BY d.full_date ORDER BY d.full_date
    """
    df = qdf(sql)
    plt.figure(figsize=(14, 6))
    plt.plot(df["date"], df["revenue"], linewidth=1)
    plt.title("Daily Revenue Over Time", fontsize=14)
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    save_figure_to_s3("daily_revenue_over_time", version)
    plt.close()


def plot_monthly_revenue(version):
    sql = """
        SELECT DATE_TRUNC('month', d.full_date)::date AS month, ROUND(SUM(f.total_price), 2) AS revenue
        FROM analytics.fact_sales f
        JOIN analytics.dim_date d ON d.date_key = f.date_key
        GROUP BY 1 ORDER BY 1
    """
    df = qdf(sql)
    plt.figure(figsize=(12, 5))
    plt.plot(df["month"], df["revenue"], linewidth=2)
    plt.title("Monthly Revenue Over Time", fontsize=14)
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    save_figure_to_s3("monthly_revenue_over_time", version)
    plt.close()


def plot_top_countries(version):
    sql = """
        SELECT c.country, ROUND(SUM(f.total_price), 2) AS revenue
        FROM analytics.fact_sales f
        JOIN analytics.dim_country c ON c.country_id = f.country_id
        GROUP BY c.country ORDER BY revenue DESC LIMIT 10
    """
    df = qdf(sql)
    colors = ["#4CAF50","#C44E52","#4C72B0","#DD8452","#55A868","#8172B3","#937860","#DA8BC3","#8C8C8C","#CCB974"]
    plt.figure(figsize=(10, 6))
    plt.barh(df["country"][::-1], df["revenue"][::-1], color=colors)
    for i, v in enumerate(df["revenue"][::-1]):
        plt.text(v, i, f"${v:,.0f}", va="center", ha="left", fontsize=10)
    plt.title("Top 10 Countries by Revenue", fontsize=14)
    plt.xlabel("Revenue")
    plt.ylabel("Country")
    plt.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    save_figure_to_s3("top_countries_by_revenue", version)
    plt.close()


def plot_top_products(version):
    sql = """
        SELECT p.stockcode, COALESCE(NULLIF(TRIM(p.product_name), ''), '[No Description]') AS product_name,
               ROUND(SUM(f.total_price), 2) AS revenue
        FROM analytics.fact_sales f
        JOIN analytics.dim_product p ON p.stockcode = f.stockcode
        GROUP BY p.stockcode, p.product_name ORDER BY revenue DESC LIMIT 10
    """
    df = qdf(sql)
    df["label"] = df["stockcode"].astype(str) + " - " + df["product_name"].astype(str)
    colors = ["#4CAF50","#C44E52","#4C72B0","#DD8452","#55A868","#8172B3","#937860","#DA8BC3","#8C8C8C","#CCB974"]
    plt.figure(figsize=(12, 7))
    plt.barh(df["label"][::-1], df["revenue"][::-1], color=colors)
    for i, v in enumerate(df["revenue"][::-1]):
        plt.text(v, i, f"${v:,.0f}", va="center", ha="left", fontsize=9)
    plt.title("Top 10 Products by Revenue", fontsize=14)
    plt.xlabel("Revenue")
    plt.ylabel("Product (Stockcode - Name)")
    plt.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    save_figure_to_s3("top_products_by_revenue", version)
    plt.close()


def plot_revenue_breakdown(version):
    sql = """
        SELECT
            ROUND(SUM(CASE WHEN quantity > 0 THEN total_price ELSE 0 END), 2) AS gross_sales,
            ROUND(SUM(CASE WHEN quantity < 0 THEN total_price ELSE 0 END), 2) AS returns,
            ROUND(SUM(total_price), 2) AS net_revenue
        FROM analytics.fact_sales
    """
    df = qdf(sql)
    df_plot = pd.DataFrame({
        "type":   ["Gross Sales", "Returns", "Net Revenue"],
        "amount": [df.loc[0,"gross_sales"], df.loc[0,"returns"], df.loc[0,"net_revenue"]],
    })
    colors = ["#4CAF50", "#C44E52", "#4C72B0"]
    plt.figure(figsize=(8, 5))
    plt.bar(df_plot["type"], df_plot["amount"], color=colors)
    for i, v in enumerate(df_plot["amount"]):
        plt.text(i, v, f"${v:,.0f}", ha="center", va="bottom", fontsize=10)
    plt.title("Revenue Breakdown: Gross Sales vs Returns vs Net Revenue", fontsize=14)
    plt.ylabel("Revenue")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    save_figure_to_s3("revenue_breakdown_gross_returns_net", version)
    plt.close()


def main():
    new_sig = compute_data_signature()
    state   = load_state()

    if state.get("data_signature") == new_sig:
        # data has not changed since the last run, skip visual generation
        print("No new data detected. Skipping visual generation.")
        return

    version = int(state.get("version", 0)) + 1
    print(f"New data detected. Generating visuals version {version}")

    plot_daily_revenue(version)
    plot_monthly_revenue(version)
    plot_top_countries(version)
    plot_top_products(version)
    plot_revenue_breakdown(version)

    save_state(new_sig, version)
    print("State saved. Visual generation complete.")


main()
job.commit()
