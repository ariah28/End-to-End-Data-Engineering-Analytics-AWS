# End-to-End Data Engineering and Analytics Pipeline (AWS)

Online Retail II

Technologies: Amazon S3, AWS Glue (PySpark), Amazon Redshift, AWS Step Functions, Amazon SNS, Amazon CloudWatch, Terraform

---

## Project Overview

This is the AWS version of the End-to-End Data Engineering and Analytics Pipeline. It implements the same pipeline logic and star schema, and produces the same analytical results as the local version. The difference is that every component now runs on AWS-managed services rather than a local machine.

The pipeline ingests raw retail transaction data, applies data profiling and validation, synchronizes data incrementally from raw to staging, builds an analytics-ready star schema, and executes automated analytics workflows including statistical testing, regression modeling, visualization generation, and BI dataset export.

AWS Step Functions orchestrate all steps. Failures trigger an email alert via Amazon SNS. All logs are captured in Amazon CloudWatch.

This project is designed to run on AWS and contains production-ready code structured for deployment. The pipeline scripts, SQL, Terraform, and Step Functions configuration are written to be deployed as-is. Still, they will require environment-specific values before running — specifically the S3 bucket name, Redshift cluster endpoint, IAM role ARNs, and AWS account ID. These values are documented in .env, example, and the placeholder comments inside warehouse_sql_AWS/03_load_raw_online_retail.sql and infrastructure_AWS/variables.tf. No logic changes are needed — only configuration values tied to your AWS account.

---

## AWS Architecture

```
S3 (raw CSV files)
    |
    v
AWS Glue (PySpark ETL jobs)
    |
    v
Amazon Redshift (raw schema)
    |
    v
AWS Glue (data profiling and validation)
    |
    v
AWS Glue (raw to staging sync)
    |
    v
Amazon Redshift (staging schema)
    |
    v
AWS Glue (analytics model build)
    |
    v
Amazon Redshift (analytics star schema)
    |
    v
AWS Glue (statistics, regression, visuals, Tableau export)
    |
    v
S3 (outputs: figures, modeling results, tableau CSV)

Step Functions orchestrates every step above.
SNS sends an email on any failure.
CloudWatch captures all logs.
```

---

## Technology Stack

Storage: Amazon S3

Data Processing: AWS Glue with PySpark

Data Warehouse: Amazon Redshift

Orchestration: AWS Step Functions

Alerting: Amazon SNS

Monitoring and Logging: Amazon CloudWatch

Infrastructure as Code: Terraform

---

## Project Structure

```
End-to-End-Data-Engineering-Analytics_AWS/
|
|-- pipeline_scripts_AWS/
|   |-- 06_raw_to_staging_sync.py
|   |-- 09_statistics_from_csv_q4_test.py
|   |-- 10_q4_regression_with_order_control.py
|   |-- 11_export_tableau_daily_kpis.py
|   +-- generate_visuals.py
|
|-- warehouse_sql_AWS/
|   |-- 01_create_schemas.sql
|   |-- 02_create_raw_online_retail_table.sql
|   |-- 03_load_raw_online_retail.sql
|   |-- 04_raw_data_profiling_checks.sql
|   |-- 05_staging_validation_checks.sql
|   |-- 07_build_analytics_model.sql
|   +-- 08_analytics_business_queries.sql
|
|-- step_functions_AWS/
|   +-- pipeline_state_machine.json
|
|-- infrastructure_AWS/
|   |-- main.tf
|   |-- variables.tf
|   |-- s3.tf
|   |-- redshift.tf
|   |-- glue.tf
|   |-- step_functions.tf
|   |-- sns.tf
|   +-- cloudwatch.tf
|
|-- analytics_AWS/
|   +-- outputs/
|       |-- figures/
|       |-- modeling/
|       +-- tableau/
|
|-- documentation_AWS/
|   +-- screenshots/
|
|-- MIGRATION_GUIDE.md
|-- requirements.txt
|-- .env.example
|-- README.md
+-- .gitignore
```

---

## Data Source

The pipeline uses the Online Retail II dataset, split into two yearly files, both stored in the S3 raw prefix:

```
s3://your-bucket/raw/Year_2009-2010_online_retail_II.csv
s3://your-bucket/raw/Year_2010-2011_online_retail_II.csv
```

To run the pipeline with new data, upload an additional file to the raw prefix using the same column schema. The incremental sync step will automatically detect and load only the new rows.

---

## Analytics Data Model

The pipeline builds a star schema optimized for reporting.

Fact table: analytics.fact_sales

Dimension tables:
- analytics.dim_date
- analytics.dim_country
- analytics.dim_product
- analytics.dim_customer

---

## Pipeline Steps

The Step Functions state machine runs these steps in sequence:

1. Create schemas in Redshift (raw, staging, analytics)
2. Create the raw.online_retail table
3. Run raw data profiling checks
4. Sync raw CSV files from S3 into Redshift and rebuild the staging
5. Run staging validation checks
6. Build the analytics star schema from staging
7. Run business analytics queries
8. Generate Python visualizations saved to S3
9. Run Q4 Welch t-tests and save the report to S3
10. Run Q4 OLS regression and save the dataset to S3

Each step retries up to 4 times with a 3-minute interval before triggering an SNS failure alert.

---

## Pipeline Outputs

All outputs are written to S3 by the Glue jobs:

Visualizations: s3://your-bucket/outputs/figures/latest/ and versioned/

Statistical report: s3://your-bucket/outputs/modeling/q4_stats_tests_report.txt

Regression dataset: s3://your-bucket/outputs/modeling/q4_regression_dataset_daily.csv

Tableau export: s3://your-bucket/outputs/tableau/tableau_daily_kpis.csv

---

## Failure Alerts and Monitoring

When any Step Functions step fails, an SNS email is sent with the step name and error message. The full stack trace is available in CloudWatch under the log group for the relevant Glue job.

CloudWatch log groups:
- /aws/states/online-retail-pipeline (Step Functions)
- /aws/glue/jobs/online-retail-06-raw-to-staging-sync
- /aws/glue/jobs/online-retail-09-generate-visuals
- /aws/glue/jobs/online-retail-10-q4-statistics
- /aws/glue/jobs/online-retail-11-q4-regression
- /aws/glue/jobs/online-retail-11-tableau-export

---

## Deployment

Prerequisites: AWS CLI configured, Terraform 1.5 or higher installed.

1. Copy .env.example to .env and fill in your values.
2. Upload both CSV files to s3://your-bucket/raw/.
3. Upload all scripts from pipeline_scripts_AWS/ to s3://your-bucket/scripts/.
4. Navigate to infrastructure_AWS/ and run:

```
terraform init
terraform plan
terraform apply
```

5. Confirm the SNS subscription email before the first pipeline run.
6. Trigger a manual run from the Step Functions console or wait for the scheduled trigger.

To tear down all infrastructure and stop AWS charges:

```
terraform destroy
```

---

## Screenshots

Screenshots of the deployed pipeline (Step Functions execution, Glue job runs, Redshift query results, CloudWatch logs, and SNS failure email) will be added after the first successful AWS deployment. For reference, the local version of this project includes equivalent screenshots from the Airflow, PostgreSQL, and Tableau implementations.

---

## Key Insights

The same analytical results from the local pipeline apply here:

- Q4 revenue shows a statistically significant increase compared to non-Q4 periods
- Return behavior differs between seasonal and non-seasonal periods
- Daily order volume explains a large portion of revenue variation
- After controlling for order volume, Q4 still contributes additional revenue lift
- A small number of countries and products drive a disproportionate share of total revenue

---

## Related Project

The local version of this pipeline (PostgreSQL, Apache Airflow, local Python) is available in the companion repository: End-to-End-Data-Engineering-Analytics. Comparing both versions shows how the same pipeline logic translates from a local development environment to a production AWS architecture.
