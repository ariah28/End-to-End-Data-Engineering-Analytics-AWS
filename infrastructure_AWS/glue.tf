# AWS Glue jobs that replace the local Python scripts
# each job corresponds to one step in the pipeline and one script in pipeline_scripts_AWS
# all jobs share the same IAM role, worker type, and environment variables
# the scripts are uploaded to S3 and referenced by their S3 path

# local values shared across all Glue job definitions
locals {
  glue_script_bucket  = aws_s3_bucket.pipeline_bucket.bucket
  glue_scripts_prefix = "scripts"
  glue_temp_dir       = "s3://${aws_s3_bucket.pipeline_bucket.bucket}/tmp/"

  # environment variables passed to every Glue job
  # these replace the local .env file used in the local pipeline
  common_glue_env = {
    "--S3_BUCKET"          = aws_s3_bucket.pipeline_bucket.bucket
    "--REDSHIFT_JDBC_URL"  = "jdbc:redshift://${aws_redshift_cluster.main.endpoint}/${var.redshift_database_name}"
    "--REDSHIFT_USER"      = var.redshift_master_username
    "--REDSHIFT_PASSWORD"  = var.redshift_master_password
    "--REDSHIFT_TMPDIR_S3" = local.glue_temp_dir
    "--S3_RAW_PREFIX"      = "raw/"
    "--S3_OUTPUTS_PREFIX"  = "outputs/modeling/"
    "--S3_FIGURES_PREFIX"  = "outputs/figures/"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--TempDir"            = local.glue_temp_dir
  }
}

# raw to staging sync job
# reads both CSV files from S3, identifies new rows, loads them into Redshift,
# and rebuilds the staging table with all data quality rules applied
resource "aws_glue_job" "raw_to_staging_sync" {
  name         = "${var.project_name}-06-raw-to-staging-sync"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_script_bucket}/${local.glue_scripts_prefix}/06_raw_to_staging_sync.py"
    python_version  = var.glue_python_version
  }

  default_arguments = local.common_glue_env

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  tags = { Project = var.project_name }
}

# Q4 statistics job
# runs Welch t-tests on daily revenue and return rate data from Redshift
# saves the test report to S3
resource "aws_glue_job" "q4_statistics" {
  name         = "${var.project_name}-10-q4-statistics"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_script_bucket}/${local.glue_scripts_prefix}/09_statistics_from_csv_q4_test.py"
    python_version  = var.glue_python_version
  }

  default_arguments = local.common_glue_env

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  tags = { Project = var.project_name }
}

# Q4 regression job
# runs OLS regression with daily_revenue as the dependent variable
# saves the dataset to S3 and prints regression coefficients
resource "aws_glue_job" "q4_regression" {
  name         = "${var.project_name}-11-q4-regression"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_script_bucket}/${local.glue_scripts_prefix}/10_q4_regression_with_order_control.py"
    python_version  = var.glue_python_version
  }

  default_arguments = local.common_glue_env

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  tags = { Project = var.project_name }
}

# Tableau export job
# queries the analytics star schema and exports the daily KPI dataset to S3 as CSV
resource "aws_glue_job" "tableau_export" {
  name         = "${var.project_name}-11-tableau-export"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_script_bucket}/${local.glue_scripts_prefix}/11_export_tableau_daily_kpis.py"
    python_version  = var.glue_python_version
  }

  default_arguments = merge(local.common_glue_env, {
    "--S3_OUTPUTS_PREFIX" = "outputs/tableau/"
  })

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  tags = { Project = var.project_name }
}

# visualization generation job
# detects whether data has changed since the last run
# generates all five plots and saves them to versioned and latest paths in S3
resource "aws_glue_job" "generate_visuals" {
  name         = "${var.project_name}-09-generate-visuals"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_script_bucket}/${local.glue_scripts_prefix}/generate_visuals.py"
    python_version  = var.glue_python_version
  }

  default_arguments = local.common_glue_env

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  tags = { Project = var.project_name }
}
