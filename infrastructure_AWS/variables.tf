# all configurable values for the pipeline infrastructure are defined here
# change these variables to match your AWS account, region, and naming preferences
# sensitive values like passwords should be passed via environment variables or a secrets manager
# and never hardcoded in this file

variable "aws_region" {
  description = "AWS region where all resources will be created"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used as a prefix for all resource names to keep them identifiable"
  type        = string
  default     = "online-retail"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket that stores raw CSV files and all pipeline outputs"
  type        = string
  default     = "online-retail-data-pipeline"
}

variable "redshift_cluster_identifier" {
  description = "Unique identifier for the Redshift cluster"
  type        = string
  default     = "online-retail-cluster"
}

variable "redshift_database_name" {
  description = "Name of the database created inside the Redshift cluster"
  type        = string
  default     = "onlineretail"
}

variable "redshift_master_username" {
  description = "Master username for the Redshift cluster"
  type        = string
  default     = "adminuser"
}

variable "redshift_master_password" {
  description = "Master password for the Redshift cluster. Pass this via TF_VAR_redshift_master_password environment variable and never commit the value to Git."
  type        = string
  sensitive   = true
}

variable "redshift_node_type" {
  description = "Redshift node type. dc2.large is the smallest available and sufficient for a portfolio dataset."
  type        = string
  default     = "dc2.large"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes. 1 is sufficient for development and portfolio use."
  type        = number
  default     = 1
}

variable "glue_python_version" {
  description = "Python version used by all Glue jobs"
  type        = string
  default     = "3"
}

variable "glue_worker_type" {
  description = "Glue worker type. G.1X is the smallest available and sufficient for this dataset."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers. 2 is the minimum and sufficient for development use."
  type        = number
  default     = 2
}

variable "alert_email" {
  description = "Email address that receives SNS failure notifications when a pipeline step fails"
  type        = string
  default     = "ariah2@uic.edu"
}

variable "pipeline_schedule" {
  description = "EventBridge cron expression for the pipeline schedule. Default runs daily at 6am UTC."
  type        = string
  default     = "cron(0 6 * * ? *)"
}
