# AWS provider configuration
# all resources in this project are created in the region defined in variables.tf
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# retrieve the current AWS account ID
# used when constructing IAM role ARNs and resource policies
data "aws_caller_identity" "current" {}

# IAM role that allows Glue jobs to access S3 and Redshift
# this role is assumed by all Glue jobs when they run
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# attach the AWS-managed Glue service policy to the Glue role
# this grants the permissions Glue needs to read from S3, write logs to CloudWatch, and access Secrets Manager
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# grant the Glue role full access to the pipeline S3 bucket
# Glue reads CSV files from the raw prefix and writes outputs to the outputs prefix
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.pipeline_bucket.arn,
        "${aws_s3_bucket.pipeline_bucket.arn}/*"
      ]
    }]
  })
}

# IAM role that allows Step Functions to start Glue jobs and publish to SNS
resource "aws_iam_role" "step_functions_role" {
  name = "${var.project_name}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# grant Step Functions permission to start and monitor Glue job runs
resource "aws_iam_role_policy" "sfn_glue_policy" {
  name = "${var.project_name}-sfn-glue-policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.pipeline_alerts.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogDelivery", "logs:PutLogEvents", "logs:GetLogDelivery"]
        Resource = "*"
      }
    ]
  })
}

# IAM role that allows EventBridge to start the Step Functions state machine on a schedule
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_policy" {
  name = "${var.project_name}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.pipeline.arn
    }]
  })
}

# EventBridge Scheduler rule that triggers the pipeline on the configured schedule
# default is daily at 6am UTC as defined in variables.tf
resource "aws_scheduler_schedule" "pipeline_schedule" {
  name       = "${var.project_name}-daily-run"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = var.pipeline_schedule

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.eventbridge_role.arn
  }
}
