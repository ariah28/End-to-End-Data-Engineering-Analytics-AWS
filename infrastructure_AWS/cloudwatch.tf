# CloudWatch log groups for all pipeline components
# every Glue job and the Step Functions state machine writes logs here
# these logs are the primary debugging tool when a pipeline step fails
# retention is set to 30 days to avoid unnecessary storage costs

# log group for the Step Functions state machine
resource "aws_cloudwatch_log_group" "step_functions_logs" {
  name              = "/aws/states/${var.project_name}-pipeline"
  retention_in_days = 30

  tags = {
    Project = var.project_name
  }
}

# log group for the raw to staging sync Glue job
resource "aws_cloudwatch_log_group" "glue_raw_to_staging" {
  name              = "/aws/glue/jobs/${var.project_name}-06-raw-to-staging-sync"
  retention_in_days = 30

  tags = {
    Project = var.project_name
  }
}

# log group for the Q4 statistics Glue job
resource "aws_cloudwatch_log_group" "glue_q4_statistics" {
  name              = "/aws/glue/jobs/${var.project_name}-10-q4-statistics"
  retention_in_days = 30

  tags = {
    Project = var.project_name
  }
}

# log group for the Q4 regression Glue job
resource "aws_cloudwatch_log_group" "glue_q4_regression" {
  name              = "/aws/glue/jobs/${var.project_name}-11-q4-regression"
  retention_in_days = 30

  tags = {
    Project = var.project_name
  }
}

# log group for the Tableau export Glue job
resource "aws_cloudwatch_log_group" "glue_tableau_export" {
  name              = "/aws/glue/jobs/${var.project_name}-11-tableau-export"
  retention_in_days = 30

  tags = {
    Project = var.project_name
  }
}

# log group for the visualization generation Glue job
resource "aws_cloudwatch_log_group" "glue_generate_visuals" {
  name              = "/aws/glue/jobs/${var.project_name}-09-generate-visuals"
  retention_in_days = 30

  tags = {
    Project = var.project_name
  }
}

# CloudWatch metric alarm that fires when the Step Functions state machine enters a FAILED state
# this provides an additional layer of monitoring beyond the SNS email notification
resource "aws_cloudwatch_metric_alarm" "pipeline_failure_alarm" {
  alarm_name          = "${var.project_name}-pipeline-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Fires when the Online Retail pipeline Step Functions execution fails"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  tags = {
    Project = var.project_name
  }
}
