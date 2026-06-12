# Step Functions state machine that orchestrates the full pipeline
# this replaces the local Apache Airflow DAG
# the state machine definition is loaded from the JSON file in step_functions_AWS/
# the SNS topic ARN is injected at deploy time using templatefile()
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project_name}-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  # load the state machine definition from the JSON file
  # replace the sns_topic_arn placeholder with the actual ARN at deploy time
  definition = templatefile(
    "${path.module}/../step_functions_AWS/pipeline_state_machine.json",
    {
      sns_topic_arn = aws_sns_topic.pipeline_alerts.arn
    }
  )

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.step_functions_logs.arn}:*"
  }

  tags = {
    Project = var.project_name
  }
}
