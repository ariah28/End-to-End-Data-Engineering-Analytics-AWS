# SNS topic that receives failure notifications from the Step Functions state machine
# when any pipeline step fails, Step Functions publishes a message to this topic
# the message includes the step name and error details
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-pipeline-alerts"

  tags = {
    Project = var.project_name
  }
}

# email subscription to the SNS topic
# AWS will send a confirmation email to this address when the infrastructure is first deployed
# the subscription becomes active only after the confirmation link is clicked
resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# output the SNS topic ARN so it can be referenced in other configurations
output "sns_topic_arn" {
  description = "ARN of the SNS topic used for pipeline failure alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}
