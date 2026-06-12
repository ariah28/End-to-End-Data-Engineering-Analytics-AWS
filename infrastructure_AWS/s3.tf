# S3 bucket that serves as the central storage for the entire pipeline
# raw CSV files are uploaded here before the pipeline runs
# all pipeline outputs (figures, modeling results, tableau export) are written here by Glue jobs
resource "aws_s3_bucket" "pipeline_bucket" {
  bucket        = var.s3_bucket_name
  force_destroy = true

  tags = {
    Project = var.project_name
  }
}

# block all public access to the bucket
# data in this bucket is only accessed by Glue, Redshift, and authorized IAM roles
resource "aws_s3_bucket_public_access_block" "pipeline_bucket_acl" {
  bucket                  = aws_s3_bucket.pipeline_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# enable versioning so accidental overwrites of raw CSV files can be recovered
resource "aws_s3_bucket_versioning" "pipeline_bucket_versioning" {
  bucket = aws_s3_bucket.pipeline_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# create the folder structure inside the bucket using empty objects as placeholders
# S3 does not have real folders but these objects make the structure visible in the console
resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.pipeline_bucket.id
  key     = "raw/"
  content = ""
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.pipeline_bucket.id
  key     = "scripts/"
  content = ""
}

resource "aws_s3_object" "outputs_figures_prefix" {
  bucket  = aws_s3_bucket.pipeline_bucket.id
  key     = "outputs/figures/"
  content = ""
}

resource "aws_s3_object" "outputs_modeling_prefix" {
  bucket  = aws_s3_bucket.pipeline_bucket.id
  key     = "outputs/modeling/"
  content = ""
}

resource "aws_s3_object" "outputs_tableau_prefix" {
  bucket  = aws_s3_bucket.pipeline_bucket.id
  key     = "outputs/tableau/"
  content = ""
}

resource "aws_s3_object" "temp_prefix" {
  bucket  = aws_s3_bucket.pipeline_bucket.id
  key     = "tmp/"
  content = ""
}
