# Redshift cluster that replaces the local PostgreSQL database
# dc2.large with a single node is sufficient for this dataset and keeps costs low
# the cluster runs inside a default VPC for simplicity
# for production use, a dedicated VPC with private subnets is recommended

# security group that allows inbound Redshift traffic on port 5439
# restrict the CIDR to your IP address for security; 0.0.0.0/0 is open to all
resource "aws_security_group" "redshift_sg" {
  name        = "${var.project_name}-redshift-sg"
  description = "Allow inbound Redshift access"

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# IAM role that allows Redshift to run COPY commands reading from S3
# this is required for the 03_load_raw_online_retail.sql COPY statements
resource "aws_iam_role" "redshift_s3_role" {
  name = "${var.project_name}-redshift-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_policy" {
  role       = aws_iam_role.redshift_s3_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# the Redshift cluster itself
resource "aws_redshift_cluster" "main" {
  cluster_identifier        = var.redshift_cluster_identifier
  database_name             = var.redshift_database_name
  master_username           = var.redshift_master_username
  master_password           = var.redshift_master_password
  node_type                 = var.redshift_node_type
  number_of_nodes           = var.redshift_number_of_nodes
  cluster_type              = "single-node"
  skip_final_snapshot       = true
  publicly_accessible       = true
  vpc_security_group_ids    = [aws_security_group.redshift_sg.id]
  iam_roles                 = [aws_iam_role.redshift_s3_role.arn]

  tags = {
    Project = var.project_name
  }
}

# output the JDBC connection URL so it can be referenced by Glue job parameters
output "redshift_jdbc_url" {
  description = "JDBC URL for connecting Glue jobs to Redshift"
  value       = "jdbc:redshift://${aws_redshift_cluster.main.endpoint}/${var.redshift_database_name}"
  sensitive   = true
}
