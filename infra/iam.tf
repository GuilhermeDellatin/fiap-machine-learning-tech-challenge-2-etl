# ------------ Assumes ------------
data "aws_iam_policy_document" "assume_glue" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service" identifiers = ["glue.amazonaws.com"] }
  }
}

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service" identifiers = ["lambda.amazonaws.com"] }
  }
}


data "aws_iam_policy_document" "assume_sfn" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service" identifiers = ["states.amazonaws.com"] }
  }
}


# ------------ Policies (inline p/ simplicidade) ------------
resource "aws_iam_role" "glue_role" {
  name = "b3-glue-exec-role"
  assume_role_policy = data.aws_iam_policy_document.assume_glue.json
}

resource "aws_iam_role" "lambda_role" {
  name = "b3-lambda-startstep-role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

resource "aws_iam_role" "sfn_role" {
  name = "b3-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.assume_sfn.json
}

# ------------ Policies (inline p/ simplicidade) ------------
resource "aws_iam_role_policy" "glue_inline" {
  name = "b3-glue-inline"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"], Resource = "*" },
      { Effect = "Allow", Action = [
        "s3:GetObject","s3:PutObject","s3:ListBucket","s3:DeleteObject","s3:GetBucketLocation"
      ], Resource = [
        "arn:aws:s3:::${var.bucket_name}",
        "arn:aws:s3:::${var.bucket_name}/*"
      ] },
      { Effect = "Allow", Action = [
        "glue:GetDatabase","glue:CreateDatabase","glue:GetTable","glue:CreateTable",
        "glue:UpdateTable","glue:BatchCreatePartition","glue:GetPartitions"
      ], Resource = "*" }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_inline" {
  name = "b3-lambda-inline"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"], Resource = "*" },
      { Effect = "Allow", Action = ["states:StartExecution"], Resource = "*" } # pode restringir ao ARN da SFN após criação
    ]
  })
}

resource "aws_iam_role_policy" "sfn_inline" {
  name = "b3-sfn-inline"
  role = aws_iam_role.sfn_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", Action = [
        "logs:CreateLogDelivery","logs:CreateLogStream","logs:PutLogEvents","logs:DescribeLogGroups"
      ], Resource = "*" },
      { Effect = "Allow", Action = ["glue:StartJobRun"], Resource = "*" }
    ]
  })
}
