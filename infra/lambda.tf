resource "aws_lambda_function" "start_step" {
  function_name = "b3_etl_s3_trigger"
  role = aws_iam_role.lambda_role.arn
  handler = "start_step.lambda_handler"
  runtime = "python3.13"
  filename = var.lambda_zip_path

  environment {
    variables = {
      STEP_FUNCTION_ARN = aws_sfn_state_machine.b3_sfn.arn
    }
  }
}


# Permite o S3 invocar a Lambda
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id = "AllowS3Invoke"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.start_step.function_name
  principal = "s3.amazonaws.com"
  source_arn = "arn:aws:s3:::${var.bucket_name}"
}