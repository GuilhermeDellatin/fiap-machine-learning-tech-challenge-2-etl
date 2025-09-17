# Notificação S3 → Lambda (aciona no prefixo raw/)
resource "aws_s3_bucket_notification" "raw_notify" {
  bucket = var.bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.start_step.arn
    events = ["s3:ObjectCreated:*"]
    filter_prefix = var.raw_prefix
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}