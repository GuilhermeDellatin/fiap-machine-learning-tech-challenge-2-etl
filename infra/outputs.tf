output "lambda_arn" {
  value = module.lambda.lambda_arn
}

output "step_function_arn" {
  value = module.step_function.step_function_arn
}

output "glue_jobs" {
  value = {
    extract   = aws_glue_job.extract.name
    transform = aws_glue_job.transform.name
    load      = aws_glue_job.load.name
  }
}