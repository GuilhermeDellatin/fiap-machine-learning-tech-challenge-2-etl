# Publica scripts no mesmo bucket (glue/scripts/...)
resource "aws_s3_object" "glue_extract" {
  bucket       = var.bucket_name
  key          = "glue/scripts/extract.py"
  source       = "../glue/jobs/extract.py"
  content_type = "text/x-python"
  etag         = filemd5("../glue/jobs/extract.py")
}

resource "aws_s3_object" "glue_transform" {
  bucket       = var.bucket_name
  key          = "glue/scripts/transform.py"
  source       = "../glue/jobs/transform.py"
  content_type = "text/x-python"
  etag         = filemd5("../glue/jobs/transform.py")
}

resource "aws_s3_object" "glue_load" {
  bucket       = var.bucket_name
  key          = "glue/scripts/load.py"
  source       = "../glue/jobs/load.py"
  content_type = "text/x-python"
  etag         = filemd5("../glue/jobs/load.py")
}

# Glue Jobs (Glue 5.0)
resource "aws_glue_job" "extract" {
  name               = "B3Extract"
  role_arn           = aws_iam_role.glue_role.arn
  glue_version       = var.glue_version
  number_of_workers  = var.glue_number_workers
  worker_type        = var.glue_worker_type

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_name}/glue/scripts/extract.py"
  }

  default_arguments = {
    "--bucket_name"   = var.bucket_name
    "--raw_prefix"     = var.raw_prefix
    "--database"       = var.database
    "--table_raw"      = var.table_raw
    "--job-language"   = "python"
    "--enable-metrics" = "true"
  }
}

resource "aws_glue_job" "transform" {
  name               = "B3Transform"
  role_arn           = aws_iam_role.glue_role.arn
  glue_version       = var.glue_version
  number_of_workers  = var.glue_number_workers
  worker_type        = var.glue_worker_type

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_name}/glue/scripts/transform.py"
  }

  default_arguments = {
    "--bucket_name"     = var.bucket_name
    "--raw_prefix"       = var.raw_prefix
    "--refined_prefix"   = var.refined_prefix
    "--database"         = var.database
    "--table_raw"        = var.table_raw
    "--table_refined"    = var.table_refined
    "--job-language"     = "python"
    "--enable-metrics"   = "true"
  }
}

resource "aws_glue_job" "load" {
  name               = "B3Load"
  role_arn           = aws_iam_role.glue_role.arn
  glue_version       = var.glue_version
  number_of_workers  = var.glue_number_workers
  worker_type        = var.glue_worker_type

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_name}/glue/scripts/load.py"
  }

  default_arguments = {
    "--bucket_name"     = var.bucket_name
    "--refined_prefix"   = var.refined_prefix
    "--database"         = var.database
    "--table_refined"    = var.table_refined
    "--job-language"     = "python"
    "--enable-metrics"   = "true"
  }
}

# Step Functions (Extract → Transform → Load)
resource "aws_sfn_state_machine" "b3_sfn" {
  name     = "b3-etl-step-function"
  role_arn = aws_iam_role.sfn_role.arn

  definition = jsonencode({
    Comment = "B3 ETL",
    StartAt = "Extract",
    States = {
      Extract = {
        Type = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          JobName = aws_glue_job.extract.name,
          Arguments = {
            "--bucket_name"  = var.bucket_name,
            "--raw_prefix"    = var.raw_prefix,
            "--database"      = var.database,
            "--table_raw"     = var.table_raw,
            "--s3_key.$"      = "$.Records[0].s3.object.key"
          }
        },
        Next = "Transform"
      },
      Transform = {
        Type = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          JobName = aws_glue_job.transform.name,
          Arguments = {
            "--bucket_name"     = var.bucket_name,
            "--raw_prefix"       = var.raw_prefix,
            "--refined_prefix"   = var.refined_prefix,
            "--database"         = var.database,
            "--table_raw"        = var.table_raw,
            "--table_refined"    = var.table_refined,
            "--s3_key.$"         = "$.Records[0].s3.object.key"
          }
        },
        Next = "Load"
      },
      Load = {
        Type = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          JobName = aws_glue_job.load.name,
          Arguments = {
            "--bucket_name"     = var.bucket_name,
            "--refined_prefix"   = var.refined_prefix,
            "--database"         = var.database,
            "--table_refined"    = var.table_refined,
            "--s3_key.$"         = "$.Records[0].s3.object.key"
          }
        },
        End = true
      }
    }
  })
}