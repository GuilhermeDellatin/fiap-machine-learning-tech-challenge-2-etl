variable "aws_region" { type = string, default = "us-east-2" }
variable "bucket_name" { type = string, default = "postech-ml-fase2-us-east-2" }
variable "raw_prefix" { type = string, default = "raw/" }
variable "refined_prefix"{ type = string, default = "refined/" }


variable "database" { type = string, default = "b3_db" }
variable "table_raw" { type = string, default = "b3_pregao_table_raw" }
variable "table_refined" { type = string, default = "b3_pregao_table_refined" }


# Caminho do pacote da Lambda (zip)
variable "lambda_zip_path" { type = string, default = "../lambda/build/start_step.zip" }


# Config Glue
variable "glue_version" { type = string, default = "5.0" }
variable "glue_worker_type" { type = string, default = "G.1X" }
variable "glue_number_workers" { type = number, default = 2 }