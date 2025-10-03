import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")

def lambda_handler(event, context):
    """
    Exemplo de event (opcional):
    {"arguments": {"--MY_ARG": "value"}}
    """
    if not GLUE_JOB_NAME:
        logger.error("Variável de ambiente GLUE_JOB_NAME não definida.")
        return {"statusCode": 500, "body": "GLUE_JOB_NAME not set"}

    # argumentos opcionais para o Glue job (prefixo -- para argumentos do Glue)
    job_args = event.get("arguments", None)

    try:
        if job_args:
            # Start job run com argumentos personalizados
            response = glue.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments=job_args
            )
        else:
            # Start job run padrão
            response = glue.start_job_run(JobName=GLUE_JOB_NAME)

        job_run_id = response.get("JobRunId")
        logger.info(f"Glue job started: {GLUE_JOB_NAME} / runId={job_run_id}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "GlueJobName": GLUE_JOB_NAME,
                "JobRunId": job_run_id
            })
        }

    except Exception as e:
        logger.exception("Erro ao disparar Glue Job")
        return {"statusCode": 500, "body": str(e)}
