import json
import os
import boto3

def lambda_handler(event, context):
    sfn = boto3.client('stepfunctions')
    stepfunctions = os.environ['STEPFUNCTIONS']
    sfn.start_execution(
        stateMachineArn=stepfunctions,
        input=json.dumps(event)
    )
    return {'status': 'Step Function started'}