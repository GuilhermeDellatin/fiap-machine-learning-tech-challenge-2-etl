import sys
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
import logging

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
bucket_name = '861115334572-refined'
input_path = f's3://{bucket_name}/b3/'
database_name = 'default'
table_name = 'b3_tbl_refined'

# Schema definition (adjust as needed)
schema_columns = [
    {'Name': 'ticker', 'Type': 'string'},
    {'Name': 'type', 'Type': 'string'},
    {'Name': 'part', 'Type': 'double'},
    {'Name': 'theoricalQty', 'Type': 'string'},
    {'Name': 'initial_date', 'Type': 'string'},
    {'Name': 'mean_part_7_days', 'Type': 'double'},
    {'Name': 'median_part_7_days', 'Type': 'double'},
    {'Name': 'std_part_7_days', 'Type': 'double'},
    {'Name': 'max_part_7_days', 'Type': 'double'},
    {'Name': 'min_part_7_days', 'Type': 'double'}
]
input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
serde_info = {
    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
    'Parameters': {}
}

# Create boto3 client
session = boto3.Session()
glue_client = session.client('glue')

# Create the table in Glue Data Catalog if it does not exist


def ensure_table_exists():
    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': schema_columns,
                        'Location': input_path,
                        'InputFormat': input_format,
                        'OutputFormat': output_format,
                        'SerdeInfo': serde_info
                    },
                    'PartitionKeys': [
                        {'Name': 'code', 'Type': 'string'},
                        {'Name': 'reference_date', 'Type': 'string'}
                    ],
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification': 'parquet'
                    }
                }
            )
        else:
            raise


ensure_table_exists()

# Read partitioned data from S3
df = spark.read.parquet(input_path)

dyf = DynamicFrame.fromDF(df, glueContext, 'dyf')

s3_client = boto3.client('s3')

# Extract bucket and prefix from input_path
bucket = bucket_name
prefix = 'b3/'

logging.basicConfig(level=logging.INFO)

partitions = set()
paginator = s3_client.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get('Contents', []):
        # Key example: b3/code=ABC/reference_date=2024-01-01/file.parquet
        match = re.search(r'code=([^/]+)/reference_date=([^/]+)/', obj['Key'])
        if match:
            code = match.group(1)
            reference_date = match.group(2)
            partitions.add((code, reference_date))

# Build the list of partitions for Glue
partition_inputs = []
for code, reference_date in partitions:
    partition_location = f's3://{bucket}/b3/code={code}/reference_date={reference_date}/'
    partition_inputs.append({
        'Values': [code, reference_date],
        'StorageDescriptor': {
            'Columns': schema_columns,
            'Location': partition_location,
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': serde_info,
            'Parameters': {}
        },
        'Parameters': {}
    })

# Explicitly register partitions (in batches of up to 100)
if partition_inputs:
    for i in range(0, len(partition_inputs), 100):
        try:
            response = glue_client.batch_create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInputList=partition_inputs[i:i+100]
            )
            if response.get('Errors'):
                logging.error(f"Errors creating partitions: {response['Errors']}")
            else:
                logging.info(f"Successfully created partitions batch {i//100 + 1}")
        except Exception as e:
            logging.error(f"Exception during batch_create_partition: {e}")

job.commit()