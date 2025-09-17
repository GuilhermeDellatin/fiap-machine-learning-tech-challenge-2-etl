import sys
import re
import time
import logging
from typing import Set, Tuple, List, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

ARGS = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bucket_name',   # ex: postech-ml-fase2-us-east-2
    'prefix',        # ex: refined/ (com barra)
    'database',      # ex: default
    'table',         # ex: b3_prego_table_refined
    # opcional incremental:
    'input_uri'      # ex: s3://postech-ml-fase2-us-east-2/refined/code=PETR4/reference_date=2024-01-01/part-000.snappy.parquet
])

bucket_name = ARGS.get('bucket_name', 'postech-ml-fase2-us-east-2')
prefix      = ARGS.get('prefix', 'refined/')
database    = ARGS.get('database', 'default')
table       = ARGS.get('table', 'b3_prego_table_refined')
input_uri   = ARGS.get('input_uri')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(ARGS['JOB_NAME'], ARGS)

INPUT_FORMAT  = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
SERDE_INFO    = {
    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
    'Parameters': {'serialization.format': '1'}
}

# Ajuste o schema conforme seu refined (mantive o seu)
SCHEMA_COLUMNS = [
    {'Name': 'ticker',              'Type': 'string'},
    {'Name': 'type',                'Type': 'string'},
    {'Name': 'part',                'Type': 'double'},
    {'Name': 'theoricalQty',        'Type': 'string'},
    {'Name': 'initial_date',        'Type': 'string'},
    {'Name': 'mean_part_7_days',    'Type': 'double'},
    {'Name': 'median_part_7_days',  'Type': 'double'},
    {'Name': 'std_part_7_days',     'Type': 'double'},
    {'Name': 'max_part_7_days',     'Type': 'double'},
    {'Name': 'min_part_7_days',     'Type': 'double'}
]
PARTITION_KEYS = [
    {'Name': 'code',            'Type': 'string'},
    {'Name': 'reference_date',  'Type': 'string'}
]

session = boto3.Session()
glue = session.client('glue')
s3   = session.client('s3')

def norm_prefix(p: str) -> str:
    p = p.lstrip('/')
    return p if p.endswith('/') else p + '/'

prefix = norm_prefix(prefix)
base_location = f's3://{bucket_name}/{prefix}'

PART_RE = re.compile(r'code=([^/]+)/reference_date=(\d{4}-\d{2}-\d{2})/')

def ensure_table():
    try:
        glue.get_table(DatabaseName=database, Name=table)
        log.info("Tabela %s.%s já existe.", database, table)
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityNotFoundException':
            raise
        glue.create_table(
            DatabaseName=database,
            TableInput={
                'Name': table,
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {'classification': 'parquet', 'EXTERNAL': 'TRUE'},
                'PartitionKeys': PARTITION_KEYS,
                'StorageDescriptor': {
                    'Columns': SCHEMA_COLUMNS,
                    'Location': base_location,
                    'InputFormat': INPUT_FORMAT,
                    'OutputFormat': OUTPUT_FORMAT,
                    'SerdeInfo': SERDE_INFO,
                    'StoredAsSubDirectories': True
                }
            }
        )
        log.info("Tabela %s.%s criada.", database, table)

def existing_parts() -> Set[Tuple[str, str]]:
    out: Set[Tuple[str, str]] = set()
    paginator = glue.get_paginator('get_partitions')
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        for p in page.get('Partitions', []):
            vals = p.get('Values', [])
            if len(vals) >= 2:
                out.add((vals[0], vals[1]))
    log.info("Partições já registradas: %d", len(out))
    return out

def parse_from_uri(uri: str) -> Optional[Tuple[str, str]]:
    m = re.match(r's3://[^/]+/(.+)', uri or '')
    key = m.group(1) if m else uri
    m2 = PART_RE.search(key or '')
    return (m2.group(1), m2.group(2)) if m2 else None

def discover_parts_s3() -> Set[Tuple[str, str]]:
    found: Set[Tuple[str, str]] = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            m = PART_RE.search(obj['Key'])
            if m:
                found.add((m.group(1), m.group(2)))
    log.info("Partições encontradas no S3: %d", len(found))
    return found

def build_partition_input(code: str, refdate: str) -> Dict:
    return {
        'Values': [code, refdate],
        'StorageDescriptor': {
            'Columns': SCHEMA_COLUMNS,
            'Location': f's3://{bucket_name}/{prefix}code={code}/reference_date={refdate}/',
            'InputFormat': INPUT_FORMAT,
            'OutputFormat': OUTPUT_FORMAT,
            'SerdeInfo': SERDE_INFO,
            'StoredAsSubDirectories': True,
            'Parameters': {}
        },
        'Parameters': {}
    }

def batch_create(parts: List[Dict], batch_size=100, max_retries=5):
    if not parts: return
    for i in range(0, len(parts), batch_size):
        batch = parts[i:i+batch_size]
        attempt = 0
        while True:
            try:
                resp = glue.batch_create_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionInputList=batch
                )
                errs = resp.get('Errors') or []
                if errs:
                    log.warning("Erros no lote %d: %s", (i//batch_size)+1, errs)
                else:
                    log.info("Lote %d criado (%d partições).", (i//batch_size)+1, len(batch))
                break
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code', '')
                if code in {'ThrottlingException','TooManyRequestsException'} and attempt < max_retries:
                    attempt += 1
                    time.sleep(min(2**attempt, 32))
                    continue
                raise

def main():
    ensure_table()

    if input_uri:
        # Incremental
        parsed = parse_from_uri(input_uri)
        if not parsed:
            log.warning("input_uri sem padrão de partição: %s", input_uri)
            job.commit()
            return
        wanted = {parsed}
        log.info("Modo incremental: %s", parsed)
    else:
        # Full scan
        wanted = discover_parts_s3()

    if not wanted:
        log.info("Sem partições a registrar.")
        job.commit()
        return

    existing = existing_parts()
    to_create = sorted(wanted - existing)
    if not to_create:
        log.info("Todas as partições já existem.")
        job.commit()
        return

    part_inputs = [build_partition_input(code, refdate) for code, refdate in to_create]
    batch_create(part_inputs)

    job.commit()
    log.info("Concluído.")

if __name__ == '__main__':
    main()
