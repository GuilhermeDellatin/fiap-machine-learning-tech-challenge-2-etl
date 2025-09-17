import sys
import re
import time
import logging
from typing import Set, Tuple, List, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

ARGS = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bucket',    # ex: postech-ml-fase2-us-east-2
    'prefix',    # ex: raw/  (com ou sem barra)
    'database',  # ex: default
    'table',     # ex: b3_pregao_table_raw
    # opcional p/ modo incremental:
    'input_uri'  # ex: s3://postech-ml-fase2-us-east-2/raw/date=2025-09-16/b3_.parquet
]) if '--JOB_NAME' in sys.argv or 'JOB_NAME' in sys.argv else {
    # fallback local/dev (opcional)
}

# Defaults p/ execução local (remove se preferir estrito)
bucket = ARGS.get('bucket', 'postech-ml-fase2-us-east-2')
prefix = ARGS.get('prefix', 'raw/')
database = ARGS.get('database', 'default')
table = ARGS.get('table', 'b3_pregao_table_raw')
input_uri = ARGS.get('input_uri')  # None => full scan

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def norm_prefix(p: str) -> str:
    p = p.lstrip('/')
    return p if p.endswith('/') else p + '/'


prefix = norm_prefix(prefix)
base_s3 = f's3://{bucket}/{prefix}'

SCHEMA_COLUMNS = [
    {'Name': 'segment', 'Type': 'int'},
    {'Name': 'cod', 'Type': 'string'},
    {'Name': 'asset', 'Type': 'string'},
    {'Name': 'type', 'Type': 'string'},
    {'Name': 'part', 'Type': 'string'},
    {'Name': 'partAcum', 'Type': 'int'},
    {'Name': 'theoricalQty', 'Type': 'string'}
]
PARTITION_KEYS = [{'Name': 'date', 'Type': 'string'}]

INPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUT_FORMAT = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
SERDE_INFO = {
    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
    'Parameters': {'serialization.format': '1'}
}

DATE_RE = re.compile(r'date=(\d{4}-\d{2}-\d{2})/')

session = boto3.Session()
glue = session.client('glue')
s3 = session.client('s3')


def ensure_database(db: str):
    try:
        glue.create_database(DatabaseInput={'Name': db})
        log.info("Database '%s' criado.", db)
    except ClientError as e:
        if e.response['Error']['Code'] != 'AlreadyExistsException':
            raise
        log.info("Database '%s' já existe.", db)


def ensure_table(db: str, tbl: str, location: str):
    try:
        glue.get_table(DatabaseName=db, Name=tbl)
        log.info("Tabela %s.%s já existe.", db, tbl)
        return
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityNotFoundException':
            raise
    glue.create_table(
        DatabaseName=db,
        TableInput={
            'Name': tbl,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {'classification': 'parquet', 'EXTERNAL': 'TRUE'},
            'PartitionKeys': PARTITION_KEYS,
            'StorageDescriptor': {
                'Columns': SCHEMA_COLUMNS,
                'Location': location,
                'InputFormat': INPUT_FORMAT,
                'OutputFormat': OUTPUT_FORMAT,
                'SerdeInfo': SERDE_INFO,
                'StoredAsSubDirectories': True
            }
        }
    )
    log.info("Tabela %s.%s criada.", db, tbl)


def existing_partitions(db: str, tbl: str) -> Set[str]:
    vals: Set[str] = set()
    paginator = glue.get_paginator('get_partitions')
    for page in paginator.paginate(DatabaseName=db, TableName=tbl):
        for p in page.get('Partitions', []):
            if p.get('Values'):
                vals.add(p['Values'][0])  # date
    log.info("Partições já no catálogo: %d", len(vals))
    return vals


def discover_dates_from_s3(bucket: str, pref: str) -> Set[str]:
    found: Set[str] = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=pref):
        for obj in page.get('Contents', []):
            m = DATE_RE.search(obj['Key'])
            if m:
                found.add(m.group(1))
    log.info("Partições encontradas no S3: %d", len(found))
    return found


def date_from_input_uri(uri: str) -> Optional[str]:
    m = re.match(r's3://[^/]+/(.+)', uri or '')
    key = m.group(1) if m else uri
    m2 = DATE_RE.search(key or '')
    return m2.group(1) if m2 else None


def batch_create(db: str, tbl: str, parts: List[Dict], batch_size=100, max_retries=5):
    if not parts:
        return
    for i in range(0, len(parts), batch_size):
        batch = parts[i:i + batch_size]
        attempt = 0
        while True:
            try:
                resp = glue.batch_create_partition(
                    DatabaseName=db, TableName=tbl, PartitionInputList=batch
                )
                errs = resp.get('Errors') or []
                if errs:
                    log.warning("Erros no lote %d: %s", (i // batch_size) + 1, errs)
                else:
                    log.info("Lote %d criado com sucesso (%d partições).",
                             (i // batch_size) + 1, len(batch))
                break
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code', '')
                if code in {'ThrottlingException', 'TooManyRequestsException'} and attempt < max_retries:
                    attempt += 1
                    time.sleep(min(2 ** attempt, 32))
                    continue
                raise


def main():
    ensure_database(database)
    ensure_table(database, table, base_s3)

    if input_uri:
        # modo incremental
        date_val = date_from_input_uri(input_uri)
        if not date_val:
            log.warning("Não consegui extrair 'date=' de %s. Nada a registrar.", input_uri)
            return
        wanted = {date_val}
        log.info("Modo incremental: registrando apenas date=%s", date_val)
    else:
        # modo full
        wanted = discover_dates_from_s3(bucket, prefix)

    if not wanted:
        log.info("Nenhuma partição para registrar.")
        return

    existing = existing_partitions(database, table)
    new_vals = sorted(wanted - existing)
    if not new_vals:
        log.info("Nenhuma partição nova (tudo já no catálogo).")
        return

    part_inputs = [{
        'Values': [d],
        'StorageDescriptor': {
            'Columns': SCHEMA_COLUMNS,
            'Location': f'{base_s3}date={d}/',
            'InputFormat': INPUT_FORMAT,
            'OutputFormat': OUTPUT_FORMAT,
            'SerdeInfo': SERDE_INFO,
            'StoredAsSubDirectories': True
        },
        'Parameters': {}
    } for d in new_vals]

    batch_create(database, table, part_inputs)


if __name__ == '__main__':
    main()
