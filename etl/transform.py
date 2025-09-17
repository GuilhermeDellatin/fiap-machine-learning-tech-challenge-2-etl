import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType

ARGS = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'in_database',   # ex: default
    'in_table',      # ex: b3_prego_table_raw
    'out_bucket',    # ex: postech-ml-fase2-us-east-2
    'out_prefix',    # ex: refined
    # opcionais:
    'window_days',   # por linhas (default 7)
    'mode'           # overwrite|append (default overwrite com overwrite dinamico)
])

in_database = ARGS.get('in_database', 'default')
in_table    = ARGS.get('in_table', 'b3_prego_table_raw')
out_bucket  = ARGS.get('out_bucket', 'postech-ml-fase2-us-east-2')
out_prefix  = ARGS.get('out_prefix', 'refined')
window_n    = int(ARGS.get('window_days', '7'))
mode        = ARGS.get('mode', 'overwrite').lower()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(ARGS['JOB_NAME'], ARGS)

# overwrite dinâmico para não apagar partições não tocadas
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

dyf = glueContext.create_dynamic_frame.from_catalog(
    database=in_database,
    table_name=in_table
)
df = dyf.toDF()

# Colunas essenciais esperadas (antes do rename)
essential = ['cod', 'asset', 'type', 'part', 'theoricalQty', 'date']

# Remoção de colunas totalmente nulas (mantém essenciais se existirem)
non_null_counts = df.select([F.count(F.col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
keep_cols = [c for c, cnt in non_null_counts.items() if cnt > 0 or c in essential]
df = df.select(*keep_cols)

# Renomes e normalizações
if 'cod' in df.columns:
    df = df.withColumnRenamed('cod', 'code')
if 'asset' in df.columns:
    df = df.withColumnRenamed('asset', 'ticker')
if 'date' in df.columns:
    df = df.withColumnRenamed('date', 'reference_date')

# Sanitizações: part (double), theoricalQty (bigint), reference_date (string->date->string)
if 'part' in df.columns:
    df = df.withColumn('part', F.regexp_replace(F.col('part'), ',', '.').cast(DoubleType()))

if 'theoricalQty' in df.columns:
    # remove tudo que não for dígito/sinal e converte pra long
    df = df.withColumn('theoricalQty', F.regexp_replace('theoricalQty', r'[^0-9-]', '').cast(LongType()))

if 'reference_date' in df.columns:
    df = df.withColumn('reference_date_date', F.to_date('reference_date', 'yyyy-MM-dd'))
else:
    raise ValueError("Coluna 'reference_date' ausente após renomear 'date'.")

# Remove linhas sem chave de partição
df = df.filter(F.col('code').isNotNull() & F.col('reference_date_date').isNotNull())

# Opcional: deduplicar por (code, reference_date) mantendo o último registro
df = df.withColumn('rn', F.row_number().over(
    Window.partitionBy('code', 'reference_date_date').orderBy(F.monotonically_increasing_id())
)).filter(F.col('rn') == 1).drop('rn')

# initial_date por código (mínimo dentro do código)
df = df.withColumn(
    'initial_date',
    F.date_format(F.min('reference_date_date').over(Window.partitionBy('code')), 'yyyy-MM-dd')
)

# Janelas de N linhas (ex: 7 últimos registros do código)
w = Window.partitionBy('code').orderBy('reference_date_date').rowsBetween(-(window_n-1), 0)

df = df.withColumn('mean_part_7_days',   F.avg('part').over(w)) \
       .withColumn('median_part_7_days', F.expr('percentile_approx(part, 0.5)').over(w)) \
       .withColumn('std_part_7_days',    F.stddev('part').over(w)) \
       .withColumn('max_part_7_days',    F.max('part').over(w)) \
       .withColumn('min_part_7_days',    F.min('part').over(w))

# Ordena e prepara para escrita
out_path = f's3://{out_bucket}/{out_prefix}'

# Mostras rápidas no log (evita prints enormes)
log.info("Escrevendo em %s", out_path)
log.info("Schema final:\n%s", df._jdf.schema().treeString())

(df
 .withColumn('reference_date', F.date_format('reference_date_date', 'yyyy-MM-dd'))
 .drop('reference_date_date')
 .write
 .partitionBy('code', 'reference_date')
 .mode(mode)
 .parquet(out_path)
)

job.commit()
