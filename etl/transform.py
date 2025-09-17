import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://postech-ml-fase2-us-east-2/raw/"],
        "recurse": True
    },
    transformation_ctx="source_dyf"
)

df = source_dyf.toDF()

df = df.select(
    "setor",
    "codigo",
    "acao",
    "tipo",
    "porcentagem_participacao",
    "porcentagem_participacao_acumulada",
    "quantidade_teorica",
    "data_pregao"
)

df = df.dropDuplicates()

df = df.fillna({
    "setor": "UNKNOWN",
    "codigo": "UNKNOWN",
    "acao": "UNKNOWN",
    "tipo": "UNKNOWN",
    "porcentagem_participacao": 0.0,
    "porcentagem_participacao_acumulada": 0.0,
    "quantidade_teorica": 0
})

df = df.withColumn(
    "data_pregao",
    col("data_pregao").cast(StringType())
).fillna({"data_pregao": "1970-01-01"})

transformed_dyf = DynamicFrame.fromDF(df, glueContext, "transformed_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://postech-ml-fase2-us-east-2/refined/",
        "partitionKeys": ["data_pregao"]
    },
    transformation_ctx="refined_dyf"
)

job.commit()
