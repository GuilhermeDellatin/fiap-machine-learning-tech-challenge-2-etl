import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, avg, sum, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType

# --- 1. INICIALIZAÇÃO DO JOB ---
# Adicione o novo argumento 's3_input_path' à lista de argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 2. EXTRAÇÃO DOS DADOS BRUTOS ---
# Use o caminho do arquivo recebido da Lambda em vez de um caminho fixo
s3_input_path = args['s3_input_path']

source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [s3_input_path]  # <-- MODIFICADO: Lê apenas o arquivo específico
        # "recurse": True  <-- REMOVIDO: Não é mais necessário
    },
    transformation_ctx="source_dyf"
)

# --- 3. TRANSFORMAÇÃO DOS DADOS (seu código original permanece aqui) ---
df = source_dyf.toDF()

# Seleção de colunas e limpeza inicial
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

# --- NOVAS TRANSFORMAÇÕES OBRIGATÓRIAS ---
df = df.withColumnRenamed("codigo", "codigo_acao") \
       .withColumnRenamed("acao", "nome_acao")

df = df.withColumn("data_pregao_ts", col("data_pregao").cast(TimestampType()))

window_spec_moving_avg = Window.partitionBy("codigo_acao") \
                               .orderBy("data_pregao_ts") \
                               .rowsBetween(-6, 0)
df = df.withColumn(
    "media_movel_7d_qtde_teorica",
    avg("quantidade_teorica").over(window_spec_moving_avg)
)

window_spec_group_sum = Window.partitionBy("data_pregao", "setor")

df = df.withColumn(
    "total_qtde_teorica_setor_dia",
    sum("quantidade_teorica").over(window_spec_group_sum)
)

df = df.withColumn("data_pregao", date_format(col("data_pregao_ts"), "yyyy-MM-dd"))
df = df.drop("data_pregao_ts")

transformed_dyf = DynamicFrame.fromDF(df, glueContext, "transformed_dyf")

# --- 4. CARGA DOS DADOS REFINADOS (sem alterações) ---
# O modo de escrita padrão para partições funciona como um "upsert".
# Como seu DataFrame agora só contém dados do dia, ele apenas adicionará/sobrescreverá
# a partição do dia atual, sem tocar nas antigas.
datasink = glueContext.getSink(
    path="s3://postech-ml-fase2-us-east-2/refined/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["data_pregao", "codigo_acao"],
    enableUpdateCatalog=True,
    transformation_ctx="datasink"
)
datasink.setCatalogInfo(
    catalogDatabase="postech_ml_fase2_db",
    catalogTableName="dados_refinados_acoes"
)
datasink.setFormat("glueparquet", compression="snappy")
datasink.writeFrame(transformed_dyf)

# --- 5. FINALIZAÇÃO DO JOB ---
job.commit()