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

# --- 3. TRANSFORMAÇÃO DOS DADOS ---
df = source_dyf.toDF()

# Seleção de colunas e limpeza inicial (semelhante ao original)
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

# Requisito 5.B: Renomear duas colunas
df = df.withColumnRenamed("codigo", "codigo_acao") \
       .withColumnRenamed("acao", "nome_acao")

# Preparar coluna de data para cálculos de janela
df = df.withColumn("data_pregao_ts", col("data_pregao").cast(TimestampType()))

# Requisito 5.C: Realizar um cálculo com base na data (Média Móvel de 7 dias)
# Define a janela de operação: para cada ação, ordenada por data, nos últimos 7 dias
window_spec_moving_avg = Window.partitionBy("codigo_acao") \
                               .orderBy("data_pregao_ts") \
                               .rowsBetween(-6, 0) # 6 dias anteriores + dia atual = 7 dias

df = df.withColumn(
    "media_movel_7d_qtde_teorica",
    avg("quantidade_teorica").over(window_spec_moving_avg)
)

# Requisito 5.A: Agrupamento e sumarização (Soma da quantidade por setor no dia)
# Define a janela de operação: para cada setor, no mesmo dia
window_spec_group_sum = Window.partitionBy("data_pregao", "setor")

df = df.withColumn(
    "total_qtde_teorica_setor_dia",
    sum("quantidade_teorica").over(window_spec_group_sum)
)

# Formata a coluna de data para o padrão YYYY-MM-DD para o particionamento
df = df.withColumn("data_pregao", date_format(col("data_pregao_ts"), "yyyy-MM-dd"))
df = df.drop("data_pregao_ts") # Remove a coluna de data temporária

# Converter de volta para DynamicFrame
transformed_dyf = DynamicFrame.fromDF(df, glueContext, "transformed_dyf")


# --- 4. CARGA DOS DADOS REFINADOS ---

# Requisito 7: Configurar o sink para salvar no S3 e catalogar no Glue Data Catalog
datasink = glueContext.getSink(
    path="s3://postech-ml-fase2-us-east-2/refined/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    # Requisito 6: Particionar por data e código da ação
    partitionKeys=["data_pregao", "codigo_acao"],
    enableUpdateCatalog=True,
    transformation_ctx="datasink"
)

# Define o banco de dados e a tabela a serem criados/atualizados no catálogo
datasink.setCatalogInfo(
    catalogDatabase="postech_ml_fase2_db",
    catalogTableName="dados_refinados_acoes"
)

# Requisito 6: Salvar no formato Parquet
datasink.setFormat("glueparquet", compression="snappy")

# Escreve o DynamicFrame processado
datasink.writeFrame(transformed_dyf)


# --- 5. FINALIZAÇÃO DO JOB ---
job.commit()