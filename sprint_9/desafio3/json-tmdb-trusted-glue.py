import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import from_json, col, explode, array_contains, concat_ws, to_date
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_RAW_PATH', 'S3_TRUSTED_PATH'])

conf = SparkConf().setAppName("TMDB").setMaster("local")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Lê os dados JSON do bucket S3
data_frame = spark.read.json(args['S3_RAW_PATH'], multiLine=True)

# Define o esquema dos dados
schema = StructType([
    StructField('title', StringType(), True),
    StructField('id', IntegerType(), True),
    StructField('popularity', StringType(), True),
    StructField('overview', StringType(), True),
    StructField('genres', ArrayType(StringType()), True),
    StructField('release_date', StringType(), True)
])

# Converte a coluna genres de array para string
data_frame = data_frame.withColumn("genres", concat_ws(", ", col("genres")))

# Converte a coluna release_date de string para data
data_frame = data_frame.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

# Filtra os filmes do gênero de terror e selecionando colunas relevantes para análise
df_horror = data_frame.where(col("genres").contains("Terror")).select("id", "title", "overview", "genres", "popularity", "release_date")

# Salva o resultado em formato Parquet no caminho especificado
df_horror.write.mode("overwrite").parquet(args['S3_TRUSTED_PATH'])

job.commit()