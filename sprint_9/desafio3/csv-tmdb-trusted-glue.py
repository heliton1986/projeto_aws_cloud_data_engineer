# Importando libs
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_RAW_PATH', 'S3_TRUSTED_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_dynamic = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            args['S3_RAW_PATH']
        ]
    },
    "csv",
    {"withHeader": True, "separator": "|"},
)

data_frame = df_dynamic.toDF()

# Remove colunas indesejadas
columns_to_drop = ["anoFalecimento", "profissao", "titulosMaisConhecidos", "personagem"]
data_frame = data_frame.drop(*columns_to_drop)

# Converte os tipos de dados necessários
data_frame = data_frame.withColumn("id", col("id").cast("string")) \
    .withColumn("anoLancamento", col("anoLancamento").cast("int")) \
    .withColumn("notaMedia", col("notaMedia").cast("float")) \
    .withColumn("numeroVotos", col("numeroVotos").cast("int")) \
    .withColumn("anoNascimento", col("anoNascimento").cast("int"))

# Substitui os valores "\N" por nulos (None)
data_frame = data_frame.replace("\\N", None)

# Remove registros com valores nulos
data_frame = data_frame.na.drop()

# Remove IDs duplicados
data_frame = data_frame.dropDuplicates(["id"])

# Filtra os registros que contenham na coluna "genero" os valores "horror" e selecionar colunas "id", "tituloPincipal", "anoLancamento", "genero", "notaMedia", "numeroVotos"
data_frame_genero = data_frame.where(col("genero").contains("Horror")).select("id", "tituloPincipal", "anoLancamento", "genero", "notaMedia", "numeroVotos")

# Salva o resultado em formato Parquet no caminho especificado
data_frame_genero.write.mode("overwrite").parquet(args['S3_TRUSTED_PATH'])

job.commit()