import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

# @params: [JOB_NAME]


args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

df_dynamic = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file
        ]
    },
    "csv",
    {"withHeader": True, "separator": ","},
)


def uppercase(rec):
    rec["nome"] = rec["nome"].upper()
    return rec

df = Map.apply(frame=df_dynamic, f=uppercase)
        
df_spark = df.toDF()

# Escrever o código necessário para alterar a caixa dos valores da coluna nome
# para MAIÚSCULO.
df_spark.withColumn('nome', upper('nome').alias('Nome Maiuscula')).show()

# Schema
df_spark.printSchema()

# Contagem de registros
print(f'Contagem de registros :{df_spark.count()}')

# Imprimir a contagem de nomes, agrupando os dados do dataframe pelas colunas ano
# e sexo.Ordene os dados de modo que o ano mais recente apareça como primeiro
# registro do dataframe
df_spark.groupBy('ano', 'sexo').agg(count('nome').alias('Contagem')).orderBy(desc('ano')).show()

# Apresentar qual foi o nome feminino com mais registros e em que ano ocorreu
df_spark.filter(df_spark.sexo == 'F').groupBy(df_spark.nome, df_spark.ano).agg(sum(df_spark.total).alias('Registros')).orderBy(desc('Registros')).limit(1).show()

# Apresentar qual foi o nome masculino com mais registros e em que ano ocorreu
df_spark.filter(df_spark.sexo == 'M').groupBy(df_spark.nome, df_spark.ano).agg(sum(df_spark.total).alias('Registros')).orderBy(desc('Registros')).limit(1).show()

# Apresentar o total de registros (masculinos e femininos) para cada ano presente no
# dataframe.Considere apenas as primeiras 10 linhas, ordenadas pelo ano, de forma
# crescente
df_spark.groupBy(df_spark.ano, df_spark.sexo).agg(sum(df_spark.total).alias('Registros')).orderBy(asc('ano')).limit(10).show()


# df = df_dynamic.toDF()
# df_upper = df.withColumn('nome', upper('nome'))
# df_upper.show()


df_spark.write.option("header", True) \
        .partitionBy("sexo", "ano") \
        .mode("overwrite") \
        .json(target_path)


job.commit()