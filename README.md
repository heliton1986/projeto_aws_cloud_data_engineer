# Projeto AWS Cloud Data Engineer

## Apresentação

Nesse projeto foi feito a ingestão de 2 arquivos csv series e movies para a camada RAW do S3, separando através dos diretórios Movies e Series e nestes, os subdiretórios de datas particionando por ano, mês e dia, através de um container Docker utilizando Python e sua biblioteca boto3.

Em seguida, através da API do site do TMDB realizei a ingestão dos dados no formato json utilizando o AWS Lambda também para a camada RAW do S3, separando através do diretório TMDB e dentro deste, os subdiretórios de datas particionando por ano, mês e dia, fazendo com que tenha uma governança de dados eficiente para acesso a cada área específica.

Com os arquivos csv e json na camada Raw, criei um job no Aws Glue com pyspark para limpar os dados, como remoção de registros duplicados, registros nulos, filtrando os filmes pelo gênero de terror e persistindo ambos os arquivos para o formato parquet para a camada Trusted, sendo o json particionado por ano/mês/dia.
Criando catálogo de dados no Glue através de Query SQL pelo Aws Athena.

Em sequência, criei outro job Glue para puxar as tabelas criadas no pelo Athena e criar views temporárias de dimensões e fato, join entre estas views, agregações e ordenações através do spark.sql, atribuindo à dataframes específicos e persistindo em formato parquet na camada Refined.

Nessa camada criei novos catálogos de dados através do Athena e o diagrama dimensional para entrega para o time de negócio trabalhar com alguma ferramenta de self-service BI.

### Primeira Parte

![projeto parte 1](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/37c21771-49b3-43c4-a438-b3423c449a51)


- Confirmação de conexão da AWS com Boto3:
  
![aws_cli_boto3_confirmacao](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/5aee09d3-1f77-44c5-8a4c-efb7b5f4867e)

Em seguida, através da API do site do TMDB realizei a ingestão dos dados no formato json utilizando o AWS Lambda também para a camada RAW do S3, separando através do diretório TMDB e dentro deste, os subdiretórios de datas particionando por ano, mês e dia, fazendo com que tenha uma governança de dados eficiente para acesso a cada área específica.

- Código Python que carrega arquivos CSV para a Nuvem para o bucket S3 na camada Raw:

```python
import datetime
import logging
import os
import boto3
from botocore.exceptions import ClientError

session = boto3.Session(profile_name="heliton")

s3 = session.client('s3')

date = datetime.datetime.now()

def subir_arquivo(bucket, object_name=None):

    categorias = ['Movies', 'Series']

    try:

        for i in range(len(categorias)):

            fazer_upload(categorias[i], object_name, bucket)

    except ClientError as e:

        logging.error(e)

        return False

    finally:

        print("\nExecução finalizada")

    return True


def fazer_upload(categorias, object_name, bucket):

    if object_name is None:
        object_name = os.path.basename(
            f"{(categorias).lower()}.csv")


    file = f"{(categorias).lower()}.csv"

    outpath = "RAW/Local/CSV/{}/{}/{}/{}/{}".format(
        categorias, date.year, date.month, date.day, object_name)

    print(f'\nIniciando carregamento em {outpath}')

    response = s3.upload_file(file, bucket, outpath)

    print('Carregamento finalizado....')

    return True

if __name__ == '__main__':

    subir_arquivo("data-lake-heliton")
```

- Particionamento do Csv no S3 na camada Raw

![aws_cli_boto3_s3_1](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/a029a49f-dba0-4aed-954e-f8737fc429dc)
![aws_cli_boto3_s3_2](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/2a74b43d-401e-4259-8a8a-e211386f9034)
![aws_cli_boto3_s3_3](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/a41dfd84-dc70-48f6-83ba-d6b5988f9f77)

### Segunda Parte

Agora, com os arquivos csv na camada Raw, criei um Lambda para extrair os dodos da API do TMDB também para a camada Raw no formato Json particionado por ano, mês, dia.

- Criando role Lambda para obter permissões de acesso ao S3:

![01-desafio2_permissoes_lambda](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/d139778b-0cb9-452c-989f-6aac8beb8e7c)

- Criação da Layer para execução do Lambda:

![02-desafio2_criar_camada_zip](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/e02bf312-8553-40c0-aa7e-a1e6b52dcbcd)

- Agendamento do Lambda:

![03-desafio2_agendamento](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/3ac842ee-6775-4e7e-95ba-090b6f8a3895)

- Código Python para extrair dados da API para o S3 na camada Raw:

```python
from tmdbv3api import TMDb, Discover, Genre
from datetime import datetime, timedelta
import boto3
import json
from botocore.exceptions import ClientError
import logging
import os

# Criar uma sessão do boto3

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
API_KEY = os.environ["API_KEY"]

session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN
)

def lambda_handler(event, context):
    tmdb = TMDb()
    tmdb.api_key = API_KEY
    tmdb.language = 'pt-BR'
    
    # metodos TMDB para achar os filmes
    discover = Discover()
    genre = Genre()
    
    # Obtenha a lista de gêneros de filmes
    genres = genre.movie_list()
    
    # Criando um dicionário para mapear IDs de gênero para nomes de gênero
    genre_dict = {g['id']: g['name'] for g in genres}
    
    # Parametros para pegar as datas do intervalo
    params = {
        'with_genres': 27 # ID do gênero de terror
    }
    
    # count contar a quantidade de filmes achados
    # escolher a quantidade de paginas da API
    count = 0
    movies_data = []
    
    for page in range(1, 101):
        params['page'] = page
        movies = discover.discover_movies(params)
        count += len(movies)
    
        for movie in movies:
            print(movie)
            movie_data = movie.__dict__.copy()
            movie_data['genres'] = [genre_dict[g] for g in movie['genre_ids']]
            movies_data.append(movie_data)
    
    # Convertendo a lista de dados dos filmes em formato JSON
    movies_json = json.dumps(movies_data, ensure_ascii=False)

    
    # Conexão com o S3
    s3 = session.client('s3')
    
    # O caminho completo no bucket S3
    bucket_name = 'data-lake-heliton'
    current_date = datetime.now()
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')
    object_key = f'RAW/TMDB/JSON/{year}/{month}/{day}/filmes_popular.json'
    
    # Envio do JSON para o bucket no S3
    s3.put_object(
        Body=movies_json,
        Bucket=bucket_name,
        Key=object_key
    )
```

- Deploy do Lambda:

![05-desafio2_log_deploy_lambda](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/2449f304-aaf2-4cbb-a525-a0e4e6ab45da)

- Log de Agendamento:
![06-desafio2_log_agendamento](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/d548a6ee-8db3-4f75-ab91-bfca6d945b67)

-  Particionamento do Json no S3 na camada Raw:

![07-desafio2_s3](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/2c9c9e6c-6423-422d-ae75-0aff725ac31e)

### Terceira Parte

Agora, com os arquivos csv e json na camada Raw, criei um job no Aws Glue com pyspark para limpar os dados, como remoção de registros duplicados, registros nulos, filtrando os filmes pelo gênero de terror e persistindo ambos os arquivos para o formato parquet para a camada Trusted, sendo o json particionado por ano/mês/dia.

- Criação do Job Glue para transformação dos Csv da camada Raw para a Trusted:

![csv-tmdb-trusted-glue-1](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/49e192b2-ce78-4495-946e-bd1047966933)
![csv-tmdb-trusted-glue-2](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/1afe5d3e-8d99-4337-8383-c072dfd492d4)

- Código Pyspark para transformação e carregamento dos dados do Csv para a camada Trusted em formato parquet:

````python
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
````

- Criação do Job Glue para transformação do Json da camada Raw para a Trusted:

![json-tmdb-trusted-glue-1](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/5f8c82c4-8051-4b14-9e85-4fab6c6d8545)
![json-tmdb-trusted-glue-2](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/fde145a6-da06-4d5c-8c26-c6de13586775)

- Código Pyspark para transformação e carregamento dos dados do Json para a camada Trusted em formato parquet:

````python
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
````

- Criando catálogo de dados no Glue através de Query SQL pelo Aws Athena.

````sql
-- Criando tabela csv
CREATE EXTERNAL TABLE tb_csv (
    id string,
    tituloPincipal string,
    anoLancamento integer,
    genero string,
    notaMedia float,
    numeroVotos integer 
)
STORED AS PARQUET
LOCATION 's3://data-lake-heliton/trusted/csv_imdb/movies/'

-----------------------------------
-- Criando tabela json
CREATE EXTERNAL TABLE tb_json (
  id int,
  title string,
  overview string,
  genres string,
  popularity double,
  release_date date
)
STORED AS PARQUET
LOCATION 's3://data-lake-heliton/trusted/TMDB/2023/07/07/'
----------------------------------------

-- Criando view dim_nome_filme na trusted
CREATE OR REPLACE TEMP VIEW dim_nome_filme AS
SELECT tb_csv.id as id_csv, 
    tb_json.id as id_json,
    tb_csv.tituloPincipal, 
    tb_csv.genero, 
    tb_json.overview
FROM tb_csv
JOIN tb_json
ON tb_csv.tituloPincipal = tb_json.title
----------------------------

-- Criando view fact_popularidade na trusted
REATE OR REPLACE VIEW fact_popularidade AS
SELECT tb_csv.id as id_csv, 
       tb_json.id as id_json,
       tb_csv.notamedia, 
       tb_csv.numerovotos, 
       tb_json.popularity
FROM tb_csv
JOIN tb_json
ON tb_csv.tituloPincipal = tb_json.title; 

----------------------------------------------
-- Criando view dim_tempo na trusted
CREATE OR REPLACE VIEW dim_tempo AS
SELECT tb_csv.id as id_csv, 
       tb_json.id as id_json, 
       tb_csv.anolancamento, 
       tb_json.release_date
FROM tb_csv
JOIN tb_json
ON tb_csv.tituloPincipal = tb_json.title; 

--------------------------------
-- Criando tabela joined_df na refined
CREATE EXTERNAL TABLE IF NOT EXISTS refined.joined_df (
    tituloPincipal string,
    genero string,
    notamedia double,
    numerovotos int,
    popularity double,
    anolancamento int,
    release_date date,
    overview string
)
STORED AS PARQUET
LOCATION 's3://data-lake-heliton/refined/juncao_dimensao_fato/';

------------------------------------
-- Criando tabela aggregated_order_df na refined
CREATE EXTERNAL TABLE IF NOT EXISTS refined.aggregated_order_df (
    anolancamento int,
    tituloPincipal string,
    maior_numero_votos_por_ano int,
    maior_popularidade_por_ano double
)
STORED AS PARQUET
LOCATION 's3://data-lake-heliton/refined/agregar_ordenar/';
````

Em sequência, criei outro job Glue para puxar as tabelas criadas no pelo Athena e criar views temporárias de dimensões e fato, join entre estas views, agregações e ordenações através do spark.sql, atribuindo à dataframes específicos e persistindo em formato parquet na camada Refined.

````python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
import time

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Criação um DynamicFrame a partir de uma tabela do catálogo de dados do AWS Glue
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="trusted-csv-json-modelagem-refined", table_name="tb_csv")

# Criação um DynamicFrame para a segunda tabela
datasource1 = glueContext.create_dynamic_frame.from_catalog(database="trusted-csv-json-modelagem-refined", table_name="tb_json")

# Convertendo DynamicFrame em DataFrame
df0 = datasource0.toDF()
df1 = datasource1.toDF()

# Criaçao uma view temporária
df0.createOrReplaceTempView("tb_csv")
df1.createOrReplaceTempView("tb_json")

df_tb_csv = df0
df_tb_json = df1

# Criação a view dim_nome_filme
spark.sql("""
    CREATE OR REPLACE TEMP VIEW dim_nome_filme AS
    SELECT tb_csv.id as id_csv, 
          tb_json.id as id_json,
          tb_csv.tituloPincipal, 
          tb_csv.genero, 
          tb_json.overview
    FROM tb_csv
    JOIN tb_json
    ON tb_csv.tituloPincipal = tb_json.title
""")

# Criação a view fact_popularidade
spark.sql("""
    CREATE OR REPLACE TEMP VIEW fact_popularidade AS
    SELECT tb_csv.id as id_csv, 
          tb_json.id as id_json,
          tb_csv.notamedia, 
          tb_csv.numerovotos, 
          tb_json.popularity
    FROM tb_csv
    JOIN tb_json
    ON tb_csv.tituloPincipal = tb_json.title
""")

# Criação a view dim_tempo
spark.sql("""
    CREATE OR REPLACE TEMP VIEW dim_tempo AS
    SELECT tb_csv.id as id_csv, 
          tb_json.id as id_json, 
          tb_csv.anolancamento, 
          tb_json.release_date
    FROM tb_csv
    JOIN tb_json
    ON tb_csv.tituloPincipal = tb_json.title
""")

# Carregar as views como DataFrames
df_dim_nome_filme = spark.table("dim_nome_filme")
df_fact_popularidade = spark.table("fact_popularidade")
df_dim_tempo = spark.table("dim_tempo")

    
# Realizar a junção das tabelas e selecionar as colunas desejadas
joined_df = spark.sql("""
    SELECT
        dim_nome_filme.tituloPincipal,
        dim_nome_filme.genero,
        fact_popularidade.notamedia,
        fact_popularidade.numerovotos,
        fact_popularidade.popularity,
        dim_tempo.anolancamento,
        dim_tempo.release_date,
        dim_nome_filme.overview
    FROM
        dim_nome_filme
    JOIN
        fact_popularidade ON dim_nome_filme.id_csv = fact_popularidade.id_csv
    JOIN
        dim_tempo ON dim_nome_filme.id_csv = dim_tempo.id_csv
""")

# view temporária para o DataFrame joined_df
joined_df.createOrReplaceTempView("joined_df")

# Realizar a agregação e ordenar o resultado
aggregated_order_df = spark.sql("""
    SELECT
        anolancamento,
        tituloPincipal,
        MAX(numerovotos) AS maior_numero_votos_por_ano,
        MAX(popularity) AS maior_popularidade_por_ano
    FROM
        joined_df
    GROUP BY
        anolancamento,
        tituloPincipal
    ORDER BY
        anolancamento DESC,
        maior_popularidade_por_ano DESC,
        maior_numero_votos_por_ano DESC
""")

#  Salvar as dimensões e a tabela de fato em um local de sua escolha
output_path_dim_nome_filme = "s3://data-lake-heliton/refined/dim_nome_filme/"
output_path_dim_tempo = "s3://data-lake-heliton/refined/dim_tempo/"
output_path_fact_popularidade = "s3://data-lake-heliton/refined/fact_popularidade/"
output_path_joined_df = "s3://data-lake-heliton/refined/juncao_dimensao_fato/"
output_path_aggregated_order_df = "s3://data-lake-heliton/refined/agregar_ordenar/"


df_dim_nome_filme.write.parquet(output_path_dim_nome_filme, mode="overwrite")
time.sleep(10)
df_dim_tempo.write.parquet(output_path_dim_tempo, mode="overwrite")
time.sleep(10)
df_fact_popularidade.write.parquet(output_path_fact_popularidade, mode="overwrite")
time.sleep(10)
joined_df.write.parquet(output_path_joined_df, mode="overwrite")
time.sleep(10)
aggregated_order_df.write.parquet(output_path_aggregated_order_df, mode="overwrite")
````

Nessa camada criei novos catálogos de dados através do Athena e o diagrama dimensional para entrega para o time de negócio trabalhar com alguma ferramenta de self-service BI.

- Dimensões e fato criadas na camada Refined:

![refined_s3](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/cb0c2a57-68f3-4bfd-bc35-d634659b1b1e)

- Diagrama Dimensional:

![modelagem-refined](https://github.com/heliton1986/projeto_aws_cloud_data_engineer/assets/45739569/b0714a44-de1d-4c6d-869f-addf4eeff9f3)



