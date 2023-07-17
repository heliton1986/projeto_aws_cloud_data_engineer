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





