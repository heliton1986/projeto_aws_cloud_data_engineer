## Processamento de Dados para Trusted do CSV

Foi feito jobs no Aws Glue para:
- Remover colunas que não serão usadas para análises futuras:
```
columns_to_drop = ["anoFalecimento", "profissao", "titulosMaisConhecidos", "personagem"]
data_frame = data_frame.drop(*columns_to_drop)
```
- Definir os schemas das colunas para os tipos corretos:
```
data_frame = data_frame.withColumn("id", col("id").cast("string")) \
    .withColumn("anoLancamento", col("anoLancamento").cast("int")) \
    .withColumn("notaMedia", col("notaMedia").cast("float")) \
    .withColumn("numeroVotos", col("numeroVotos").cast("int")) \
    .withColumn("anoNascimento", col("anoNascimento").cast("int"))
```
- Substituição dos valores igual a "\N" para nulo "None":
```
data_frame = data_frame.replace("\\N", None)
```
- Remoção dos valores nulos:
```
data_frame = data_frame.na.drop()
```
- Remoção dos Ids duplicados:
```
data_frame = data_frame.dropDuplicates(["id"])
```
- Filtro dos registros que contenham na coluna "genero", o valor "horror" e selecionar colunas "id", "tituloPincipal", "anoLancamento", "genero", "notaMedia", "numeroVotos":
```
data_frame_genero = data_frame.where(col("genero").contains("Horror")) \
    .select("id", "tituloPincipal", "anoLancamento", "genero", "notaMedia", "numeroVotos")
```
- Salvar o resultado em formato Parquet.
```
data_frame_genero.write.mode("overwrite").parquet(args['S3_TRUSTED_PATH'])
```

## Processamento de Dados para Trusted do JSON

Foi feito jobs no Aws Glue para:
- Definir os schemas das colunas para os tipos corretos:
```
schema = StructType([
    StructField('title', StringType(), True),
    StructField('id', IntegerType(), True),
    StructField('popularity', StringType(), True),
    StructField('overview', StringType(), True),
    StructField('genres', ArrayType(StringType()), True),
    StructField('release_date', StringType(), True)
])
```
- Definir a coluna genres de array para string:
```
data_frame = data_frame.withColumn("genres", concat_ws(", ", col("genres")))
```
- Definir a coluna release_date de string para data:
```
data_frame = data_frame.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
```
- Filtrar os filmes do gênero de terror:
```
df_horror = data_frame.where(col("genres").contains("Terror")) \
    .select("id", "title", "overview", "genres", "popularity", "release_date")
```
- Salva o resultado em formato Parquet no caminho especificado:
```
df_horror.write.mode("overwrite").parquet(args['S3_TRUSTED_PATH'])
```

## Criando Database, Tabelas e Views de dimensão e fato na Trusted e Código SQL para Catálogo

![database-trusted](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/e7c54cae-4b0f-4ebb-8684-0c2aac54728d)

```
-- Criar tabela csv
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
-- Criar tabela json
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

-- criar view dim_nome_filme na trusted
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

-- criar view fact_popularidade na trusted
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
-- criar view dim_tempo na trusted
CREATE OR REPLACE VIEW dim_tempo AS
SELECT tb_csv.id as id_csv, 
       tb_json.id as id_json, 
       tb_csv.anolancamento, 
       tb_json.release_date
FROM tb_csv
JOIN tb_json
ON tb_csv.tituloPincipal = tb_json.title; 

```

## Modelagem da Trusted para Refined

Segue o código para modelar os dados da Trusted e envio para a Refined:

![1](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/aeec83f7-85d9-42d0-be0c-e0aa12981b7a)

![2](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/700f0f5c-f783-4bd2-80fd-0fa188572cf9)

![3](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/39e6c917-9b77-4b0a-8370-4de5bb8a5f28)

![4](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/52082bfd-8f2f-4187-9c8b-aa94c157b9ea)

![5](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/95ac0ebe-4bff-499c-b7fe-4192c79e91e0)

![6](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/56c2f507-0b39-4483-a503-dd53af7f4257)


## Criando Database Database, Tabelas de Agregação e Ordenação na Refined  e Código SQL para Catálogo

![order-agrega](https://github.com/heliton1986/aws_cloud_data_engineer_compass_uol/assets/45739569/276b99c8-5e66-4666-88f4-ee609c1f157e)
```
-- criar tabela joined_df na refined
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
-- criar tabela aggregated_order_df na refined
CREATE EXTERNAL TABLE IF NOT EXISTS refined.aggregated_order_df (
    anolancamento int,
    tituloPincipal string,
    maior_numero_votos_por_ano int,
    maior_popularidade_por_ano double
)
STORED AS PARQUET
LOCATION 's3://data-lake-heliton/refined/agregar_ordenar/';
```













