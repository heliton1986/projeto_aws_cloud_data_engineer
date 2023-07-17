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

--------------------------------
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