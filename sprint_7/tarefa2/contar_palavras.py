
# Sparksession
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("Nome do Aplicativo") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Spark Contexto
sc = spark.sparkContext

# Fazendo a leitura do arquivo
arquivo_raw = sc.textFile('/home/heliton/Documentos/repos/novo_aws_cloud_data_engineer_compass_uol/README.md')

# Apresenrtar as 10 linhas
arquivo_raw.take(10)

import re
palavras = arquivo_raw.flatMap(lambda line: re.split('\W+', line.lower().strip()))

# Filtrar apenas palavras com caracteres com tamanho maior ou igual a 1
palavras = palavras.filter(lambda x: len(x) >= 1)

palavras = palavras.map(lambda w: (w,1))

# Criar tupla da quantidade de palavras
from operator import add
palavras = palavras.reduceByKey(add)

palavras.take(10)

# Ordenar palavras
palavras_order = palavras.map(lambda x:(x[1],x[0])).sortByKey(False)

palavras_order.collect()