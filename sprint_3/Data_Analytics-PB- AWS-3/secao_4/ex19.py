'''
Calcule o valor mínimo, valor máximo, valor médio e a mediana da lista gerada na célula abaixo:


Obs.: Lembrem-se, para calcular a mediana a lista deve estar ordenada!


import random 
# amostra aleatoriamente 50 números do intervalo 0...500
random_list = random.sample(range(500),50)

Use as variáveis abaixo para representar cada operação matemática

mediana
media
valor_minimo 
valor_maximo 
'''

# %%

import random
# amostra aleatoriamente 50 números do intervalo 0...500
random_list = random.sample(range(500), 50)

# Ordenar lista
random_list_order = sorted(random_list)

# print(random_list_order)

# Use as variáveis abaixo para representar cada operação matemática
import statistics

mediana = statistics.median(random_list_order)
media = sum(random_list_order) / len(random_list_order)
valor_minimo = min(random_list_order)
valor_maximo = max(random_list_order)

print(
    f' Media: {media}, Mediana: {mediana}, Mínimo: {valor_minimo}, Máximo: {valor_maximo}')
