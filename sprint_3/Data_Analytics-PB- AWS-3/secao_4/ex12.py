'''
Leia o arquivo person.json, faça o parsing e imprima seu conteúdo.

Dica: leia a documentação do pacote json
'''

# %%

import json

with open("person.json", encoding='utf-8') as meu_json:
    dados = json.load(meu_json)

print(dados)
