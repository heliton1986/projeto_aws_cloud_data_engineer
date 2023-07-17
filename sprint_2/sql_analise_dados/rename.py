import os

folder = r"/home/heliton/Documentos/repos/aws_cloud_data_engineer_compass_uol/sprint_2/sql_analise_dados/secao_2_ambiente_trabalho/material/Exercícios/02-Operadores/"

for file_name in os.listdir(folder):
    old_name = folder + file_name
    new_name = folder + file_name + ".sql"
    os.rename(old_name, new_name)

print(os.listdir(folder))
