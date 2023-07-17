# Identifique o ator/atriz com maior número de filmes e o respectivo número de filmes. 
import pandas as pd 
import numpy as np 
data = pd.read_csv('/home/heliton/Documentos/repos/novo_aws_cloud_data_engineer_compass_uol/sprint_7/tarefa1/actors.csv') 

# Identifique o ator/atriz com maior número de filmes e o respectivo número de filmes. 
m = data['Number of Movies'].idxmax() 
print(f'O ator/atriz com maior número de filmes é {data["Actor"][m]} com {data["Number of Movies"][m]} filmes\n') 

#Apresente a média da coluna contendo o número de filmes.  
print(f'A média do número de filmes é {data["Number of Movies"].mean()}\n')  

#Apresente o nome do ator/atriz com a maior média por filme. 
print(f'O ator/atriz com maior média por filme é {data.loc[data["Average per Movie"].idxmax(), "Actor"]}\n')  

# Apresente o nome do(s) filme(s) mais frequente(s) e sua respectiva frequência. 
# Ranking dos filmes de acordo com a frequência no dataset 
filmes = data["#1 Movie"].value_counts() 

for i in range(len(filmes)): 
    print(f'{i + 1} - O filme {filmes.index.values[i]} aparece {filmes.values[i]} vez(es) no dataset\n')

