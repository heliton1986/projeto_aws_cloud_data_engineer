'''
Escreva uma função que recebe como parâmetro uma lista
e retorna 3 listas: a lista recebida dividida em 3 partes 
iguais. Teste sua implementação com a lista abaixo

lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
'''

# %%

def retorna3(list):

    tamanho = len(list) // 3
   
    lista1 = list[:tamanho]
    
    lista2 = list[tamanho:8]
    
    lista3 = list[8:]
    
    print(lista1, lista2, lista3)

# lista1 = []
# lista2 = []
# lista3 = []

lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

retorna3(lista)


# %%


def retorna3(list):

    for num in list[:4]:
        # print(num)
        lista1.append(num)

    for num in list[4:8]:
        # print(num)
        lista2.append(num)

    for num in list[8:]:
        # print(num)
        lista3.append(num)

    print(lista1, lista2, lista3)


lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
lista1 = []
lista2 = []
lista3 = []

retorna3(lista)

