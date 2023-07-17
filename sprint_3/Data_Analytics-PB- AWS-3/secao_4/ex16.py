'''
Escreva uma função que recebe uma string de números separados por vírgula e retorne a soma de 
todos eles. 
Depois imprima a soma dos valores.

A string deve ter valor  "1,3,4,6,10,76"
'''
# %%

def numero(texto):

    num1 = texto.split(',')

    s = 0
    for i in num1:
    
        s = s + int(i)

    return s

numero_string = "1,3,4,6,10,76"

print(numero(numero_string))





