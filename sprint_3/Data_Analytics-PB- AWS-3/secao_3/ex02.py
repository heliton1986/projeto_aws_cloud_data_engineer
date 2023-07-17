# %%
# Escreva um código Python para verificar se três números digitados na entrada padrão são pares ou ímpares. 
# Para cada número, imprima como saída Par: ou Ímpar: e o número correspondente (um linha para cada número lido).

# Importante: Aplique a função range() em seu código.



# Exemplos de saída:

# Par: 2
# Ímpar: 3

for numero in range(1,4):
    numero = int(input("Digite o número: "))
    
    if numero % 2 == 0:
        print("Par:", numero)
    else:
        print("Ímpar:", numero)
        






# %%
