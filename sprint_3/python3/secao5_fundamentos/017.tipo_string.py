# %%
dir(str)
nome = 'Saulo Pedro'
nome
print(nome[0])
# nome[0] = 'P'

# 'marca d'água'
"Dias D'Avila" == 'Dias D\'Avila'
"Teste \" funciona!"
texto = 'Texto entre apostrófos pode ter "aspas"'
texto

doc = """Texto com múltiplas
    ... linhas"""
doc
print('Texto com múltiplas\n\t... linhas')
print(doc)

doc2 = '''Também é possível
... com 3 aspas simples'''
doc2

# %%
nome = 'Ana Paula'
print(nome[0])
print(nome[6])
print(nome[-3])
print(nome[4:])
print(nome[-5:])
print(nome[:3])
print(nome[2:5])

numeros = '1234567890'
print(numeros)
print(numeros[::])
print(numeros[::2])
print(numeros[1::2])
print(numeros[::-1])
print(numeros[::-2])

nome[::-1]
print(nome[-5:])
print(nome[2:5])
# %%
frase = 'Python é uma linguagem excelente'
'py' not in frase
'ing' in frase
len(frase)
frase.lower()
frase
frase = frase.upper()
frase

print(frase.split())
print(frase.split('E'))

# dir(str)
# help(str.center)

# %%
a = '123'
b = ' de Oliveira 4'
a + b
a.__add__(b)
str.__add__(a, b)

len(a)
a.__len__()
'1' in a
a.__contains__('1')

dir(str)
