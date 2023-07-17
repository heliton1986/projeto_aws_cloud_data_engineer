'''
Escreva uma função que recebe um número variável de parâmetros não nomeados e um número variado de 
parâmetros nomeados e 
imprime o valor de cada parâmetro recebido.

Teste sua função com os seguintes parâmetros:

(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)
'''

def funcao(x1,x2,x3, txt, txt1='algumacoisa', x=20):
    
    return print(f'{x1}\n{x2}\n{x3}\n{txt}\n{txt1}\n{x}')

funcao(1,3,4,'hello')