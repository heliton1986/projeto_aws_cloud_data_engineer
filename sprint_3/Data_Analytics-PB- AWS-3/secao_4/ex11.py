'''
Escreva um programa que lê o conteúdo do arquivo texto arquivo_texto.txt e imprime o seu conteúdo.

Dica: leia a documentação da função open(...)

'''

texto = open('arquivo_texto.txt', 'w+')
texto.write('Este conteúdo está em\n')
texto.write('um\n')
texto.write('arquivo\n')
texto.write('de texto.')

texto.seek(0,0)
print(texto.read(), end='')



