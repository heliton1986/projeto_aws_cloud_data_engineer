
'''
Crie uma classe Ordenadora que contenha um atributo listaBaguncada e que contenha os métodos ordenacaoCrescente 
e ordenacaoDecrescente.

Instancie um objeto chamado crescente dessa classe Ordenadora que tenha como listaBaguncada a lista [3,4,2,1,5] 
e instancie um outro objeto, decrescente dessa mesma classe com uma outra listaBaguncada sendo [9,7,6,8].

Para o primeiro objeto citado, use o método ordenacaoCrescente e para o segundo objeto, use o método

ordenacaoDecrescente.

Imprima o resultado da ordenação crescente e da ordenação decresce

[1, 2, 3, 4, 5] 
[9, 8, 7, 6]
'''

# %%

class Ordenadora:
    def __init__(self, listaBaguncada):
        self.listaBaguncada = listaBaguncada

    def ordenacaoCrescente(self):
        self.listaBaguncada.sort()
        return self.listaBaguncada

    def ordenacaoDecrescente(self):
        self.listaBaguncada.sort(reverse=True)
        return self.listaBaguncada

listaCrescente = [3, 4, 2, 1, 5]
crescente = Ordenadora(listaCrescente)
print(crescente.ordenacaoCrescente())

listaDecrescente = [9, 7, 6, 8]
decrescente = Ordenadora(listaDecrescente)
print(decrescente.ordenacaoDecrescente())


# %%

class Ordenadora:
    def __init__(self):
        self.listaBaguncada = []


    def ordenacaoCrescente(self):
       return self.listaBaguncada.sort()

    def ordenacaoDecrescente(self):
       return self.listaBaguncada.sort(reverse=True)

crescente = Ordenadora()
crescente.listaBaguncada = [3, 4, 2, 1, 5]
crescente.ordenacaoCrescente()
print(crescente.listaBaguncada)

decrescente = Ordenadora()
decrescente.listaBaguncada = [9, 7, 6, 8]
decrescente.ordenacaoDecrescente()
print(decrescente.listaBaguncada)


# %%
