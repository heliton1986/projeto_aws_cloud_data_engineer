'''
mplemente duas classes Pato e Pardal que herdem de uma classe Passaro a habilidade de voar e emitir som, porém, tanto Pato quanto Pardal devem emitir sons diferentes (de maneira escrita) no console.



Imprima no console exatamente assim:

Pato
Voando...
Pato emitindo som...
Quack Quack
Pardal
Voando...
Pardal emitindo som...
Piu Piu
'''

class Passaro():
  def voar(self):
    
    print("Voando...")

  def emitir_som(self):
    print("Passaro emitindo som...")

class Pato(Passaro):
  def emitir_som(self):
    print("Pato emitindo som...")
    print("Quack Quack")

class Pardal(Passaro):
  def emitir_som(self):
    print("Pardal emitindo som...")
    print("Piu Piu")

print('Pato')
pato = Pato()
pato.voar()
pato.emitir_som()

pardal = Pardal()
pardal.voar()
pardal.emitir_som()


