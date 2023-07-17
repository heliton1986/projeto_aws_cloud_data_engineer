import hashlib 

while True:
    
    hash = input("Digite uma frase: " ).encode('utf-8')
    hash1 = hashlib.sha1(hash).hexdigest()
    print(hash1)

