/*
 E4
Apresente a query para listar a quantidade de livros publicada por cada autor.
 Ordenar as linhas pela coluna nome (autor), em ordem crescente. Além desta, 
 apresentar as colunas codautor, nascimento e quantidade (total de livros 
 de sua autoria).
 */
 
SELECT 
	a.nome as nome,
	a.codautor,
	a.nascimento, 
	count(l.autor) as quantidade
from autor a
full join livro l
	on a.codautor  = l.autor  
group by a.nome 
order by a.nome asc