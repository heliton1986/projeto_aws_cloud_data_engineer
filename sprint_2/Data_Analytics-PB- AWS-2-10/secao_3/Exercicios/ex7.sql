/*
E7
Apresente a query para listar o nome dos autores com nenhuma publicação. 
Apresentá-los em ordem crescente.
*/

SELECT a.nome
FROM autor a  
full join livro l 
	on a.codautor = l.autor 
WHERE l.publicacao ISNULL  
GROUP BY a.nome 
ORDER BY a.nome 

