/*
E6
Apresente a query para listar o autor com maior número de livros publicados.
 O resultado deve conter apenas as colunas codautor, nome, quantidade_publicacoes.
*/

SELECT 
	a.codautor,
	a.nome,
	COUNT(l.publicacao) as quantidade_publicacoes
FROM autor a 
inner join livro l 
	on a.codautor = l.autor 
GROUP BY a.nome 
ORDER BY quantidade_publicacoes DESC 
limit 1;
