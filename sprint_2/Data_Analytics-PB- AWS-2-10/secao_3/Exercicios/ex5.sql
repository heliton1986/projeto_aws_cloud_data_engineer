/*
E5
Apresente a query para listar o nome dos autores que publicaram livros 
através de editoras NÃO situadas na região sul do Brasil. Ordene o resultado 
pela coluna nome, em ordem crescente. Não podem haver nomes repetidos em seu 
retorno.
*/

SELECT 
	a.nome 
FROM autor a
inner join livro l
	on a.codautor = l.autor 
inner JOIN editora ed
	on l.editora  = ed.codeditora 
inner JOIN endereco en
	on en.codendereco = ed.endereco
WHERE en.estado not in ('RIO GRANDE DO SUL', 'PARANÁ')
GROUP BY a.nome 
ORDER BY a.nome asc 