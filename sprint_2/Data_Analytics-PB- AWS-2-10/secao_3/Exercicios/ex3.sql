/*
E3
 Apresente a query para listar as 5 editoras
  com mais livros na biblioteca. O resultado 
  deve conter apenas as colunas quantidade, 
  nome, estado e cidade. Ordenar as linhas 
  pela coluna que representa a quantidade de 
  livros em ordem decrescente.
*/

SELECT 
	COUNT(l.titulo) as quantidade, 
	ed.nome, 
	en.estado, 
	en.cidade  
FROM editora ed
inner join livro l 
	on ed.codeditora = l.editora 
inner join endereco en 
	on ed.endereco = en.codendereco 
GROUP BY ed.codeditora  
order by l.titulo asc
limit 5;