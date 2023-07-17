/*
E15
Apresente a query para listar os códigos das vendas 
identificadas como deletadas. Apresente o resultado 
em ordem crescente.
*/

select * from tbvendas t 

select 
	cdven 
FROM 
	tbvendas t 
where deletado = '1'
order by cdven asc