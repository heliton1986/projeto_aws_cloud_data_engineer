/*
E14
Apresente a query para listar o gasto médio por 
estado da federação. As colunas presentes no 
resultado devem ser estado e gastomedio. 
Considere apresentar a coluna gastomedio arredondada 
na segunda casa decimal e ordenado de forma decrescente.

Observação: Apenas vendas com status concluído.
*/

--*************************************************

select 
	estado,
	qtd,
	vrunt
from tbvendas t  limit 10;

select 
	estado,
	round(avg(qtd * vrunt),2) as gastomedio
from tbvendas t 
where status = 'Concluído'
group by estado 
order by gastomedio desc;