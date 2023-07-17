/*
E8
Apresente a query para listar o código e o nome do vendedor
 com maior número de vendas (contagem), e que estas vendas 
 estejam com o status concluída.  As colunas presentes no 
 resultado devem ser, portanto, cdvdd e nmvdd.
*/
with nova_tabela as (

	SELECT 
	vendedor.cdvdd,
	vendedor.nmvdd,
	count(vendas.qtd) as qtd_vendas
from tbvendedor vendedor 
join tbvendas vendas
	on vendedor.cdvdd = vendas.cdvdd  
WHERE vendas.status = 'Concluído'
GROUP BY vendedor.nmvdd  

)

select cdvdd,
	   nmvdd
from nova_tabela
where qtd_vendas = (select max(qtd_vendas) from nova_tabela)
