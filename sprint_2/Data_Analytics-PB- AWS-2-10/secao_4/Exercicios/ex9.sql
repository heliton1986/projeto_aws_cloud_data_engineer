/*
E9
Apresente a query para listar o código e nome do produto 
mais vendido entre as datas de 2014-02-03 até 2018-02-02, 
e que estas vendas estejam com o status concluída. 
As colunas presentes no resultado devem ser cdpro e nmpro.
*/

with tb_agrupar_concluido as (

	select 
		cdpro,
		nmpro,
		count(qtd) as qtd_vendas
	from tbvendas 
	where status = 'Concluído' and (dtven BETWEEN '2014-02-03' and '2018-02-02')
	group by nmpro

)  

select 
	cdpro,
	nmpro
from tb_agrupar_concluido
where qtd_vendas = (select max(qtd_vendas) from tb_agrupar_concluido)