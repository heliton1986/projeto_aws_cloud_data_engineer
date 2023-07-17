/*
E11
Apresente a query para listar o código e nome cliente
 com maior gasto na loja. As colunas presentes no resultado
  devem ser cdcli, nmcli e gasto, esta última representando 
  o somatório das vendas (concluídas) atribuídas ao cliente.
*/

with tb_gasto_concluido as(

	select 
		cdcli,
		nmcli,
		sum(qtd * vrunt) as gasto
	from tbvendas  
	where status = 'Concluído' 
	group by nmcli 

)

select 
	cdcli,
	nmcli,
	gasto
from tb_gasto_concluido
where gasto = (select max(gasto) from tb_gasto_concluido) 