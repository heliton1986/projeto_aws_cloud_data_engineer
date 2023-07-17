/*
E12
Apresente a query para listar código, nome e 
data de nascimento dos dependentes do vendedor 
com menor valor total bruto em vendas (não sendo zero). 
As colunas presentes no resultado devem ser cddep, nmdep, 
dtnasc e valor_total_vendas.


Observação: Apenas vendas com status concluído.
*/

with tb_valor_total_bruto as (

	select 
		dep.cddep,
		dep.nmdep,
		dep.dtnasc,
		sum(vendas.qtd * vendas.vrunt) as valor_total_vendas
	from 
		tbdependente dep
	inner join 
		tbvendedor vendedor 
			on dep.cdvdd = vendedor.cdvdd 
	inner join
		tbvendas vendas
			on vendedor.cdvdd = vendas.cdvdd 
	where vendas.status = 'Concluído' 
	group by dep.cddep 
	having valor_total_vendas <> 0

)

select 
	cddep,
	nmdep,
	dtnasc,
	valor_total_vendas
from tb_valor_total_bruto
where valor_total_vendas = (select min(valor_total_vendas) from tb_valor_total_bruto)