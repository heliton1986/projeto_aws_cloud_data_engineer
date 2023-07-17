/*
E10
A comissão de um vendedor é definida a partir de um percentual 
sobre o total de vendas (quantidade * valor unitário) por ele 
realizado. O percentual de comissão de cada vendedor está 
armazenado na coluna perccomissao, tabela tbvendedor. 

Com base em tais informações, calcule a comissão de todos os 
vendedores, considerando todas as vendas armazenadas na base 
de dados com status concluído.

As colunas presentes no resultado devem ser vendedor, 
valor_total_vendas e comissao. O valor de comissão deve ser 
apresentado em ordem decrescente arredondado na segunda casa decimal.
 */

with tb_vtotal as (

select 
	vended.nmvdd as vendedor,
	sum(vendas.qtd * vendas.vrunt) valor_total_vendas,
	vended.perccomissao 
from tbvendas vendas
inner join tbvendedor vended 
	on vendas.cdvdd = vended.cdvdd 
where status = 'Concluído'
group by vended.nmvdd 

)

select 
	vendedor,
	valor_total_vendas,
	round((valor_total_vendas * perccomissao) / 100, 2) as comissao
from tb_vtotal
order by comissao desc