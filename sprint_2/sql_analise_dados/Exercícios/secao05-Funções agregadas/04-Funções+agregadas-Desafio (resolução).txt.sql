-- EXERCÍCIOS ########################################################################

-- (Exercício 1) Conte quantos clientes da tabela sales.customers tem menos de 30 anos

select *
from sales.customers c 

select birth_date ,
	   (current_date - birth_date) / 365
from sales.customers c 



select birth_date ,
	   (current_date - birth_date) / 365
from sales.customers c 
where ((current_date - birth_date) / 365) < 30 

select count(*)
from sales.customers
where ((current_date - birth_date) / 365 ) < 30

-- (Exercício 2) Informe a idade do cliente mais velho e mais novo da tabela sales.customers

select birth_date 
from sales.customers c 

select 
	   max((current_date - birth_date) / 365) as mais_velho,
	   min((current_date - birth_date) / 365) as mais_novo
from sales.customers c  

-- (Exercício 3) Selecione todas as informações do cliente mais rico da tabela sales.customers
-- (possívelmente a resposta contém mais de um cliente)

select 
	   max(income) as mais_rico
from sales.customers c 

select first_name , income 
from sales.customers c 
where income = (select max(income) as mais_rico from sales.customers c )



-- (Exercício 4) Conte quantos veículos de cada marca tem registrado na tabela sales.products
-- Ordene o resultado pelo nome da marca

select *
from sales.products p 

select 
	brand,
	count(*)
from sales.products p 
group by brand 
order by brand asc

-- (Exercício 5) Conte quantos veículos existem registrados na tabela sales.products
-- por marca e ano do modelo. Ordene pela nome da marca e pelo ano do veículo

select 
	brand,
	model_year,
	count(*) as qtd
from sales.products p 
group by brand, model_year 
order by brand, model_year 
 

-- (Exercício 6) Conte quantos veículos de cada marca tem registrado na tabela sales.products
-- e mostre apenas as marcas que contém mais de 10 veículos registrados

select *
from sales.products p 

select 
	brand,
	count(*) as qtd
from sales.products p 
group by brand 
having count(*) > 10 

