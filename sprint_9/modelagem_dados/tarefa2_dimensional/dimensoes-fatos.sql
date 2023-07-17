--view dim_cliente 
CREATE VIEW dim_cliente AS
SELECT c.idCliente, 
	   c.nomeCliente, 
	   c.cidadeCliente, 
	   c.estadoCliente, 
	   c.paisCliente
FROM clientes c;
----------------------------

--View dim_carro 
CREATE VIEW dim_carro AS
SELECT idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro, anoCarro
FROM carros;
-------------------------------------------

--view dim_combustivel 
CREATE VIEW dim_combustivel AS
SELECT idCombustivel, tipoCombustivel
FROM combustiveis;
---------------------------------

--view dim_vendedor 
CREATE VIEW dim_vendedor AS
SELECT idVendedor, nomeVendedor, sexoVendedor, estadoVendedor
FROM vendedores;
------------------------------------

--view fact_locacao 
CREATE VIEW fact_locacao AS
SELECT l.idLocacao,
	   l.idCliente, 
	   l.idCombustivel,  
	   l.idCarro, 
	   l.idVendedor, 
	   l.qtdDiaria,
	   l.vlrDiaria,
	   date(substr(l.dataLocacao, 1, 4) || '-' || substr(l.dataLocacao, 5, 2) || '-' || substr(l.dataLocacao, 7, 2)) as dataLocacao,
	   date(substr(l.dataEntrega , 1, 4) || '-' || substr(l.dataEntrega, 5, 2) || '-' || substr(l.dataEntrega, 7, 2)) as dataEntrega
FROM locacao l
JOIN clientes c ON l.idCliente = c.idCliente
JOIN carros ca ON l.idCarro = ca.idCarro
JOIN vendedores v ON l.idVendedor = v.idVendedor
JOIN combustiveis com ON l.idCombustivel = com.idCombustivel;
-------------------------------------------

--view dim_tempo com datas e horas de locação e entrega
CREATE VIEW dim_tempo AS
SELECT DISTINCT 
    substr(l.dataLocacao, 1, 4) || '-' || substr(l.dataLocacao, 5, 2) || '-' || substr(l.dataLocacao, 7, 2) as dataLocacao,
    substr(l.dataLocacao, 1, 4) as anoLocacao,
    substr(l.dataLocacao, 5, 2) as mesLocacao,
    substr(l.dataLocacao, 7, 2) as diaLocacao,
    l.horaLocacao as horaLocacao,
    substr(l.dataEntrega , 1, 4) || '-' || substr(l.dataEntrega, 5, 2) || '-' || substr(l.dataEntrega, 7, 2) as dataEntrega,
    substr(l.dataEntrega, 1, 4) as anoEntraga,
    substr(l.dataEntrega, 5, 2) as mesEntrega,
    substr(l.dataEntrega, 7, 2) as diaEntrega,
    l.horaEntrega  as horaEntrega
FROM locacao l;





