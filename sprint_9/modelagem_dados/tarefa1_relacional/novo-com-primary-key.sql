
-- criando nova tabela clientes inserindo Primary Key na coluna idCliente
CREATE TABLE if not exists clientes (
    idCliente INT PRIMARY KEY,
    nomeCliente VARCHAR(255),
    cidadeCliente VARCHAR(255),
    estadoCliente VARCHAR(255),
    paisCliente VARCHAR(255)
);
-- Inserindo os registros na tabela cliente com base na tabela tb_locacao
INSERT INTO clientes (idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente)
SELECT DISTINCT (idCliente), nomeCliente, cidadeCliente, estadoCliente, paisCliente
FROM tb_locacao;
-------------------------------------------

-- criando nova tabela carros inserindo Primary Key na coluna idCarro
CREATE TABLE if not exists carros (
    idCarro INT PRIMARY KEY,
    kmCarro INT,
    classiCarro VARCHAR(255),
    marcaCarro VARCHAR(255),
    modeloCarro VARCHAR(255),
    anoCarro INT
);
-- Inserindo os registros na tabela carros com base na tabela tb_locacao
INSERT OR IGNORE INTO carros (idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro, anoCarro)
SELECT idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro, anoCarro
FROM tb_locacao;
--------------------------------------------------

--criando nova tabela combustiveis inserindo Primary Key na coluna idCombustivel
CREATE TABLE if not exists combustiveis (
    idCombustivel INT PRIMARY KEY,
    tipoCombustivel VARCHAR(255)
);
--Inserindo os registros na tabela combustiveis com base na tabela tb_locacao
INSERT INTO combustiveis (idCombustivel, tipoCombustivel)
SELECT DISTINCT (idCombustivel), tipoCombustivel
FROM tb_locacao;
-----------------------------------------------------

--criando nova tabela locacao inserindo Primary Key na coluna idLocacao
-- e referenciando chaves estrangeiras das outras tabelas
CREATE TABLE if not exists locacao (
    idLocacao INT PRIMARY KEY,
    idCliente INT,
    idCarro INT,
    idCombustivel INT,
    dataLocacao DATE,
    horaLocacao TIME,
    qtdDiaria INT,
    vlrDiaria FLOAT,
    dataEntrega DATE,
    horaEntrega TIME,
    idVendedor INT,
    FOREIGN KEY (idCliente) REFERENCES clientes(idCliente),
    FOREIGN KEY (idCarro) REFERENCES carros(idCarro),
    FOREIGN KEY (idCombustivel) REFERENCES combustiveis(idCombustivel),
    FOREIGN KEY (idVendedor) REFERENCES vendedores(idVendedor)
);
--Inserindo os registros na tabela locacao com base na tabela tb_locacao
INSERT INTO locacao (idLocacao, idCliente, idCarro, idCombustivel, dataLocacao, horaLocacao, qtdDiaria, vlrDiaria, dataEntrega, horaEntrega, idVendedor)
SELECT DISTINCT idLocacao, idCliente, idCarro, idCombustivel, dataLocacao, horaLocacao, qtdDiaria, vlrDiaria, dataEntrega, horaEntrega, idVendedor
FROM tb_locacao;
-----------------------------------------

--criando nova tabela vendedores inserindo Primary Key na coluna idVendedor
CREATE TABLE if not exists vendedores (
    idVendedor INT PRIMARY KEY,
    nomeVendedor VARCHAR(255),
    sexoVendedor INT,
    estadoVendedor VARCHAR(255)
);
--Inserindo os registros na tabela vendedores com base na tabela tb_locacao
INSERT INTO vendedores (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
SELECT DISTINCT (idVendedor), nomeVendedor, sexoVendedor, estadoVendedor
FROM tb_locacao;

