/*  =======================================================================
        QUERY - Pega clientes e data fidelidade 
    ======================================================================= */
WITH CLIENTES as (
    SELECT 
        IdCliente,
        NmCliente,     
        DtRefFidelidade,
        TxRamo,
        IdSituacao
              
    FROM [DATA_WAREHOUSE].[dbo].[Clientes]
    WHERE CONVERT(date, DtRefFidelidade) BETWEEN '2023-09-01' AND '2023-09-30'

),

/*  =======================================================================
        QUERY - Pega os valores do contrato de aquisição
    ======================================================================= */
VALOR as (
    SELECT 
        IdCliente,
        VlContrato,
        Convert(date, DtAtivacao) AS DtAtivacao
    FROM DATA_WAREHOUSE.dbo.ClientesServicos 
),

/*  =======================================================================
        QUERY - Pega os informação das vendas (canal, tipo, coordenador)
    ======================================================================= */
TIPO AS (
    SELECT 
        IdCliente,
        TpCanalVendas,
        TipoLead,
        CanalVendas,
        CoordenadorVendas
    FROM [DATA_WAREHOUSE].[dbo].[VendasGeral]

),

/*  =======================================================================
        QUERY - Pega a situação do cliente
    ======================================================================= */
SITUACAO AS (
    SELECT 
        IdSituacao,
        Descricao
    FROM [DATA_WAREHOUSE].[dbo].[Dim_ClienteSituacao]
)

/*  =======================================================================
        QUERY - Junta todas as tabelas
    ======================================================================= */
SELECT 
    CLIENTES.IdCliente,
    MAX(SITUACAO.Descricao)         AS Descricao,
    MAX(CLIENTES.DtRefFidelidade)   AS DtRefFidelidade,
    SUM(VALOR.VlContrato)           AS VlContrato,
    MAX(CLIENTES.TxRamo)            AS TxRamo,
    MAX(TIPO.TpCanalVendas)         AS TpCanalVendas,
    MAX(TIPO.TipoLead)              AS TipoLead,
    MAX(TIPO.CanalVendas)           AS CanalVendas,
    MAX(TIPO.CoordenadorVendas)     AS CoordenadorVendas
FROM 
    CLIENTES
INNER JOIN VALOR        ON CLIENTES.IdCliente = VALOR.IdCliente AND CLIENTES.DtRefFidelidade = VALOR.DtAtivacao
INNER JOIN TIPO         ON CLIENTES.IdCliente = TIPO.IdCliente 
INNER JOIN SITUACAO     ON CLIENTES.IdSituacao = SITUACAO.IdSituacao 

GROUP BY 
    CLIENTES.IdCliente

ORDER BY
    CLIENTES.IdCliente