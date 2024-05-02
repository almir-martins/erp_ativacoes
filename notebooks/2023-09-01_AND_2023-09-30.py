# ===================================================================
# 1 - Imports
# ===================================================================
import os
import warnings
import openpyxl
import pyodbc
import numpy                    as np
import pandas                   as pd
import seaborn                  as sns
import matplotlib.pyplot        as plt

warnings.filterwarnings('ignore')

# ===================================================================
# 2 - Funções de apoio
# ===================================================================
# Configura o notebook
def jupyter_settings():
    import warnings
    warnings.filterwarnings('ignore')
    # %matplotlib inline

    # Tamanho e estilo dos gráficos
    plt.style.use('bmh')
    plt.rcParams['figure.figsize'] = [22, 9]
    plt.rcParams['font.size'] = 21

    # Configuração de exibição das linhas e colunas do pandas
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    pd.set_option('display.expand_frame_repr', False)

    # configuração do pandas para quantidade de casas decimais
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    sns.set()

def data_inicial_final():
    # Pega o nome do arquivo
    NAME_PY = (os.path.basename(__file__))
    # Retira o .py
    NAME = NAME_PY.split('.')[0]
    # Retira os underlines
    DATAS = NAME.split('_')
    # Junta organizadamente (data_inicial + AND + data_final)
    QUERY_PART = f"'{DATAS[0]}' {DATAS[1]} '{DATAS[2]}'"

    return QUERY_PART


# ===================================================================
# 3 - Aquisição dos dados
## 3.1 - Conexão com o banco
# ===================================================================
# Credenciais fornecidas
connection = pyodbc.connect( 
    'Driver={SQL Server};'
    'Server=10.67.99.10\MSSQLSERVER2019;'
    'Database=DATA_WAREHOUSE;'
    'UID=userCustomerServices;'
    'PWD=userCustomerServices@ADT_2023*;'
    'Trusted_Connection=no;')

# ===================================================================    
## 3.2 - Queries
# ===================================================================
QUERY1 = '''
/*  =======================================================================
        QUERY - Pega clientes e data fidelidade 
    ======================================================================= */
WITH CLIENTES as (
    SELECT 
        IdCliente,
        NmCliente,     
        DtRefFidelidade,
        TxRamo
              
    FROM [DATA_WAREHOUSE].[dbo].[Clientes]
    WHERE CONVERT(date, DtRefFidelidade) BETWEEN '''
    
    #'2023-09-01' AND '2023-09-30'

QUERY2 = '''    
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

TIPO AS (
    SELECT 
        IdCliente,
        TpCanalVendas,
        TipoLead,
        CanalVendas,
        CoordenadorVendas
    FROM [DATA_WAREHOUSE].[dbo].[VendasGeral]

)

SELECT 
    CLIENTES.IdCliente,
    MAX(CLIENTES.DtRefFidelidade)   AS DtRefFidelidade,
    SUM(VALOR.VlContrato)           AS VlContrato,
    MAX(CLIENTES.TxRamo)            AS TxRamo,
    MAX(TIPO.TpCanalVendas)         AS TpCanalVendas,
    MAX(TIPO.TipoLead)              AS TipoLead,
    MAX(TIPO.CanalVendas)           AS CanalVendas,
    MAX(TIPO.CoordenadorVendas)     AS CoordenadorVendas
FROM 
    CLIENTES
INNER JOIN VALOR    ON CLIENTES.IdCliente = VALOR.IdCliente AND CLIENTES.DtRefFidelidade = VALOR.DtAtivacao
INNER JOIN TIPO     ON CLIENTES.IdCliente = TIPO.IdCliente 

GROUP BY 
    CLIENTES.IdCliente

ORDER BY
    CLIENTES.IdCliente
'''
QUERY = QUERY1 + data_inicial_final() + QUERY2
# print(QUERY)


# ===================================================================
## 3.3 - Pegando os dados do banco
# ===================================================================
df = pd.read_sql(QUERY, connection)
connection.close()

df.TxRamo = df.TxRamo.apply(lambda x: 'Residencial' if x=='CONDOMINIAL' else str.capitalize(x))

# ===================================================================
## 3.4 - Pegando dados de metas (planilha)
# ===================================================================
metas = pd.read_excel('../dados/METAS.xlsx')

# ===================================================================
## 3.5 - Criando a base de atingimento por vendedor
# ===================================================================
# Agrupa para pegar a contagem de valores
df_faixa_vendedor = df[["CanalVendas", "IdCliente"]].groupby("CanalVendas").count().reset_index()

# Renomeia as colunas
df_faixa_vendedor.columns = ["Vendedor", "Vendas"]

# Junta os dois dataframes
df_faixa_vendedor = pd.merge(df_faixa_vendedor, metas, left_on="Vendedor", right_on="Canal de Venda", how="left").drop(columns=["Canal de Venda"])

# Cria coluna de Atingimento
df_faixa_vendedor["Atingimento"] = df_faixa_vendedor.Vendas / df_faixa_vendedor.Meta
df_faixa_vendedor["Atingimento"].fillna(0, inplace=True)

# Define a coluna de faixas de valores
df_faixa_vendedor["Faixa"] = df_faixa_vendedor.Atingimento.apply(
    lambda x: "Meta não informada"
    if x==0
    else "< 59,99%"
    if x < 0.6
    else "< 69,99%"
    if x < 0.7
    else "< 79,99%"
    if x < 0.8
    else "< 89,99%"
    if x < 0.9
    else "< 99,99%"
    if x < 1
    else "< 109,99%"
    if x < 1.1
    else "< 119,99%"
    if x < 1.2
    else "< 129,99%"
    if x < 1.3
    else "< 139,99%"
    if x < 1.4
    else "< 79,99%"
)

# Define a coluna de faixas de valores (somente MPP)
df_faixa_vendedor["MPP"] = df_faixa_vendedor.Vendas.apply(
    lambda x: "< 11"
    if x < 11
    else "< 21"
    if x < 21
    else "< 31"
    if x < 31
    else "< 61"
    if x < 61
    else "< 81"
    if x < 81
    else "< 131"
    if x < 131
    else "< 150"
    if x < 150
    else ">= 150"
)

# Junta a tabela de dados com a tabela de faixas
df_dados_faixa = df.merge(df_faixa_vendedor, left_on='CanalVendas', right_on='Vendedor', how='inner').drop(columns=['Vendedor'])

# ===================================================================
# 4 - Cálculo do fator
## 4.1 - Cálculo do vendedor
# ===================================================================
faixas_percentuais_vendedor = {
    "Venda InternaTrabalho PróprioResidencial< 59,99%": 0.5,
    "Venda InternaTrabalho PróprioResidencial< 69,99%": 0.7,
    "Venda InternaTrabalho PróprioResidencial< 79,99%": 0.8,
    "Venda InternaTrabalho PróprioResidencial< 89,99%": 0.9,
    "Venda InternaTrabalho PróprioResidencial< 99,99%": 1,
    "Venda InternaTrabalho PróprioResidencial< 109,99%": 2,
    "Venda InternaTrabalho PróprioResidencial< 119,99%": 2.3,
    "Venda InternaTrabalho PróprioResidencial< 129,99%": 2.6,
    "Venda InternaTrabalho PróprioResidencial< 139,99%": 2.9,
    "Venda InternaTrabalho PróprioResidencial> 140": 3.2,
    "Venda InternaTrabalho PróprioEmpresarial< 59,99%": 0.3,
    "Venda InternaTrabalho PróprioEmpresarial< 69,99%": 0.5,
    "Venda InternaTrabalho PróprioEmpresarial< 79,99%": 0.6,
    "Venda InternaTrabalho PróprioEmpresarial< 89,99%": 0.7,
    "Venda InternaTrabalho PróprioEmpresarial< 99,99%": 0.8,
    "Venda InternaTrabalho PróprioEmpresarial< 109,99%": 1,
    "Venda InternaTrabalho PróprioEmpresarial< 119,99%": 1.4,
    "Venda InternaTrabalho PróprioEmpresarial< 129,99%": 1.8,
    "Venda InternaTrabalho PróprioEmpresarial< 139,99%": 2.2,
    "Venda InternaTrabalho PróprioEmpresarial> 140": 2.5,
    "Venda InternaLead< 59,99%": 0.1,
    "Venda InternaLead< 69,99%": 0.2,
    "Venda InternaLead< 79,99%": 0.3,
    "Venda InternaLead< 89,99%": 0.4,
    "Venda InternaLead< 99,99%": 0.5,
    "Venda InternaLead< 109,99%": 0.6,
    "Venda InternaLead< 119,99%": 0.8,
    "Venda InternaLead< 129,99%": 1,
    "Venda InternaLead< 139,99%": 1.2,
    "Venda InternaLead> 140": 1.5,
    "TelevendasTrabalho PróprioResidencial< 59,99%": 0.3,
    "TelevendasTrabalho PróprioResidencial< 69,99%": 0.5,
    "TelevendasTrabalho PróprioResidencial< 79,99%": 0.6,
    "TelevendasTrabalho PróprioResidencial< 89,99%": 0.7,
    "TelevendasTrabalho PróprioResidencial< 99,99%": 0.8,
    "TelevendasTrabalho PróprioResidencial< 109,99%": 1,
    "TelevendasTrabalho PróprioResidencial< 119,99%": 1.4,
    "TelevendasTrabalho PróprioResidencial< 129,99%": 1.8,
    "TelevendasTrabalho PróprioResidencial< 139,99%": 2.2,
    "TelevendasTrabalho PróprioResidencial> 140": 2.5,
    "TelevendasTrabalho PróprioEmpresarial< 59,99%": 0.3,
    "TelevendasTrabalho PróprioEmpresarial< 69,99%": 0.5,
    "TelevendasTrabalho PróprioEmpresarial< 79,99%": 0.6,
    "TelevendasTrabalho PróprioEmpresarial< 89,99%": 0.7,
    "TelevendasTrabalho PróprioEmpresarial< 99,99%": 0.8,
    "TelevendasTrabalho PróprioEmpresarial< 109,99%": 1,
    "TelevendasTrabalho PróprioEmpresarial< 119,99%": 1.4,
    "TelevendasTrabalho PróprioEmpresarial< 129,99%": 1.8,
    "TelevendasTrabalho PróprioEmpresarial< 139,99%": 2.2,
    "TelevendasTrabalho PróprioEmpresarial> 140": 2.5,
    "TelevendasLead< 59,99%": 0.3,
    "TelevendasLead< 69,99%": 0.5,
    "TelevendasLead< 79,99%": 0.6,
    "TelevendasLead< 89,99%": 0.7,
    "TelevendasLead< 99,99%": 0.8,
    "TelevendasLead< 109,99%": 1,
    "TelevendasLead< 119,99%": 1.4,
    "TelevendasLead< 129,99%": 1.8,
    "TelevendasLead< 139,99%": 2.2,
    "TelevendasLead> 140": 2.5,
    "MPP< 11": 3,
    "MPP< 21": 3.5,
    "MPP< 31": 4,
    "MPP< 61": 4,
    "MPP< 81": 4,
    "MPP< 131": 4.5,
    "MPP< 150": 4.5,
    "MPP>= 150": 4.5,
}

# Adequando a coluna TxRamo para omitir os dados caso seja MPP ou Lead
df_dados_faixa.TxRamo = df_dados_faixa.apply(lambda x: '' if (x.TpCanalVendas == 'MPP') | (x.TipoLead == 'Lead') else x.TxRamo, axis=1)

# Adequando a coluna TpCanalVendas para omitir o valor 'Corporate'
df_dados_faixa.TpCanalVendas = df_dados_faixa.TpCanalVendas.apply(lambda x: '' if x == 'Corporate' else x)


# Calculando o percentual da comissão
df_dados_faixa["Comissão %"] = df_dados_faixa.apply(
    lambda x: faixas_percentuais_vendedor[x.TpCanalVendas + x.MPP]
    if x.TpCanalVendas == "MPP"
    else 0
    if (x.CanalVendas == "CORPORATIVO - ADT") | (x.TpCanalVendas == 'Supervisor') | (x.TpCanalVendas == 'E-Commerce') | (x.Faixa == "Meta não informada")
    else faixas_percentuais_vendedor[x.TpCanalVendas + x.TipoLead + x.TxRamo + x.Faixa],
    axis=1,
)

# Calculando o valor da comissão
df_dados_faixa['Comissão Valor'] = df_dados_faixa['VlContrato'] * df_dados_faixa['Comissão %']


# Mesclando a planilha de campanha
df_campanha = pd.read_excel('../dados/CAMPANHA.xlsx')
df_final_vendedor = df_dados_faixa.merge(df_campanha, left_on='CanalVendas', right_on='Parceiro', how='left').drop(columns=['Parceiro'])
df_final_vendedor['Termo Aditivo'].fillna('Não', inplace=True)

# Calculando campanha
df_final_vendedor['Valor Campanha'] = df_final_vendedor.apply(lambda x: 0 if x['Termo Aditivo'] == 'Não' else 3 * x['VlContrato'], axis=1)

# Calculando valor total
df_final_vendedor['Valor Comissão Vendedor'] = df_final_vendedor['Valor Campanha'] + df_final_vendedor['Comissão Valor']

# ===================================================================
## 4.2 - Cálculo do Supervisor
# ===================================================================
# Encontrar os vendedores e as respectivas metas, agrupados por supervisor
df_metas = df_final_vendedor[['CanalVendas', 'CoordenadorVendas', 'Meta']].groupby(['CanalVendas', 'CoordenadorVendas']).max().reset_index()
# Somar as metas individuais dos vendedores, por supervisor
df_metas = df_metas[['CoordenadorVendas', 'Meta']].groupby('CoordenadorVendas').sum().reset_index()
# Renomear colunas
df_metas.columns = ['CoordenadorVendas',	'Meta Supervisor']

# Encontrar os vendedores e as respectivas vendas, agrupados por supervisor
df_vendas = df_final_vendedor[['CanalVendas', 'CoordenadorVendas', 'Vendas']].groupby(['CanalVendas', 'CoordenadorVendas']).max().reset_index()
# Somar as vendas individuais dos vendedores, por supervisor
df_vendas = df_vendas[['CoordenadorVendas', 'Vendas']].groupby('CoordenadorVendas').sum().reset_index()
# Renomear colunas
df_vendas.columns = ['CoordenadorVendas',	'Vendas Supervisor']

# Juntando as tabelas
df_metas = df_vendas.merge(df_metas, on='CoordenadorVendas', how='inner')

# Juntando a meta do supervisor ao aos demais dados
df_final_supervisor = df_final_vendedor.merge(df_metas, on='CoordenadorVendas', how='inner')

# valor
df_final_supervisor['Atingimento Supervisor'] = np.where((df_final_supervisor['Meta Supervisor'] == 0), 0, df_final_supervisor['Vendas Supervisor'] / df_final_supervisor['Meta Supervisor'])

# Define a coluna de faixas de valores para os supervisores
df_final_supervisor["Faixa Supervisor"] = df_final_supervisor['Atingimento Supervisor'].apply(
    lambda x: "< 49,99%"
    if x < .5
    else "< 59,99%"
    if x < 0.6
    else "< 69,99%"
    if x < 0.7
    else "< 79,99%"
    if x < 0.8
    else "< 89,99%"
    if x < 0.9
    else "< 99,99%"
    if x < 1
    else "< 109,99%"
    if x < 1.1
    else "< 119,99%"
    if x < 1.2
    else "< 129,99%"
    if x < 1.3
    else "> 130%"
)

# Define a coluna de faixas de valores para os Supervisores (somente MPP)
df_final_supervisor["MPP Supervisor"] = df_final_supervisor['Atingimento Supervisor'].apply(
    lambda x: "< 79,99%"
    if x < 0.8
    else "< 89,99%"
    if x < 0.9
    else "< 99,99%"
    if x < 1
    else "< 109,99%"
    if x < 1.1
    else "< 119,99%"
    if x < 1.2
    else "< 129,99%"
    if x < 1.3
    else "< 139,99%"
    if x < 1.4
    else '> 140%'
)

df_final_supervisor['faixa concatenada'] = df_final_supervisor.apply(lambda x: x['MPP Supervisor'] if x['TpCanalVendas'] == 'MPP' else x['Faixa Supervisor'], axis=1)

faixas_percentuais_supervisor = {
    "Venda Interna< 49,99%": 0,
    "Venda Interna< 59,99%": 0.04,
    "Venda Interna< 69,99%": 0.06,
    "Venda Interna< 79,99%": 0.07,
    "Venda Interna< 89,99%": 0.08,
    "Venda Interna< 99,99%": 0.09,
    "Venda Interna< 109,99%": 0.12,
    "Venda Interna< 119,99%": 0.14,
    "Venda Interna< 129,99%": 0.15,
    "Venda Interna> 130%": 0.17,
    "Televendas< 49,99%": 0,
    "Televendas< 59,99%": 0.04,
    "Televendas< 69,99%": 0.06,
    "Televendas< 79,99%": 0.07,
    "Televendas< 89,99%": 0.08,
    "Televendas< 99,99%": 0.09,
    "Televendas< 109,99%": 0.12,
    "Televendas< 119,99%": 0.14,
    "Televendas< 129,99%": 0.15,
    "Televendas> 130%": 0.17,
    "MPP< 79,99%": 0,
    "MPP< 89,99%": 0.06,
    "MPP< 99,99%": 0.1,
    "MPP< 109,99%": 0.16,
    "MPP< 119,99%": 0.19,
    "MPP< 129,99%": 0.22,
    "MPP< 1399,99%": 0.25,
    "MPP> 140%": 0.3,
}

#  Calculando o percentual da comissão
df_final_supervisor['Comissão % Supervisor'] = df_final_supervisor.apply(
    lambda x: faixas_percentuais_supervisor[ x['TpCanalVendas'] + x['faixa concatenada'] ]
    if x.TpCanalVendas not in ["E-Commerce", 'Corporate', 'Supervisor']
    else 0,    
    axis=1
)

# Calculando o valor da comissão
df_final_supervisor['Valor Comissão Supervisor'] = df_final_supervisor['Comissão % Supervisor'] * df_final_supervisor['VlContrato']

df_final = df_final_supervisor.copy()

# ===================================================================
# 5 - Gerando relatórios
# 5.1 - Agrupando os dados por vendedor/supervisor
# ===================================================================
# Gera o relatório por Vendedor
df_vendedor_agrupado = df_final_supervisor[['CanalVendas', 'Valor Comissão Vendedor']].groupby('CanalVendas').sum().reset_index()

# Gera o relatório por Supervisor
df_supervisor_agrupado = df_final_supervisor[['CoordenadorVendas', 'Valor Comissão Supervisor']].groupby('CoordenadorVendas').sum().reset_index()

# ===================================================================
# 5 - Exportando relatórios como xlsx
# ===================================================================
# Gera os relatórios geral e agrupados
df_supervisor_agrupado.to_excel('../dados/relatorios/Comissão Supervisor.xlsx', index=False)
df_vendedor_agrupado.to_excel('../dados/relatorios/Comissão Vendedor.xlsx', index=False)
df_final.to_excel('../dados/relatorios/Planilha Geral.xlsx', index=False)