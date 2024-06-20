import pyodbc as sqlServer
from datetime import datetime

# Dados de conexão ao banco de dados SQL Server
servidor = "[insira o servidor do banco de dados]"
banco_dados = "[insira o nome do banco]"
usuario = "[insira o usuário de acesso ao banco]"
senha = "[insira a senha de acesso ao banco]"

def executar_update(sql, name, servidor, banco_dados, usuario, senha):

    # Conectar ao banco de dados
    conexao = sqlServer.connect('DRIVER={SQL Server};SERVER='+servidor+';DATABASE='+banco_dados+';UID='+usuario+';PWD='+senha)
    cursor = conexao.cursor()

    # Executar o script de atualização
    cursor.execute(sql)
    
    # Commit para efetivar as mudanças no banco de dados
    conexao.commit()
    
    # Fechar conexão
    conexao.close()

    #Confirmando a execução
    print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Atualização realizada com sucesso: {name}")

def executor_consulta(consulta):    
    # Conexão ao banco de dados SQL Server
    conexao = sqlServer.connect('DRIVER={SQL Server};SERVER='+servidor+';DATABASE='+banco_dados+';UID='+usuario+';PWD='+senha)

    cursor = conexao.cursor()
    
    # Executar a consulta SQL
    cursor.execute(consulta)

    # Realiza o mapeamento das colunas da consulta SQL
    colunas = [column[0] for column in cursor.description]

    # Atribui todos os dados da consulta SQL
    resultados = cursor.fetchall()

    # Fechar o cursor e a conexão
    cursor.close()
    conexao.close()

    # Laço de repetição aninhada para atribuir chave valor dos resultados da consulta SQL
    dados_formatados = [{colunas[i]: row[i] for i in range(len(colunas))} for row in resultados]

    return dados_formatados

sqlUpdate1 = "UPDATE TB_STG_MAILING SET DataEntrada = NULL FROM TB_STG_MAILING"
sqlUpdate2 = "UPDATE TB_STG_MAILING SET DataEntrada = NULL FROM TB_STG_EXPURGO_MAILING"
sqlUpdate3 = "UPDATE TB_STG_MAILING SET Multa = NULL FROM TB_STG_MAILING"
sqlUpdate4 = "UPDATE TB_STG_MAILING SET Multa = NULL FROM TB_STG_EXPURGO_MAILING"
sqlUpdate5 = "UPDATE TB_STG_MAILING SET Sinalizador = 'INAPTO ABAIXO DE 100' FROM TB_STG_MAILING WHERE ValorSaldoDevedor < 10000;"
sqlUpdate6 = "UPDATE TB_STG_EXPURGO_MAILING SET Sinalizador = 'INAPTO ABAIXO DE 100' FROM TB_STG_EXPURGO_MAILING WHERE ValorSaldoDevedor < 10000;"
sqlUpdate7 = "UPDATE TB_STG_MAILING SET Sinalizador = 'INAPTO POR MULTA' FROM TB_STG_MAILING WHERE ValorSaldoDevedor = 260000;"
sqlUpdate8 = "UPDATE TB_STG_EXPURGO_MAILING SET Sinalizador = 'INAPTO POR MULTA' FROM TB_STG_EXPURGO_MAILING WHERE ValorSaldoDevedor = 260000;"
sqlUpdate9 = "UPDATE TB_STG_MAILING SET Sinalizador = 'INAPTO POR MULTA' FROM TB_STG_MAILING WHERE ValorSaldoDevedor = 130000;"
sqlUpdate10 = "UPDATE TB_STG_EXPURGO_MAILING SET Sinalizador = 'INAPTO POR MULTA' FROM TB_STG_EXPURGO_MAILING WHERE ValorSaldoDevedor = 130000;"
sqlUpdate11 = "UPDATE TB_STG_MAILING SET Sinalizador = 'APTO' FROM TB_STG_MAILING WHERE ValorSaldoDevedor > = 10000 AND ValorSaldoDevedor != 260000 AND ValorSaldoDevedor != 130000;"
sqlUpdate12 = "UPDATE TB_STG_EXPURGO_MAILING SET Sinalizador = 'APTO' FROM TB_STG_EXPURGO_MAILING WHERE ValorSaldoDevedor > = 10000 AND ValorSaldoDevedor != 260000 AND ValorSaldoDevedor != 130000;"
sqlUpdate13 = "UPDATE TB_STG_MAILING SET NumTelefone1 = CONCAT(DDDTelefone1,Telefone1);"
sqlUpdate14 = "UPDATE TB_STG_MAILING SET NumTelefone2 = CONCAT(DDDTelefone2,Telefone2);"
sqlUpdate15 = "UPDATE TB_STG_MAILING SET NumTelefone3 = CONCAT(DDDTelefone3,Telefone3);"


sqlUpdateFaixaValor1 = '''
UPDATE TB_STG_MAILING
SET FaixaValorCPF = B.Faixa
FROM TB_STG_MAILING M
INNER JOIN(
SELECT CpfCnpjDevedor,
		CASE
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 0.00 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 50.00 THEN '1 - FAIXA  0-50'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 50.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 200.00 THEN '2 - FAIXA  50-200'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 200.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 500.00 THEN '3 - FAIXA  200-500'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 500.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 1000.00 THEN '4 - FAIXA  500-1k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 1000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 3000.00 THEN '5 - FAIXA  1k-3k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 3000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 5000.00 THEN '6 - FAIXA  3k-5k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 5000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 10000.00 THEN '7 - FAIXA  5k-10k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 10000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 30000.00 THEN '8 - FAIXA  10k-30k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 30000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 100000.00 THEN '9 - FAIXA  30k-100k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 100000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 500000.00 THEN '10 - FAIXA  100k-500k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 500000.01 THEN '11 - FAIXA  >500k'
		ELSE 'nd' END AS Faixa
FROM TB_STG_MAILING
GROUP BY CpfCnpjDevedor) B
ON M.CpfCnpjDevedor = B.CpfCnpjDevedor;
'''

sqlUpdateFaixaValor2 = '''
UPDATE TB_STG_MAILING
SET FaixaValorCPF = B.Faixa
FROM TB_STG_MAILING M
INNER JOIN(
SELECT CpfCnpjDevedor,
		CASE
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 0.00 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 50.00 THEN '1 - FAIXA  0-50'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 50.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 200.00 THEN '2 - FAIXA  50-200'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 200.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 500.00 THEN '3 - FAIXA  200-500'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 500.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 1000.00 THEN '4 - FAIXA  500-1k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 1000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 3000.00 THEN '5 - FAIXA  1k-3k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 3000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 5000.00 THEN '6 - FAIXA  3k-5k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 5000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 10000.00 THEN '7 - FAIXA  5k-10k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 10000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 30000.00 THEN '8 - FAIXA  10k-30k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 30000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 100000.00 THEN '9 - FAIXA  30k-100k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 100000.01 and SUM(CAST(ValorSaldoDevedor as float)/100) <= 500000.00 THEN '10 - FAIXA  100k-500k'
		WHEN SUM(CAST(ValorSaldoDevedor as float)/100) > = 500000.01 THEN '11 - FAIXA  >500k'
		ELSE 'nd' END AS Faixa
FROM TB_STG_MAILING
GROUP BY CpfCnpjDevedor) B
ON M.CpfCnpjDevedor = B.CpfCnpjDevedor;
'''

sqlETL = '''
SELECT *,
	CASE
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 0 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 80 THEN '1 - FAIXA 01: MENOR QUE 80'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 81 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 180 THEN '2 - FAIXA 02: 81 A 180'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 181 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 360 THEN '3 - FAIXA 03: 181 A 360'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 361 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 400 THEN '4 - FAIXA 04: 361 A 400'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 401 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 500 THEN '5 - FAIXA 05: 401 A 500'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 501 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 600 THEN '6 - FAIXA 06: 501 A 600'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 501 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 720 THEN '7 - FAIXA 07: 601 A 720'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 721 
				AND CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) < = 1080 THEN '8 - FAIXA 08: 721 A 1080'
			WHEN CAST(DATEDIFF(day, DataVencimento, getdate()) AS INT) > = 1081 THEN '9 - FAIXA 09: ACIMA DE 1080'
			ELSE 'nd' END AS TempoAtraso FROM(
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY CpfCnpjGrupoEconomico 
			ORDER BY 
				CASE 
					WHEN Sinalizador = 'APTO' THEN 1 
					WHEN Sinalizador = 'INAPTO ABAIXO DE 100' THEN 2 
					WHEN Sinalizador = 'INAPTO POR MULTA' THEN 3 
				END ASC, 
				DataVencimento ASC
		) AS RankPorData FROM(
		SELECT * FROM (
			SELECT * FROM TB_STG_MAILING
			UNION ALL
			SELECT * FROM TB_STG_EXPURGO_MAILING ) AS A
		WHERE A.Situation is null) AS RNK_APTIDAO) AS B
WHERE B.RankPorData = 1
'''

