import pandas as pd
import ETL_Interativo_SQL as buscador
from datetime import datetime, timedelta
import ETL_verificador_Tel_Email as verificador

# Ignorar recomendações de busca por documentação do Pandas, nos metodos .loc
pd.options.mode.chained_assignment = None

# Calcular a data de 30 dias atrás
data_hoje = datetime.today().date()
data_30_dias_atras = pd.Timestamp(data_hoje - timedelta(days=30))

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Vamos começar a tratar o mailing da [operation].")

buscador.executar_update(buscador.sqlUpdate1, "Update de limpeza da coluna DataEntrada da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate2, "Update de limpeza da coluna DataEntrada da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate3, "Update de limpeza da coluna Multa da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate4, "Update de limpeza da coluna Multa da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate5, "Update de Inapto abaixo de 100 da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate6, "Update de Inapto abaixo de 100 da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate7, "Update de Inapto por multa de 2600 da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate8, "Update de Inapto por multa de 2600 da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate9, "Update de Inapto por multa de 1300 da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate10, "Update de Inapto por multa de 1300 da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate11, "Update de Apto da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate12, "Update de Apto da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate13, "Update de NumTelefone1 da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate14, "Update de NumTelefone2 da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdate15, "Update de NumTelefone3 da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdateFaixaValor1, "Update de Faixa de Valor da tabela TB_STG_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

buscador.executar_update(buscador.sqlUpdateFaixaValor2, "Update de Faixa de Valor da tabela TB_STG_EXPURGO_MAILING", buscador.servidor, buscador.banco_dados, buscador.usuario, buscador.senha)

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Buscando base de mailing sem baixa, ordenando os APTOS e o vencimento mais antigo. Por favor, aguarde!")
# Criação de dataframe
baseBruta = pd.DataFrame(buscador.executor_consulta(buscador.sqlETL))
baseBruta['CpfCnpjGrupoEconomico'] = baseBruta['CpfCnpjGrupoEconomico'].astype(str).apply(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x))
baseBruta['CpfCnpjDevedor'] = baseBruta['CpfCnpjDevedor'].astype(str).apply(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x))
baseBruta['Cep'] = baseBruta['Cep'].astype(str).apply(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x))
baseBruta['NumeroDocumento'] = baseBruta['NumeroDocumento'].astype(str).apply(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x))

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Voltei com novidades, já vou filtrar os APTOS")
baseApta = baseBruta.loc[baseBruta['Sinalizador'] == 'APTO']
baseMulta = baseBruta.loc[baseBruta['Sinalizador'] == 'INAPTO POR MULTA']
baseAbaixoDe100 = baseBruta.loc[baseBruta['Sinalizador'] == 'INAPTO ABAIXO DE 100']

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Estou verificando os emails incorretos")
baseBruta['StatusEmail'] = baseBruta['Email1'].apply(verificador.check_email)
baseApta['StatusEmail'] = baseApta['Email1'].apply(verificador.check_email)
baseMulta['StatusEmail'] = baseMulta['Email1'].apply(verificador.check_email)
baseAbaixoDe100['StatusEmail'] = baseAbaixoDe100['Email1'].apply(verificador.check_email)

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] O mais importante agora, verificar os números")
baseBruta['VerificarNumTel1'] = baseBruta['NumTelefone1'].apply(verificador.validar_telefone)
baseBruta['VerificarNumTel2'] = baseBruta['NumTelefone2'].apply(verificador.validar_telefone)
baseBruta['VerificarNumTel3'] = baseBruta['NumTelefone3'].apply(verificador.validar_telefone)
baseApta['VerificarNumTel1'] = baseApta['NumTelefone1'].apply(verificador.validar_telefone)
baseApta['VerificarNumTel2'] = baseApta['NumTelefone2'].apply(verificador.validar_telefone)
baseApta['VerificarNumTel3'] = baseApta['NumTelefone3'].apply(verificador.validar_telefone)
baseMulta['VerificarNumTel1'] = baseMulta['NumTelefone1'].apply(verificador.validar_telefone)
baseMulta['VerificarNumTel2'] = baseMulta['NumTelefone2'].apply(verificador.validar_telefone)
baseMulta['VerificarNumTel3'] = baseMulta['NumTelefone3'].apply(verificador.validar_telefone)
baseAbaixoDe100['VerificarNumTel1'] = baseAbaixoDe100['NumTelefone1'].apply(verificador.validar_telefone)
baseAbaixoDe100['VerificarNumTel2'] = baseAbaixoDe100['NumTelefone2'].apply(verificador.validar_telefone)
baseAbaixoDe100['VerificarNumTel3'] = baseAbaixoDe100['NumTelefone3'].apply(verificador.validar_telefone)

baseBruta['NewTelefone1'] = baseBruta.apply(verificador.organizar_numeros, axis=1)
baseBruta['NewTelefone2'] = baseBruta.apply(verificador.organizar_numeros2, axis=1)
baseBruta['NewTelefone3'] = baseBruta.apply(verificador.organizar_numeros3, axis=1)
baseApta['NewTelefone1'] = baseApta.apply(verificador.organizar_numeros, axis=1)
baseApta['NewTelefone2'] = baseApta.apply(verificador.organizar_numeros2, axis=1)
baseApta['NewTelefone3'] = baseApta.apply(verificador.organizar_numeros3, axis=1)
baseMulta['NewTelefone1'] = baseMulta.apply(verificador.organizar_numeros, axis=1)
baseMulta['NewTelefone2'] = baseMulta.apply(verificador.organizar_numeros2, axis=1)
baseMulta['NewTelefone3'] = baseMulta.apply(verificador.organizar_numeros3, axis=1)
baseAbaixoDe100['NewTelefone1'] = baseAbaixoDe100.apply(verificador.organizar_numeros, axis=1)
baseAbaixoDe100['NewTelefone2'] = baseAbaixoDe100.apply(verificador.organizar_numeros2, axis=1)
baseAbaixoDe100['NewTelefone3'] = baseAbaixoDe100.apply(verificador.organizar_numeros3, axis=1)

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Eita! Tenho que caçar as tabulações que não pertence ao cliente")
baseNaoPertenceAEmpresa = baseApta.loc[baseApta['UltimoContatoCPF'] == 'Telefone Errado (Não pertence a empresa)']

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Selecionando os casos, em que ocorreu negociação")
baseUltimasNegociadas = baseApta[baseApta["UltimoContatoCPF"].isin(['Efetivou a negociação', 'Negociação', 'Efetivou a negociação a vista', 'Efetivou a negociação Parcelado', 'NEGOCIAÇÃO CONCLUÍDA A VISTA', 'NEGOCIAÇÃO CONCLUÍDA PARCIAL'])]

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Verifiquei os casos menores que 30 dias, para não cobrar novamente")
baseUltimasNegociadas['DataParaFiltragem'] = pd.to_datetime(baseUltimasNegociadas['DataUltimoContatoCPF'], format="%d/%m/%Y %H:%M:%S")

baseUltimasNegociadasMaior30dias = baseUltimasNegociadas[baseUltimasNegociadas['DataParaFiltragem'] < data_30_dias_atras]

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Selecão realizada, então, preciso limpar a base mais uma vez, filtrando aquilo que verdadeiramente importa")
baseAptaNivel2 = baseApta.loc[
    (baseApta['UltimoContatoCPF'] != 'Telefone Errado (Não pertence a empresa)') &
    (baseApta['UltimoContatoCPF'] != 'Efetivou a negociação') &
    (baseApta['UltimoContatoCPF'] != 'Negociação') &
    (baseApta['UltimoContatoCPF'] != 'Efetivou a negociação a vista') &
    (baseApta['UltimoContatoCPF'] != 'Efetivou a negociação Parcelado') &
    (baseApta['UltimoContatoCPF'] != 'NEGOCIAÇÃO CONCLUÍDA A VISTA') &
    (baseApta['UltimoContatoCPF'] != 'NEGOCIAÇÃO CONCLUÍDA PARCIAL')
]

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Fiz minha parte, agora é contigo!")

# Selecionar as colunas relevantes de cada DataFrame
df1_selected = baseApta[['CpfCnpjGrupoEconomico', 'NomeDevedor', 'Email1', 'InformacaoAdicional5', 'FaixaValorCPF', 'TempoAtraso', 'StatusEmail']]
df2_selected = baseMulta[['CpfCnpjGrupoEconomico', 'NomeDevedor', 'Email1', 'InformacaoAdicional5', 'FaixaValorCPF', 'TempoAtraso', 'StatusEmail']]
df3_selected = baseAbaixoDe100[['CpfCnpjGrupoEconomico', 'NomeDevedor', 'Email1', 'InformacaoAdicional5', 'FaixaValorCPF', 'TempoAtraso', 'StatusEmail']]

# Concatenar os DataFrames ao longo do eixo das colunas
merged_df = pd.concat([df1_selected, df2_selected, df3_selected], axis=0)

baseParaDisparoEmail = merged_df.loc[merged_df['StatusEmail'] == 'Email Válido']

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseBruta.txt")
baseBruta.to_csv('BaseBruta.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseNaoPertenceAEmpresa.txt")
baseNaoPertenceAEmpresa.to_csv('BaseNaoPertenceAEmpresa.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseUltimasNegociadas.txt")
baseUltimasNegociadas.to_csv('BaseUltimasNegociadas.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseUltimasNegociadasMaior30dias.txt")
baseUltimasNegociadasMaior30dias.to_csv('BaseUltimasNegociadasMaior30dias.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseApta.txt")
baseAptaNivel2.to_csv('BaseApta.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseMulta.txt")
baseMulta.to_csv('BaseMulta.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseAbaixoDe100.txt")
baseAbaixoDe100.to_csv('BaseAbaixoDe100.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Salvando o arquivo BaseDisparoEmail.txt")
baseParaDisparoEmail.to_csv('BaseDisparoEmail.txt', index=False, sep=';', encoding='utf-8')

print(f"[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}] Até a próxima!")
