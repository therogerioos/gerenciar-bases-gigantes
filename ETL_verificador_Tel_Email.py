from ETL_blacklist_emails import blacklist_emails

def check_email(email):
    if email == '-':
        return 'Email sem enriquecimento'
    elif email in blacklist_emails:
        return 'Email precisa de enriquecimento'
    else:
        return 'Email Válido'
    

def validar_telefone(telefone):
    if telefone is None:
        return 'Sem Telefone'
    elif len(telefone) == 11:
        numero = telefone[2:]
        if telefone[2] == '9': 
            numero_sem_nonodigito = numero[1:]
            if len(set(numero_sem_nonodigito)) > 2:
                return 'Tel Celular - Correto'
            else:
                return 'Tel Celular - Incorreto'
        else:
            return 'Tel Celular - Incorreto'
    elif len(telefone) == 10:
        numero = telefone[2:]
        if telefone[2] in ['2', '3', '4', '5'] and len(set(numero)) > 2:
            return 'Tel Fixo - Correto'
        else:
            return 'Tel Incorreto'
    else:
        return 'Sem Telefone'
    

def organizar_numeros(row):
    if row['VerificarNumTel1'] == 'Tel Celular - Correto':
        return row['NumTelefone1']
    elif row['VerificarNumTel2'] == 'Tel Celular - Correto':
        return row['NumTelefone2']
    elif row['VerificarNumTel3'] == 'Tel Celular - Correto':
        return row['NumTelefone3']
    elif row['VerificarNumTel1'] == 'Tel Fixo - Correto':
        return row['NumTelefone1']
    elif row['VerificarNumTel2'] == 'Tel Fixo - Correto':
        return row['NumTelefone2']
    elif row['VerificarNumTel3'] == 'Tel Fixo - Correto':
        return row['NumTelefone3']
    else:
        return None  # ou qualquer valor padrão que você queira atribuir se nenhum número for encontrado
    
def organizar_numeros2(row):
    if row['VerificarNumTel2'] == 'Tel Celular - Correto' and row['NumTelefone2'] != row['NewTelefone1']:
        return row['NumTelefone2']
    elif row['VerificarNumTel3'] == 'Tel Celular - Correto' and row['NumTelefone3'] != row['NewTelefone1']:
        return row['NumTelefone3']
    elif row['VerificarNumTel2'] == 'Tel Fixo - Correto' and row['NumTelefone2'] != row['NewTelefone1']:
        return row['NumTelefone2']
    elif row['VerificarNumTel3'] == 'Tel Fixo - Correto' and row['NumTelefone3'] != row['NewTelefone1']:
        return row['NumTelefone3']
    else:
        return None  # ou qualquer valor padrão que você queira atribuir se nenhum número for encontrado
    
def organizar_numeros3(row):
    if row['VerificarNumTel3'] == 'Tel Celular - Correto' and row['NumTelefone3'] != row['NewTelefone1'] and row['NumTelefone3'] != row['NewTelefone2']:
        return row['NumTelefone3']
    elif row['VerificarNumTel2'] == 'Tel Celular - Correto' and row['NumTelefone2'] != row['NewTelefone1'] and row['NumTelefone2'] != row['NewTelefone2']:
        return row['NumTelefone2']
    elif row['VerificarNumTel3'] == 'Tel Fixo - Correto' and row['NumTelefone3'] != row['NewTelefone1'] and row['NumTelefone3'] != row['NewTelefone2']:
        return row['NumTelefone3']
    elif row['VerificarNumTel2'] == 'Tel Fixo - Correto' and row['NumTelefone2'] != row['NewTelefone1'] and row['NumTelefone2'] != row['NewTelefone2']:
        return row['NumTelefone2']
    else:
        return None  # ou qualquer valor padrão que você queira atribuir se nenhum número for encontrado