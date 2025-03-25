import os
import sys
from simpledbf import Dbf5
from pathlib import Path
from pandas import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import date


# para testar
//
user = 'sa_sesa_pgsql'
passw = 'sesapostgres2020'
host = '10.29.216.46'  # dev
port = '5432'
database = 'sesa'
conn = 'postgresql://' + user + ':' + passw + '@' + host + ':' + port + '/' + database

# Faz a conexão
engine = create_engine('postgresql://sa_sesa_pgsql:sesapostgres2020@10.29.216.46:5432/sesa')

# Base do diretório onde o script roda. 
basedir = os.path.abspath(os.path.dirname(__file__))

# Conta a quantidade de arquivos no basedir. 
qtd_arquivos = len(os.listdir(basedir))



# Verifica se a conexao com o banco de dados esta OK
def db_teste():
    try:
        db = engine
        with db.connect():
            print('Conexão estabelecida com sucesso! \n')
            pass
    except Exception as e:
        print(f'Não foi possível conectar ao banco de dados.')
        sys.exit(f' Erro: {e}')

''' Mostra todos os os esquemas do banco conectado. '''
def mostra_tabelas():
    esquemas = inspector.get_schema_names()
    for esquema in esquemas:
        print(esquema)

''' Mostra todas as tabelas de um esquema especifico. '''
def mostra_tabelas():
    tabelas = inspector.get_table_names(schema='geo')
    for tabela in tabelas:
        print(tabela)

# Refletir a tabela escolhida do esquema específico
#geo_tb_regional = Table('tb_regional', metadata, autoload_with=engine, schema='geo')
#query = select(geo_tb_regional)
#result = session.execute(query)

# Iniciar sessão no banco de dados. 
#Session = sessionmaker(bind=engine)
#session = Session()

# Metadados do banco. 
#metadata = MetaData()

#Inspeção geral no banco
#inspector = inspect(engine)

# Todos os esquemas do banco
#esquemas = inspector.get_schema_names()



# Tratamento DBF


# Verifica se o arquivo DBF existe
def dbf_existe(filename):
    dbf_file = basedir + '/' + filename
    if not os.path.exists(dbf_file):
        sys.exit('Arquivo não encontrado.')
    else:
        pass



''' Essa função é responsável por converter um arquivo dbf em um dataframe.  '''
def converte_dbf2df(filename):

    # Seleciona o arquivo DBF
    dbf_existe(filename)
    dbf_file = basedir + '/' + filename

    #Identifica o arquivo DBF
    dbf = Dbf5(dbf_file, codec='latin-1')

    #Transforma em um dataframe
    df = pd.DataFrame(dbf.to_dataframe())
    
    # Realiza o tratamento das colunas
    df.columns = map(str.lower, df.columns)

    return df



def quantidade_arquivos():
    pass

def nome_arquivos():
    pass

def main():
   
    
    
    filename = input("nome do arquivo: ")
    df_file = converte_dbf2df(filename)


    #db_teste()
    #with engine.connect() as con:











if __name__ == '__main__':
    main()
