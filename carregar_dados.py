import pandas as pd
import sqlite3
from datetime import datetime
from tabulate import tabulate

# Função para adaptar datas para o formato ISO
def adapt_date(date):
    return date.isoformat()

# Função para converter datas de volta para objetos datetime
def convert_date(date_bytes):
    return datetime.fromisoformat(date_bytes.decode('utf-8'))

# Registrar os adaptadores e conversores
sqlite3.register_adapter(datetime, adapt_date)
sqlite3.register_converter('DATE', convert_date)

# Função para inserir dados no banco
def inserir_dados(df, conn):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute('''
        INSERT INTO companhias (cnpj_cia, denom_social, sit, data_atualizacao)
        VALUES (?, ?, ?, ?)
        ''', (row['CNPJ_CIA'], row['DENOM_SOCIAL'], row['SIT'], datetime.now()))
    conn.commit()
    print("Dados inseridos com sucesso.")

# Função para exibir dados da tabela
def exibir_tabela(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM companhias')
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    print(tabulate(rows, headers, tablefmt='psql'))

# Função para consultar dados
def consultar_dados(cnpj=None, data=None):
    cursor = conn.cursor()
    query = 'SELECT * FROM companhias WHERE 1=1'
    params = []
    if cnpj:
        query += ' AND cnpj_cia = ?'
        params.append(cnpj)
    if data:
        query += ' AND date(data_atualizacao) <= ?'
        params.append(data)
    cursor.execute(query, params)
    return cursor.fetchall()

# Carregar o arquivo CSV
file_path = 'cad_cia_aberta.csv'
print(f"Carregando dados do arquivo: {file_path}")
df = pd.read_csv(file_path, delimiter=';', encoding='latin1')
print("Dados carregados com sucesso.")

# Conectar ao banco de dados com conversor para DATE
conn = sqlite3.connect('companias_abertas.db', detect_types=sqlite3.PARSE_DECLTYPES)
print("Conectado ao banco de dados com sucesso.")

# Criar tabela se não existir
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS companhias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cnpj_cia TEXT,
    denom_social TEXT,
    sit TEXT,
    data_atualizacao DATE DEFAULT (date('now'))
)
''')
conn.commit()
print("Tabela criada com sucesso (ou já existia).")

# Inserir dados no banco de dados
inserir_dados(df, conn)

# Exibir dados da tabela
print("Exibindo dados da tabela:")
exibir_tabela(conn)

# Exemplo de uso da consulta
print("\nResultados da consulta:")
resultado = consultar_dados(cnpj='00.000.000/0000-00', data='2023-01-01')
for linha in resultado:
    print(linha)

print("Script executado com sucesso.")
