from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os

# definindo os argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5, 12),
    'retries': 1
}
# definindo a função responsável por importar os arquivos CSV para o PostgreSQL
def import_csv_to_postgres():
    # definindo as configurações de conexão com o banco de dados PostgreSQL
    db_config = {
        'user': 'airflow',
        'password': 'airflow',
        'host': '172.18.0.110',
        'port': 5432,
        'database': 'airflow'
    }
    conn = psycopg2.connect(**db_config)

    # caminho do arquivo atual
    csv_municipios_path = 'D:/Awari-DataEngineer/Aula-3/awari-engenharia-de-dados-docker-main/exercicios/municipios-estados/csv/municipios.csv'
    csv_estados_path = 'D:/Awari-DataEngineer/Aula-3/awari-engenharia-de-dados-docker-main/exercicios/municipios-estados/csv/estados.csv'

    # caminho relativo dos arquivos CSV
    estados_csv_path = os.path.join(csv_estados_path)
    municipios_csv_path = os.path.join(csv_municipios_path)

    # lendo os arquivos CSV como DataFrames do pandas
    df_estados = pd.read_csv(estados_csv_path)
    df_municipios = pd.read_csv(municipios_csv_path)

    # executando as queries SQL de criação de tabelas e inserção de dados
    with conn.cursor() as cur:
        # criando a tabela 'estados'
        cur.execute("""
            CREATE TABLE estados (
                codigo_uf INTEGER PRIMARY KEY,
                uf TEXT,
                nome TEXT
            );
        """)

        # inserindo os dados da tabela 'estados'
        for row in df_estados.itertuples(index=False):
            cur.execute("""
                INSERT INTO estados (codigo_uf, uf, nome) VALUES (%s, %s, %s);
            """, (row.codigo_uf, row.uf, row.nome))

        # criando a tabela 'municipios'
        cur.execute("""
            CREATE TABLE municipios (
                codigo_ibge INTEGER PRIMARY KEY,
                nome TEXT,
                codigo_uf INTEGER,
                FOREIGN KEY (codigo_uf) REFERENCES estados(codigo_uf)
            );
        """)

        # inserindo os dados da tabela 'municipios'
        for row in df_municipios.itertuples(index=False):
            cur.execute("""
                INSERT INTO municipios (codigo_ibge, nome, codigo_uf) VALUES (%s, %s, %s);
            """, (row.codigo_ibge, row.nome, row.codigo_uf))
        # commitando as alterações no banco de dados
        conn.commit()
    # fechando a conexão com o banco de dados
    conn.close()

# definindo o DAG com o nome 'import_csv_to_postgres' e os argumentos padrão definidos anteriormente
with DAG('import_csv_to_postgres', default_args=default_args, schedule_interval=None) as dag:
    # definindo a tarefa que irá executar a função 'import_csv_to_post
    task_import_csv_to_postgres = PythonOperator(
        task_id='import_csv_to_postgres',
        python_callable=import_csv_to_postgres
    )

     # criando a tarefa para criar a tabela 'estados' no banco de dados PostgreSQL
    task_create_table_estados = PostgresOperator(
        task_id='create_table_estados',
        postgres_conn_id='awari_postgresql',
        sql="""
            CREATE TABLE estados (
                codigo_uf INTEGER PRIMARY KEY,
                uf TEXT,
                nome TEXT
            )
        """
    )
    # criando a tarefa para criar a tabela 'municipios' no banco de dados PostgreSQL
    task_create_table_municipios = PostgresOperator(
        task_id='create_table_municipios',
        postgres_conn_id='awari_postgresql',
        sql="""
            CREATE TABLE municipios (
                codigo_ibge INTEGER PRIMARY KEY,
                nome TEXT,
                codigo_uf INTEGER,
                FOREIGN KEY (codigo_uf) REFERENCES estados(codigo_uf)
            )
        """
    )

    # definindo a dependência entre a tarefa de importação de dados e as tarefas de criação de tabelas
    task_import_csv_to_postgres >> [task_create_table_estados, task_create_table_municipios]
