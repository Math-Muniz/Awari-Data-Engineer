from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import BaseOperator
from custom_s3_hook import CustomS3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


AWS_BUCKET = 'aula-10'
POSTGRES_CONN_ID = 'pg_awari'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dag_export_to_postgres',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

class BIPgOperator(BaseOperator):
    def __init__(self, s3_key: str, tablename: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.s3_key = s3_key
        self.tablename = tablename
        self.custom_s3 = CustomS3Hook(bucket='aula-10')  # Custom S3Hook for interacting with S3
        self.pg_hook = PostgresHook(postgres_conn_id="pg_awari")  # PostgresHook for interacting with PostgreSQL
        self.engine = self.pg_hook.get_sqlalchemy_engine()

    def execute(self, context):
        self.process_to_pg()
        return self.s3_key
        
    def process_to_pg(self):
        s3_object = self.custom_s3.get_object(key=self.s3_key)  # Get the S3 object
        s3_data = s3_object.read()  # Read the data from the S3 object
        df = pd.read_parquet(BytesIO(s3_data))  # Read the parquet data into a DataFrame
        self.insert_to_postgres(df)  # Insert DataFrame into PostgreSQL table
        
    def insert_to_postgres(self, df):
        pg_conn = self.pg_hook.get_conn()  # Get the PostgreSQL connection
        cursor = pg_conn.cursor()  # Create a cursor
        table_columns = ','.join(df.columns)  # Get the column names
        table_values = ','.join(['%s' for _ in df.columns])  # Create placeholders for values
        insert_query = f"INSERT INTO {self.tablename} ({table_columns}) VALUES ({table_values})"  # Create the INSERT query
        cursor.executemany(insert_query, [tuple(row) for row in df.values.tolist()])  # Execute the query with DataFrame values
        pg_conn.commit()  # Commit the changes
        cursor.close()  # Close the cursor
        pg_conn.close()  # Close the connection


export_to_postgres_task = BIPgOperator(
    task_id='export_to_postgres',
    s3_key='covid/processed/covid.parquet',
    tablename='covid_data',
    dag=dag,
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)
end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator >> export_to_postgres_task >> end_operator
