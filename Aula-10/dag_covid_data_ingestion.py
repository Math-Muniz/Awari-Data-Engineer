import os
import requests
import shutil
import gzip
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

data_path = '/tmp/covid_data/'

def download_data():
    # Creates the folder if it does not exist
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    # Gets today's date and yesterday's date
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Gets the URL for yesterday's and today's files
    url_today = f'https://github.com/owid/covid-19-data/raw/master/public/data/{today}.csv.gz'
    url_yesterday = f'https://github.com/owid/covid-19-data/raw/master/public/data/{yesterday}.csv.gz'

    # Downloads today's file
    response_today = requests.get(url_today, stream=True)

    # If today's file is not available, tries to download yesterday's file
    if response_today.status_code != 200:
        response_yesterday = requests.get(url_yesterday, stream=True)
        if response_yesterday.status_code == 200:
            filename = f'{yesterday}.csv.gz'
            with open(os.path.join(data_path, filename), 'wb') as f:
                shutil.copyfileobj(response_yesterday.raw, f)
                print(f'Download do arquivo {filename} concluído.')
        else:
            print('Não foi possível baixar nenhum arquivo.')
            return
    else:
        filename = f'{today}.csv.gz'
        with open(os.path.join(data_path, filename), 'wb') as f:
            shutil.copyfileobj(response_today.raw, f)
            print(f'Download do arquivo {filename} concluído.')
    
    # Extracts the file
    with gzip.open(os.path.join(data_path, filename), 'rb') as f_in:
        with open(os.path.join(data_path, f'{filename[:-3]}'), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            print(f'Extraindo arquivo {filename}...')


with DAG('dag_covid_data_ingestion', default_args=default_args, schedule_interval='@daily') as dag_ingestion:
    task_download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_data
    )
