from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import boto3
import logging
import requests
import pandas as pd
from io import StringIO

MINIO_ENDPOINT = 'http://awari-minio-nginx:9000'
MINIO_ACCESS_KEY = '4Xf3Q7unyUtmTVFH'
MINIO_SECRET_KEY = 'q0b3SiIc28hy0750XN3uL1u5G5CzW0hR'
MINIO_BUCKET_NAME = 'aula-10'

def check_file_exists(file_name, bucket_name):
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='sa-east-1'
    )
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except:
        return False

def process_data():
    # define MinIO client
    client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False,
        region_name='sa-east-1'
    )

    # define url of the data repository
    url = 'https://github.com/owid/covid-19-data/raw/master/public/data/latest/owid-covid-latest.csv'

    # get data from url as a string
    response = requests.get(url)
    data_str = response.content.decode('utf-8')

    # create DataFrame from the data string
    df = pd.read_csv(StringIO(data_str))

    # convert DataFrame to Parquet format and save to MinIO
    parquet_filename = '/tmp/covid.parquet'
    df.to_parquet(parquet_filename, index=False)

    # upload the Parquet file to MinIO
    try:
        with open(parquet_filename, 'rb') as f:
            client.upload_fileobj(
                f, 
                MINIO_BUCKET_NAME, 
                'covid/processed/covid.parquet'
            )
    except Exception as e:
        logging.error(f"Failed to upload file to MinIO. Exception: {str(e)}")
        raise e

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_covid_disponibilization',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    check_file_task = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists,
        op_kwargs={
            'file_name': 'covid/processed/covid.parquet',
            'bucket_name': MINIO_BUCKET_NAME

        }
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        op_kwargs={
            'file_name': 'covid/processed/covid.parquet',
            'bucket_name': MINIO_BUCKET_NAME
        }
    )

    # define the success dummy task
    success = DummyOperator(
        task_id='success'
    )

    # define the failure dummy task
    failure = DummyOperator(
        task_id='failure'
    )

    # check if the file already exists in the bucket
    check_file_task >> process_data_task

    # define the logic to handle task success or failure
    process_data_task >> success
    process_data_task >> failure