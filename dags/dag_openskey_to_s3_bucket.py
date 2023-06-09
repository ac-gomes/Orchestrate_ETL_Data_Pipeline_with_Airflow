import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from utils.utils import (
    convert_to_polars_dataFrame,
    upload_to_s3_bucket,
    delete_temp_file
)


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry': timedelta(minutes=2)
}

END_POINT = Variable.get('OpenSky_END_POINT')
ALL_DATA = Variable.get('OpenSky_data')
RAW_S3_BUCKET = Variable.get('raw_s3_bucket')


def transformer(ti) -> None:
    data = ti.xcom_pull(task_ids=['Extract_flights'])
    flights = data[0]['states']
    convert_to_polars_dataFrame(flights)


with DAG(
    dag_id='OpenSkyApi_to_s3_bucket_v02.1',
    default_args=default_args,
    description='This will to get data from openSky API and put into a Amazon s3 bucket',
    start_date=datetime(2023, 5, 24),
    # to run every 5 minutes timedelta(minutes=5) airflow versions >2.1
    schedule_interval=timedelta(minutes=5),
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    Is_api_available = HttpSensor(
        task_id='Is_api_available',
        http_conn_id='opensky_api',
        endpoint=END_POINT
    )

    Extract_flights = SimpleHttpOperator(
        task_id='Extract_flights',
        http_conn_id='opensky_api',
        endpoint=ALL_DATA,
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    Transform_flights = PythonOperator(
        task_id='Transform_flights',
        python_callable=transformer
    )

    Upload_flights_to_s3 = PythonOperator(
        task_id='Upload_flights_to_s3',
        python_callable=upload_to_s3_bucket,
        op_kwargs={
            'filename': 'tmpdata/flights.json',
            'key': 'flights.json',
            'bucket_name': RAW_S3_BUCKET
        }
    )

    Clear_tmp_files = PythonOperator(
        task_id='Clear_tmp_files',
        python_callable=delete_temp_file
    )

    end = EmptyOperator(
        task_id='end'
    )

start >> Is_api_available >> Extract_flights >> Transform_flights >> Upload_flights_to_s3 >> Clear_tmp_files >> end
