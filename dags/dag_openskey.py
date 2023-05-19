
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry': timedelta(minutes=2)
}

END_POINT = Variable.get('OpenSky_END_POINT')


def env_variables():
    print(END_POINT)
    print('fim')


with DAG(
    dag_id='get_data_from_OpenSkyApi_v01.1',
    default_args=default_args,
    description='This will get data from openSkyAPI',
    start_date=datetime(2023, 5, 19),
    schedule_interval='@daily',
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

    get_flight_data = SimpleHttpOperator(
        task_id='get_flight_data',
        http_conn_id='opensky_api',
        endpoint='api/states/all',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    end = EmptyOperator(
        task_id='end'
    )

start >> Is_api_available >> get_flight_data >> end
