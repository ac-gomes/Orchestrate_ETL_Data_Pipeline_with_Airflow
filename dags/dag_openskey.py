import json
from airflow import DAG
from airflow.models import Variable

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.utils import convert_to_polars_dataFrame, convert_to_parquet, save_file


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry': timedelta(minutes=2)
}

END_POINT = Variable.get('OpenSky_END_POINT')
ALL_DATA = Variable.get('OpenSky_data')


def convert_to_df(ti) -> None:
    # import polars as pl
    data = ti.xcom_pull(task_ids=['Extract_flights'])
    flights = data[0]['states']

    df_flights = convert_to_polars_dataFrame(flights)
    print(df_flights.schema)


with DAG(
    dag_id='get_data_from_OpenSkyApi_v01.8.5',
    default_args=default_args,
    description='This will get data from openSkyAPI',
    start_date=datetime(2023, 5, 23),
    schedule_interval='@daily',
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    Is_api_available = HttpSensor(
        task_id='Is_api_available',
        http_conn_id='opensky_api',
        endpoint=f"{END_POINT}/states/all?time=1458564121&icao24=3c6444"
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
        python_callable=convert_to_df
    )

    Load_flights = PythonOperator(
        task_id='Load_flights',
        python_callable=convert_to_parquet
    )

    end = EmptyOperator(
        task_id='end'
    )

start >> Is_api_available >> Extract_flights >> Transform_flights >> Load_flights >> end
