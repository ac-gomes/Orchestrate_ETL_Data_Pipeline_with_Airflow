import json
from airflow import DAG
from airflow.models import Variable

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry': timedelta(minutes=2)
}

END_POINT = Variable.get('OpenSky_END_POINT')
ALL_DATA = Variable.get('OpenSky_data')

schema = [
    'icao24',
    'callsign',
    'origin_country',
    'time_position',
    'last_contact',
    'longitude',
    'latitude',
    'baro_altitude',
    'on_ground',
    'velocity',
    'true_track',
    'vertical_rate',
    'sensors',
    'geo_altitude',
    'squawk',
    'spi',
    'position_source'
]


def convert_to_polars_dataFrame(data):
    """This function will convert the received data in a polars dataframe"""
    import polars as pl
    try:
        df = pl.DataFrame(data=data, schema=schema)
        return df
    except Exception as Error:
        print(f"Something went wrong: {Error}")


def convert_to_parquet():
    pass


def save_file():
    pass


def Transformers(ti) -> None:
    # import polars as pl
    data = ti.xcom_pull(task_ids=['Extract_flights'])
    flights = data[0]['states']

    df_flights = convert_to_polars_dataFrame(flights)
    print(df_flights.schema)


with DAG(
    dag_id='get_data_from_OpenSkyApi_v01.8.4',
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
        python_callable=Transformers
    )

    Load_flights = PythonOperator(
        task_id='Load_flights',
        python_callable=save_file
    )

    end = EmptyOperator(
        task_id='end'
    )

start >> Is_api_available >> Extract_flights >> Transform_flights >> Load_flights >> end
