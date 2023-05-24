import polars as pl
import os
import pathlib
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

    try:
        df = pl.DataFrame(data=data, schema=schema)
        TEMP_FILE_PATH: pathlib.Path = 'tmpdata/polars_df.json'
        df.write_ndjson(TEMP_FILE_PATH)

    except Exception as Error:
        print(f"Something went wrong: {Error}")


def upload_to_s3_bucket(filename, key, bucket_name) -> None:
    hook = S3Hook('s3_conn')

    try:
        hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )

    except Exception as Error:
        print(f"Something went wrong: {Error}")
    pass


def clear_dir() -> None:
    pass
