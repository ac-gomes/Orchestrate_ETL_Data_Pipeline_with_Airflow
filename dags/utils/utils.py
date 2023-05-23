
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
