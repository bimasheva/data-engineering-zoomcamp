#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task

@task(log_prints=True, retries=3)
def ingest_data(config, base_dir, table_name, url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    file_path = os.path.join(base_dir, csv_name)
    os.system(f"python3 -m wget {url} -o {file_path}")

    print("file has been extracted")

    url = 'postgresql://{}:{}@{}:{}/{}'.format(
        config.get("user"),
        config.get("password"),
        config.get("host"),
        config.get("port"),
        config.get("db")
    )
    engine = create_engine(url)

    df_iter = pd.read_csv(file_path, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest flow")
def main_flow():
    config = {
        "host": "localhost",
        "port": 5431,
        "user": "root",
        "password": "root",
        "db": "ny_taxi"
    }
    base_dir = os.path.dirname(__file__)

    table_name = "yellow_trip"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(config, base_dir, table_name, url)


if __name__ == '__main__':
    main_flow()



