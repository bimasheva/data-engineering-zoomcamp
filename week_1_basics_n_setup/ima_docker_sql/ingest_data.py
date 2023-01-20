import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine

config = {
    "host": "localhost",
    "port": 5432,
    "user": "root",
    "password": "root",
    "db": "ny_taxi"
}
BASE_DIR = os.path.dirname(__file__)


def read_data(params):
    url = params.url
    table_name = params.table_name

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    file_path = os.path.join(BASE_DIR, csv_name)
    # os.system(f"python3 -m wget {url} -o {file_path}")

    print("file has been extracted")

    url = 'postgresql://{}:{}@{}/{}'.format(
        config.get("user"),
        config.get("password"),
        config.get("host"),
        config.get("db")
    )
    engine = create_engine(url)

    df_iter = pd.read_csv(file_path, iterator=True, chunksize=100000)

    df = next(df_iter)

    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try:
            t_start = time()

            df = next(df_iter)

            # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    read_data(args)
