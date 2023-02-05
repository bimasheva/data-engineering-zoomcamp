from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

BASE_DIR = os.path.dirname(__file__)

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read data from web into pandas DataFrame
    :param dataset_url:
    :return:
    """
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True, retries=2)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """
    Fix dtype issues
    :param df:
    :param color:
    :return:
    """
    if color=="yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    if color == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])


    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


@task(log_prints=True, retries=2)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write dataframe to the parquet file
    :param df:
    :param color:
    :param dataset_file:
    :return:
    """

    # file_path = os.path.join(BASE_DIR, csv_name)
    # path = Path(f"data/{color}/{dataset_file}.parquet")
    path = Path(f"{dataset_file}.parquet")
    to_path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')

    return (path,to_path)


@task(log_prints=True, retries=2)
def write_to_gcs(path: Path, to_path:Path) -> None:
    """
    Uploading parquet file to GCS
    :param path:
    :return:
    """

    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=to_path
    )


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """
    The main ETL function
    :return:
    """

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    file_path, to_path = write_local(df_clean, color, dataset_file)
    write_to_gcs(file_path, to_path)


@flow()
def etl_parent_flow(
        months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    months = [2,3]
    year = 2019
    etl_parent_flow(months=months, year=year)
