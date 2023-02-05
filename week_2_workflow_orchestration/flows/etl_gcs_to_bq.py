from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """
    Download trip data from GCS
    :param color:
    :param year:
    :param month:
    :return:
    """
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../../data/")

    return Path(f"../../data/{gcs_path}")


@task(retries=2, log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """
    Read parquet file and transform
    :param path:
    :return:
    """
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    # print(f"pre: Missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: Missing passenger count: {df['passenger_count'].isna().sum()}")

    return df


@task(retries=2)
def write_to_bq(df: pd.DataFrame, color: str) -> None:
    """
    Write DataFrame to BigQuery
    :param df:
    :return:
    """

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(destination_table=f"trips_data_all.{color}_trip",
              project_id="dtc-de-course-374916",
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=500_00,
              if_exists="append")


@flow(log_prints=True)
def etl_gcs_to_bq(year, month, color):
    """
    Main ETL flow to load data from GCS to BigQuery
    :return:
    """

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_to_bq(df, color)

@flow()
def etl_to_bq_parent_flow(
        months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == '__main__':
    months = [2,3]
    etl_to_bq_parent_flow(months=months)