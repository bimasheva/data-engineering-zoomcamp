import os
from pathlib import Path

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

BASE_DIR = os.path.dirname(__file__)


@task(retries=1, log_prints=True)
def fetch(dataset_url: str, file_name: str) -> Path:
    """
    Read data from web into pandas DataFrame
    :param dataset_url:
    :return:
    """

    if dataset_url.endswith('.csv.gz'):
        csv_name = f"{file_name}.csv.gz"
    else:
        csv_name = f"{file_name}.csv"

    file_path = os.path.join(BASE_DIR, csv_name)

    os.system(f"python3 -m wget {dataset_url} -o {file_path}")

    return file_path


@task(log_prints=True, retries=2)
def write_to_gcs(path: Path, dataset_file: str) -> None:
    """
    Uploading parquet file to GCS
    :param path:
    :return:
    """

    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=f"data/fhv/{dataset_file}.csv.gz"
    )


@flow(log_prints=True)
def extract_to_gcs(year: int, month: int) -> None:
    """
    The main ETL function
    :return:
    """

    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    path = fetch(dataset_url, dataset_file)
    write_to_gcs(path, dataset_file)


@flow()
def fhv_data_flow(
        months: list[int] = [1, 2], year: int = 2019
):
    for month in months:
        extract_to_gcs(year, month)


if __name__ == '__main__':
    months = list(range(1, 13))
    year = 2019
    fhv_data_flow(months=months, year=year)
