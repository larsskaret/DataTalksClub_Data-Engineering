from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("ny-rides-bucket-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")



@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("ny-rides-gcpcred-bucket")

    df.to_gbq(
        destination_table="trips_data_all.question-3",
        project_id="mythic-plexus-375706",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main E(T)L flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    rows = 0
    for month in months:
        rows += etl_gcs_to_bq(year, month, color)
    print(f"Rows: {rows}")

if __name__ == "__main__":
    etl_parent_flow()