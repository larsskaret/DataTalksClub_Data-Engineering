import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def get_file(dataset_url: str, typedict: dict) -> pd.DataFrame:
    df = pd.read_csv(dataset_url, engine="pyarrow", dtype=typedict)
    print(df.info())
    print(len(df))
    return df


@task(log_prints=True)
def write_gcs(df: pd.DataFrame, gcs_path: str) -> None:
    gcp_credentials_block = GcpCredentials.load(
        "ny-rides-gcpcred-bucket"
    )  # gcp-cred-file")
    credentials = gcp_credentials_block.get_credentials_from_service_account()
    print("path " + gcs_path)
    print(gcp_credentials_block.service_account_file)
    df.to_parquet(
        gcs_path,
        engine="pyarrow",
        compression="snappy",
        storage_options={
            "token": credentials
        }
    )


@flow(log_prints=True)
def parquet_to_gcs():
    """
    Experimental -try out things- download csv files from
    github, upload parquet files to gsc, using pandas for
    both tasks, while keeping the dtypes consistent among
    all files.
    """
    typedict = {
        "dispatching_base_num": "string",
        "pickup_datetime": "datetime64[ns]",
        "dropOff_datetime": "datetime64[ns]",
        "PUlocationID": "Int64",
        "DOlocationID": "Int64",
        "SR_Flag": "Int64",
        "Affiliated_base_number": "string",
    }
    year = 2019
    service = "fhv"

    for month in range(1, 13):
        dataset_file = f"{service}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"
        df = get_file(dataset_url, typedict)
        gcs_path = f"gs://ny_rides_data_lake_mythic-plexus-375706/data/{service}/{service}_tripdata_{year}-{month:02}.parquet"

        write_gcs(df, gcs_path)
        print(str(month) + " done")


if __name__ == "__main__":
    parquet_to_gcs()
