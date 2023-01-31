
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("ny-rides-gcpcred-bucket"),
    bucket="ny_rides_data_lake_mythic-plexus-375706",  # insert your  GCS bucket name
)

bucket_block.save("ny-rides-bucket-block", overwrite=True)