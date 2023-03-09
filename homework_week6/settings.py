import pyspark.sql.types as T

GREEN_INPUT_DATA_PATH = 'resources/green_tripdata_2019-01.csv' #resources/rides.csv'python/
FHV_INPUT_DATA_PATH = 'resources/fhv_tripdata_2019-02.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV_GREEN = CONSUME_TOPIC_RIDES_CSV_GREEN = 'green_rides_csv'
PRODUCE_TOPIC_RIDES_CSV_FHV = CONSUME_TOPIC_RIDES_CSV_FHV = 'fhv_rides_csv'

RIDE_SCHEMA = T.StructType(
    [
     T.StructField("DOlocationID", T.IntegerType()),
     T.StructField("PULocationID", T.IntegerType()),
     ])

