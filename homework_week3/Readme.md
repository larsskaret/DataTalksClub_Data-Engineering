## Preparation

 Decided to use prefect orion and modified web_to_gcs script to upload data to GCS Bucket.

 Keeping it simple, using wget to download and then upload with the existing GCS Bucket block.

 BigQuery SQL commands for creating the tables:
 ```sql
--External table
CREATE OR REPLACE EXTERNAL TABLE `mythic-plexus-375706.trips_data_all.external_fhv_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ny_rides_data_lake_mythic-plexus-375706/data/fhv/fhv_tripdata_2019-*.csv.gz']);

--Table in BigQuery
CREATE OR REPLACE TABLE `mythic-plexus-375706.trips_data_all.fhv_2019` AS
SELECT * FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;
```

---

 ## Question 1
 What is the count for fhv vehicle records for year 2019?
```sql
--43_244_696
SELECT COUNT(1) FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;
```
---

## Question 2 
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
--0 B
SELECT DISTINCT(affiliated_base_number)
FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;

--317.94MB
SELECT DISTINCT(affiliated_base_number)
FROM `mythic-plexus-375706.trips_data_all.fhv_2019`;
```

---

## Qustion 3
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```sql
--717_748
SELECT COUNT(1)
FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`
WHERE
  PUlocationID IS NULL AND
  DOlocationID IS NULL;
  ```
## Question 4 
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

### Answer
Partition by pickup_datetime Cluster on affiliated_base_number

---

## Question 5
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).

Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

```SQL
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `mythic-plexus-375706.trips_data_all.fhv_2019_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;

--647.87MB
SELECT DISTINCT(affiliated_base_number)
FROM
  `mythic-plexus-375706.trips_data_all.fhv_2019`
WHERE 
  DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

--23.05MB
SELECT DISTINCT(affiliated_base_number)
FROM
  `mythic-plexus-375706.trips_data_all.fhv_2019_partitoned_clustered`
WHERE 
  DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

---

## Question 6
Where is the data stored in the External Table you created?

### Answer
GCP Bucket

## Question 7
It is best practice in Big Query to always cluster your data

### Answer
False (example less than 1 GB size, can be worse due to metadata)

## Question 8
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table.

Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur.

### Answer
I wanted to try out pandas for handling both the typecasting and the uploading. 

When inspecting the data, I think what you want to look for is nan-values (null) and object dtypes. Null values and int (or numpy int64) does not work well together, but pandas Int64 dtype does this. *The problem* originated from some files having columns with null values and integers -> float and some files having same columns with all numbers -> int. This cause problems when acting on these columns in BigQuery.

One column also had one or more files with only null values -> object, and then some with numbers. Carefull inspection on the tables we already created in BigQuery (vased on CSV files) confirmed that this also was null values and integers.

My *experimental* pipeline is `q8_web_to_gcs.py`