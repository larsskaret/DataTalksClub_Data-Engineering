
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `mythic-plexus-375706.trips_data_all.external_fhv_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ny_rides_data_lake_mythic-plexus-375706/data/fhv/fhv_tripdata_2019-*.csv.gz']);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `mythic-plexus-375706.trips_data_all.fhv_2019` AS
SELECT * FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;

--Question 1
--43_244_696
  SELECT COUNT(1) FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;

  --Question 2
--0 B
SELECT DISTINCT(affiliated_base_number)
FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`;
--317.94MB
SELECT DISTINCT(affiliated_base_number)
FROM `mythic-plexus-375706.trips_data_all.fhv_2019`;

--Question 3
--717748
SELECT COUNT(1)
FROM `mythic-plexus-375706.trips_data_all.external_fhv_2019`
WHERE
  PUlocationID IS NULL AND
  DOlocationID IS NULL;

--Question 5
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

