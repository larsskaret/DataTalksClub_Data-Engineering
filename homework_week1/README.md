## Question 1. Knowing docker tags  
Run the command to get information on Docker  
`docker --help`  
Now run the command to get help on the "docker build" command  
Which tag has the following text? - *Write the image ID to the file* 

Command in shell:

`docker run --help | grep 'Write the image ID'`  

Answer: `--iidfile string` 

## Question 2. Understanding docker first run  
    
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed (use pip list). How many python packages/modules are installed?  

Command in shell:

`docker run -it python:3.9 bash`  
`pip list`  

Answer: 3  

## Prepare Postgres  
    
Run Postgres and load data as shown in the videos  
We'll use the green taxi trips from January 2019:  

`wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz`  

You will also need the dataset with zones:  

`wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv` 

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

---

I decided to use: 
- The [docker compose](https://github.com/larsskaret/DataTalksClub_Data-Engineering/blob/main/homework_week1/docker-compose.yaml) file from the course material to create a PostgreSQL and pgAdmin containter.
- jupyter notebook. Since this will be done only once - no need for script. And jupyer is nice to explore the data.

1. Download (wget) and explore (jupyter) the data. Check datatypes, especially datetime.

2. Run `docker compose up -d`. Notice I use port 5431 since I have PostgreSQL on local computer.

3. Check row count and divide into chunks if needed. Insert data into database, see [green_zone_upload_data.ipynb](https://github.com/larsskaret/DataTalksClub_Data-Engineering/blob/main/homework_week1/green_zone_upload_data.ipynb)

4. Log into pgAdmin http://localhost:8080/ u: `admin@admin.com` pw: root and verify data is inserted correctly.

#Note: All SQL commands are gathered in the file [homework_week1.sql](https://github.com/larsskaret/DataTalksClub_Data-Engineering/blob/main/homework_week1/homework_week1.sql)

## Question 3. Count records  
    
How many taxi trips were made on January 15 in total?  

Tip: started and finished on 2019-01-15.  

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.  

``` SQL
SELECT COUNT(1)
FROM green_taxi_trips
WHERE lpep_pickup_datetime::DATE = '2019-01-15'::DATE AND
	lpep_dropoff_datetime::DATE = '2019-01-15'::DATE;
```

Answer: 20530

## Question 4. Largest trip for each day  

Which was the day with the largest trip distance  
Use the pick up time for your calculations.  

``` sql
SELECT lpep_pickup_datetime::DATE
FROM green_taxi_trips
GROUP BY lpep_pickup_datetime::DATE
ORDER BY MAX(trip_distance) DESC
LIMIT 1;
```
Answer: 2019-01-15  

## Question 5. The number of passengers  

In 2019-01-01 how many trips had 2 and 3 passengers?  
  
 
``` sql
SELECT passenger_count, COUNT(passenger_count)
FROM green_taxi_trips
WHERE lpep_pickup_datetime::DATE = '2019-01-01'::DATE AND
	(passenger_count = 2 OR passenger_count = 3)
GROUP BY passenger_count;
```
Answer: 2: 1282 ; 3: 254  
## Question 6. Largest tip  

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?  
We want the name of the zone, not the id.  

Note: it's not a typo, it's `tip` , not `trip`  
 
```sql
SELECT zdo."Zone" AS "Zone"
FROM green_taxi_trips AS gtt
	JOIN ny_zones AS zpu ON gtt."PULocationID" = zpu."LocationID"
	JOIN ny_zones AS zdo ON gtt."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'Astoria'
ORDER BY gtt."tip_amount" DESC
LIMIT 1;
```
Answer: Long Island City/Queens Plaza

## Terraform

In this homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP install Terraform. Copy the files from the course repo here to your VM.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

## Question 1. Creating Resources

After updating the main.tf and variable.tf files run:

```terraform apply```

Paste the output of this command into the homework submission form.


Answer:

See [here](https://github.com/larsskaret/DataTalksClub_Data-Engineering/tree/main/homework_week1/Terraform_homework) for main.tf, variables.tf and output from `terraform apply`.