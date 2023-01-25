--Question 3
SELECT COUNT(1)
FROM green_taxi_trips
WHERE lpep_pickup_datetime::DATE = '2019-01-15'::DATE AND
	lpep_dropoff_datetime::DATE = '2019-01-15'::DATE;

--Question 4
SELECT lpep_pickup_datetime::DATE
FROM green_taxi_trips
GROUP BY lpep_pickup_datetime::DATE
ORDER BY MAX(trip_distance) DESC
LIMIT 1;

--Question 5
SELECT passenger_count, COUNT(passenger_count)
FROM green_taxi_trips
WHERE lpep_pickup_datetime::DATE = '2019-01-01'::DATE AND
	(passenger_count = 2 OR passenger_count = 3)
GROUP BY passenger_count;

--Question 6

SELECT zdo."Zone" AS "Zone"
FROM green_taxi_trips AS gtt
	JOIN ny_zones AS zpu ON gtt."PULocationID" = zpu."LocationID"
	JOIN ny_zones AS zdo ON gtt."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'Astoria'
ORDER BY gtt."tip_amount" DESC
LIMIT 1;