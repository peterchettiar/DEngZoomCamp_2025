# Module 1 Homework Submission

## Docker & SQL

### Question 1. Knowing docker tags

We run the `docker run --help` command to access the `--help` documentation for the `docker run` command. When you actually run the command, it gives you an exhaustive list of options, as well as their short descriptions, that potentially could be appended to the `docker run` command. This really depends on what is the outcome you are trying to achieve.

So the question is:

_Which tag has the following text?_

```bash
Automatically remove the container when it exits
```
The answer is `-rm` (see image below). 

![image](https://github.com/peterchettiar/DEngZoomCamp_2024/assets/89821181/9f90423b-f547-4a0f-8b83-814e244c4546)

Using the same example from the video lectures, if we run the following command:

```bash
docker run -it --rm --entrypoint=bash python:3.9
```

What actually happens is that docker runs the python image in its specfied version (downloads the images automatically from docker hub if not found in local repository) in interactive mode (i.e. you would be able to run other commands while the application runs in the background) and then opens `bash` shell as the entrypoint. Once we exit the programe, the container is removed.

### Question 2. Understanding docker first run

we run the same command as before, this time without the `--rm` option as follows:

```bash
docker run -it --entrypoint=bash python:3.9
```

And in the `bash` shell, if we run `pip list`, it would list down all the packages and their respective version. As such, the version for the package _wheel_ is `0.42.0`. See command output below.

![image](https://github.com/peterchettiar/DEngZoomCamp_2024/assets/89821181/f02da5fb-e7eb-443a-8a92-c0efed792bb1)

## Prepare Postgres

To answer Question 3 to 6, we would need to load both the datasets into `postgres` and do the queries directly on `pgadmin`. So the steps for uploading the datasets into `postgres` are as follows:
1. First thing we need to do is to write-up a `docker-compose.yml` file so that we can `docker-compose up` both the `postgres` and `pgadmin` containers - see [docker-compose.yml](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/docker-compose.yaml) (there is a slight modification from the one done during lecture - `volumes` for `pgadmin` was added at the end of the file)
2. Once the `docker-compose` containers are up and running in detached mode (`docker-compose up -d`) from our homework directory, run command `docker network ls` to see the name of the network that the containers are running on - this is to be used in our dockerised ingestion script in the upcoming steps
3. Once that is done, we can write an ingestion notebook to test if the data can be downloaded, and the connections with the database is stable (this step we would use localhost) - [upload_data_hw.ipynb](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/upload_data_hw.ipynb)
4. Next, we write a comprehensive ingestion python script to be used in our dockerfile - [ingest_data](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/ingest_data.py)
5. Write a `Dockerfile`([DockerFile]())
6. Now, all we need to do is to `docker build` to build the image from our `Dockerfile` - `docker build -t taxi_ingest:hw1`
7. The dockerised ingestion script image is built, now we have to `docker run` as follows:
```bash
docker run -it \
    --network=week_1_homework_default \
    taxi_ingest:hw1 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --first_table_name=green_taxi_trips \
    --second_table_name=taxi_zones \
    --green_taxi_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz" \
    --taxi_zones_url="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv" 
```
> Note: `network` name can be found by running `docker network ls` as mentioned in step 2 - usually network names created by `docker-compose` would start with the name of the directory in which the `docker-compose.yml` file is located, unless otherwise mentioned path.

> Tip: Now that we have created the container after the `docker run` command, if we were to run the same container again wtih the same configs we can run `docker ps -a` to see all the containers and copy the container ID for the dockerised ingestion script to run the container again using the command `docker start <contained_id>`

### Quetion 3: Count records

How many trips were totally made on September 18th 2019?
> Answer: 15612

```sql
SELECT
	COUNT(*)
FROM
	GREEN_TAXI_TRIPS
WHERE
	CAST(LPEP_PICKUP_DATETIME AS DATE) = '2019-09-18'
	AND CAST(LPEP_DROPOFF_DATETIME AS DATE) = '2019-09-18';
```

### Question 4: Longest trip for each day

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

> Answer : 2019=09-26

```sql
SELECT
	CAST(LPEP_PICKUP_DATETIME AS DATE) AS PICKUP_DATE,
	MAX(TRIP_DISTANCE) AS MAX_DISTANCE
FROM
	GREEN_TAXI_TRIPS
GROUP BY
	CAST(LPEP_PICKUP_DATETIME AS DATE)
ORDER BY
	MAX_DISTANCE DESC
LIMIT
	1
```

### Question 5: Three biggest pick up Boroughs

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

> Answer: "Brooklyn" "Manhattan" "Queens"

```sql
SELECT
	TZ."Borough",
	SUM(GTT."total_amount") AS TOTAL_AMT_AGG
FROM
	GREEN_TAXI_TRIPS GTT
	JOIN TAXI_ZONES TZ ON GTT."PULocationID" = TZ."LocationID"
WHERE
	CAST(LPEP_PICKUP_DATETIME AS DATE) = '2019-09-18'
	AND TZ."Borough" != 'Unknown'
GROUP BY
	TZ."Borough"
HAVING
	SUM(GTT."total_amount") > 50000
ORDER BY
	TOTAL_AMT_AGG DESC
```

### Question 6: Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? 

> Answer: JFK Airport

```sql
WITH
	PICKUP_TABLE AS (
		SELECT
			CAST(LPEP_PICKUP_DATETIME AS DATE) AS PICKUP_DATE,
			GTT."PULocationID",
			CAST(LPEP_DROPOFF_DATETIME AS DATE) AS DROPOFF_DATE,
			GTT."DOLocationID",
			TIP_AMOUNT,
			TZ."Zone" AS PICK_UP_ZONE
		FROM
			GREEN_TAXI_TRIPS GTT
			JOIN TAXI_ZONES TZ ON GTT."PULocationID" = TZ."LocationID"
	)
SELECT
	PICKUP_DATE,
	PU."PULocationID",
	DROPOFF_DATE,
	PU."DOLocationID",
	TIP_AMOUNT,
	PICK_UP_ZONE,
	TZ."Zone" AS DROPOFF_ZONE
FROM
	PICKUP_TABLE PU
	JOIN TAXI_ZONES TZ ON PU."DOLocationID" = TZ."LocationID"
WHERE
	PICK_UP_ZONE = 'Astoria'
ORDER BY
	TIP_AMOUNT DESC
LIMIT
	1
```
