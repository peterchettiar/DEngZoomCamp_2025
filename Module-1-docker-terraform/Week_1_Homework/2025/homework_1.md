# Module 1 Homework Submission

## Docker & SQL

### Question 1. Understanding docker first run

Using the same example from the video lectures, if we run the following command:

```bash
docker run -it --entrypoint=bash python:3.12.8
```

What actually happens is that docker runs the python image in its specfied version (downloads the images automatically from docker hub if not found in local repository) in interactive mode (i.e. you would be able to run other commands while the application runs in the background) and then opens `bash` shell as the entrypoint.

And in the `bash` shell, if we run `pip --version`, it would show us the installed version of `pip` in the specified version of our python container. See command output below.

![image](https://github.com/user-attachments/assets/b19025a9-30d3-4c61-ad29-5ca6b1e375c8)

> Answer: `24.3.1`

### Question 2: Understanding Docker networking and docker-compose

For the given `docker-compose.yml` file:

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

`pgadmin` is a web-based tool that makes it more convenient to access and manage our databases. And in the docker compose setup, the pgAdmin service needs to connect to PostgreSQL database service, and since docker compose services can refer to each other by name, the hostname for `pgadmin` to connect to would be `db`, with the following details:
- username = 'postgres'
- password = 'postgres'
- databse = 'ny_taxi'

Next, the port that ` pgadmin` should connect to is `5432` because this is the port on the inside of our `db` postgres container. Both the service name (`db`) and the container name (`postgres`) can be used. You should be aware that the port being used is the one exposed by the container (5432), not the port is set as port-forwarding (5432).

> [!NOTE]
> We only use port=5433 if we want to connect to `db` from our local host machine - `pgcli -h localhost -p 5433 -u root -d ny_taxi`. You must connect to localhost:5433 because port 5433 on your machine is forwarded to 5432 inside the container.

> Answer: `db:5432 ` or `postgres:5432`

## Prepare Postgres

To answer Question 3 to 6, we would need to load both the datasets into `postgres` and do the queries directly on `pgadmin`. So the steps for uploading the datasets into `postgres` are as follows:
1. First thing we need to do is to write-up a `docker-compose.yml` file so that we can `docker-compose up` both the `postgres` and `pgadmin` containers - see [docker-compose.yml](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/docker-compose.yml) (there is a slight modification from the one done during lecture - `volumes` for `pgadmin` was added at the end of the file)
2. Once the `docker-compose` containers are up and running in detached mode (`docker-compose up -d`) from our homework directory, run command `docker network ls` to see the name of the network that the containers are running on - this is to be used in our dockerised ingestion script in the upcoming steps
3. Once that is done, we can write an ingestion notebook to test if the data can be downloaded, and the connections with the database is stable (this step we would use localhost) - [upload_data_hw.ipynb](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/upload_data_hw.ipynb)
4. Next, we write a comprehensive ingestion python script to be used in our dockerfile - [ingest_data](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/ingest_data.py)
5. Write a `Dockerfile`([DockerFile](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/Dockerfile))
6. Now, all we need to do is to `docker build` to build the image from our `Dockerfile` - command used was `docker build -t taxi_ingest:hw1`
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
> Note: `network` name can be found by running `docker network ls` as mentioned in step 2 - usually default network names created by `docker-compose` would start with the name of the directory in which the `docker-compose.yml` file is located, unless otherwise mentioned path.

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

> Answer : 2019-09-26

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

### Question 7: Creating Resources

There are two terraform files, `main.tf` and `variables.tf` in [here](). As the name suggests, `main.tf` is the main configuration file for setting up the GCP resources, and `variables.tf` is the variable configuration to make the `main.tf` configurations more dynamic. Have a look at both files, they are in a `json` format and the configurations should be fairly easy to understand.

Next, we have to run three commands consecutively:
1. `terraform init` - command that initializes a Terraform working directory by downloading and installing the necessary plugins and modules required for the configuration
2. `terraform plan` - command that generates an execution plan, detailing what actions Terraform will take to change your infrastructure to match the desired state defined in your configuration files
3. `terraform apply` - command used to apply the changes required to reach the desired state defined in your configuration files. This command executes the execution plan generated by `terraform plan` and makes the necessary changes to your infrastructure

And after running the `terraform apply` command, we get the following:
![image](https://github.com/peterchettiar/DEngZoomCamp_2024/assets/89821181/a2b28dd2-62d8-49f3-92dc-99a02425d3a2)

## Prepare Postgres

To answer Question 3 to 6, we would need to load both the datasets into `postgres` and do the queries directly on `pgadmin`. So the steps for uploading the datasets into `postgres` are as follows:
1. First thing we need to do is to write-up a `docker-compose.yml` file so that we can `docker-compose up` both the `postgres` and `pgadmin` containers - see [docker-compose.yml](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-1-docker-terraform/Week_1_Homework/2025/docker-compose.yml) (there is a slight modification from the one done during lecture - `volumes` for `pgadmin` was added at the end of the file)
2. Once the `docker-compose` containers are up and running in detached mode (`docker-compose up -d`) from our homework directory, run command `docker network ls` to see the name of the network that the containers are running on - this is to be used in our dockerised ingestion script in the upcoming steps
3. Once that is done, we can write an ingestion notebook to test if the data can be downloaded, and the connections with the database is stable (this step we would use localhost) - [upload_data_hw.ipynb](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/2025/upload_data_hw.ipynb)
4. Next, we write a comprehensive ingestion python script to be used in our dockerfile - [ingest_data](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/2025/ingest_data.py)
5. Write a `Dockerfile`([DockerFile](https://github.com/peterchettiar/DEngZoomCamp_2024/blob/main/Week_1_basics_n_setup/Week_1_Homework/2025/Dockerfile))
6. Now, all we need to do is to `docker build` to build the image from our `Dockerfile` - command used was `docker build -t taxi_ingest:hw1`
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
> Note: `network` name can be found by running `docker network ls` as mentioned in step 2 - usually default network names created by `docker-compose` would start with the name of the directory in which the `docker-compose.yml` file is located, unless otherwise mentioned path.

> Tip: Now that we have created the container after the `docker run` command, if we were to run the same container again wtih the same configs we can run `docker ps -a` to see all the containers and copy the container ID for the dockerised ingestion script to run the container again using the command `docker start <contained_id>`
