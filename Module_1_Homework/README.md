## Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

``` bash
      --rm                             Automatically remove the container
                                       when it exits
```


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

``` bash
docker run -it --entrypoint=bash python:3.9

pip list

Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.42.0
```



# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

``` bash
# Running locally
python -m ingest \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--database=taxi_ny \
--ingest_type=green_taxi_trip \
--url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz

python -m ingest \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--database=taxi_ny \
--ingest_type=taxi_zone_lookup \
--url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

``` bash
# Running through docker
docker run -it \
--network=homework_ingest \
ingest_taxi:v002 \
--user=root \
--password=root \
--host=pgdatabase \
--port=5432 \
--database=taxi_ny \
--ingest_type=green_taxi_trip \
--url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz

docker run -it \
--network=homework_ingest \
ingest_taxi:v002 \
--user=root \
--password=root \
--host=pgdatabase \
--port=5432 \
--database=taxi_ny \
--ingest_type=taxi_zone_lookup \
--url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

``` sql
select count(*)
from green_taxi_trip 
where date(lpep_pickup_datetime) = '2019-09-18'
	and date(lpep_dropoff_datetime) = '2019-09-18'

"count"
15612
```

## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

``` sql
select date(lpep_pickup_datetime), max(trip_distance) as max_dist
from green_taxi_trip 
group by date(lpep_pickup_datetime)
order by max(trip_distance) desc
limit 1

"date"	      "max_dist"
"2019-09-26"	341.64
```

## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

``` sql
select "Borough", sum(total_amount) from green_taxi_trip gt
left join (select * from taxi_zone_lookup
			where "Borough" <> 'Unknown') tz
on gt."PULocationID" = tz."LocationID"
where date(lpep_pickup_datetime) = '2019-09-18'
group by "Borough"
having sum(total_amount) > 50000
order by sum(total_amount) desc

"Borough"	"sum"
"Brooklyn"	96333.24000000113
"Manhattan"	92271.30000000274
"Queens"	78671.71000000101
```

## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

``` sql
with tz as (select * from taxi_zone_lookup
			where "Borough" <> 'Unknown')

select tz_dropoff."Zone", max(tip_amount) as largest_tip from green_taxi_trip gt
left join tz tz_pickup on gt."PULocationID" = tz_pickup."LocationID"
left join tz tz_dropoff on gt."DOLocationID" = tz_dropoff."LocationID" 
where EXTRACT('year' from lpep_pickup_datetime) = 2019
	and EXTRACT('month' from lpep_pickup_datetime) = 09
	and tz_pickup."Zone" = 'Astoria'
	and tz_dropoff."Zone" IS NOT NULL
group by tz_dropoff."Zone"
order by max(tip_amount) desc
limit 5;

"Zone"	"largest_tip"
"JFK Airport"	62.31
"Woodside"	30
"Kips Bay"	28
"Astoria"	20
"Upper West Side South"	20
```


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET