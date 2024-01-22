## docker commands

``` bash
# create network to share data between containers
docker network create pg-network
```

``` bash
# create postgres database container
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v "c:/users/alfre/dropbox/study break/data engineering zoomcamp/Module_1_Docker_GCP_Terraform/ny_taxi_postgres_data":/var/lib/postgresql/data \
  -p 5432:5432 \
  --network pg-network \
  --name pg-database \
  postgres:16
```

``` bash
# restart postgres database container
docker restart pg-database
```

``` bash
# create pgadmin container
docker run -it \
    -e 'PGADMIN_DEFAULT_EMAIL=admin@admin.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=root' \
    -p 8080:80 \
    --network pg-network \
    dpage/pgadmin4
```

``` bash
# build the docker image in the module 1 folder
docker build -t taxi_ingest:v003 .
```

``` bash
# create ETL container to load data into postgres database container
docker run -it \
    --network=pg-network \
    taxi_ingest:v003 \
        --user=root \
        --password=root \
        --host=pg-database \
        --port=5432 \
        --database=ny_taxi \
        --table_name=yellow_taxi_cab \
        --csv_url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```