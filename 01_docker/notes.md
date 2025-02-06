
```bash
# create a network to communicate later with pgadmin container
docker network create pg-network

#start postgres db in a docker container 
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13

#start pgcli
pgcli -h localhost -U root -d ny_taxi -p 5432

#count file
wc -l yellow_tripdata_2021-01.csv  
# --> 1369766 yellow_tripdata_2021-01.csv

#run pgadmin to handle our postgres db
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4

# review all existing contianers
docker ps -a

# restart certain containers
docker restart pg-database
docker restart da4870f4ea54

# start ingestion locally using python script
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python loadNYdata.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=ny_taxi \
  --url=${URL}

# create docker image for ingestion job
docker build -t taxi_ingest:v001 .

# run the image in the network so it can access the DB
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=ny_taxi \
    --url=${URL}