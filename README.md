## Set Up Docker and Test it

### Run Hello World : on bash
```bash
docker run hello-world
```

2. Create docker file: Dockerfile and put dependencies
3. Create simple test_pipeline.py to be ran using the docker image that is built
4. Build image : docker build -t test:pandas .
5. Run Docker Image (Container): docker run -it test:pandas


### Running Posgres Using Docker

#### For Mac/Linux

```bash
docker run -it \
   -e POSTGRES_USER="root" \
   -e POSTGRES_PASSWORD="root" \
   -e POSTGRES_DB="ny_taxi" \
   -v $(pwd)/nytaxi_taxi_postgres_data:/var/lib/postgresql/data \
   -p 5432:5432 \
   postgres:13
```

#### For Windows

```bash
docker run -it \
   -e POSTGRES_USER="root" \
   -e POSTGRES_PASSWORD="root" \
   -e POSTGRES_DB="ny_taxi" \
   -v c:/User/path/nytaxi_taxi_postgres_data:/var/lib/postgresql/data \
   -p 5432:5432 \
   postgres:13
```

#### Install  CLI tool for accessing Postgres

Installing `pgcli`

#### for window or Mac
```bash
pip install pgcli
```
 or specifically Mac User
```bash
brew install pgcli
```

#### Access Postgres Using the Pgcli tool
```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```
To access table in db via pgcli use : \dt

#### Create Virtual Environment
```bash
virtualenv venv
```
#### Activate the venv using
```bash
source venv/bin/activate
```

####

1. Download sample data for Mac/Linux
```bash
curl -fLo yellow_tripdata_2021-01.csv --create-dirs https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```

2. Download sample data for Windows
```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```
3. Create a docker-compose file and put all requirements or services needed for the container
4. Build docker image and container, run: docker-compose build

# # To connect the postgres db instance runnning on a separate container to an airflow running in another container:
1. Add this to the docker-compose.yaml file of the the posgres container
networks:
  airflow:
    external:
      name: airflow_default

2. Add this in then posgres service as seen in the file:
    networks:
      - airflow

3.  Add your postgres db environment variable to the airflow docker-compose.yaml file
    PG_HOST: "**********" #pgdatabase
    PG_USER: "********"
    PG_PASSWORD: "****"
    PG_PORT: 5432
    PG_DATABASE: "******"

4. Spin up the airflow container and the postgres container