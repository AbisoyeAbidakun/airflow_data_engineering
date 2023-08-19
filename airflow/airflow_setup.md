## Setup (official)

### Requirements
1. Upgrade docker-compose version:2.x.x+
2. Allocate memory to docker between 4gb-8gb
3. Python: version 3.8+


### Set Airflow

1.  On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0.
    Otherwise the files created in `dags`, `logs` and `plugins` will be created with root user.

2.  Create directory using:

    ```bash
    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

    For Windows same as above.

    Create `.env` file with the content below as:

    ```
    AIRFLOW_UID=50000
    ```

3. Download or import the docker setup file from airflow's website

   ```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   ```
4. Create "Dockerfile" use to build airflow container image.
5. Build image: docker-compose build
6. Initialize airflow db; docker-compose up airflow-init
7. Initialize all the other services: docker-compose up
8. Connect external postgres container to the airflow container by assigning the airflow_defult netwrk to the pgres container: see the yaml file
9. Check for postgres db access from the airflow container.


### SetUp GCP for Local System (Local Environment Oauth-authentication)
1. Create GCP PROJECT
2. Create service account: Add Editor and storage admin, storage object admins and bigquery admin
3. Create credential keys and download it
4. Change name and location
   ```
    cd ~ && mkdir -p ~/.google/credentials/
    mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
   ```
   AS ABOVE
   ```
   mv  /Users/abidakunabisoye/Downloads/alt-data-engr-1dfdbf9f8dbf.json ~/.google/credentials/google_credentials_01.json
   ```
4. Intall gcloud on system : open new terminal and run    gcloud-sdk : https://cloud.google.com/sdk/docs/install-sdk

    ```bash
    gcloud -v
    ```
  to see if its installed successfully
5. Set the google applications credentials environment variable
  ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/Users/path/.json-file"
  ```

  SAME AS ABOVE : run on terminal

  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS = "/Users/abidakunabisoye/.google/credentials/google_credentials_01.json"
  ```
6. Run gcloud auth application-default login
7. Redirect to the website and authenticate local environment with the cloud environment

## Enable API
1. Enable Identity  and Access management API
2. Enable IAM Service Account Credentials API


## Update docker-compose file and Dockerfile
1. Add google credentials "GOOGLE_APPLICATION_CREDENTIALS" and project_id  and bucket name
    ```
        GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'

        GCP_PROJECT_ID: "alt-data-engr"
        GCP_GCS_BUCKET: "dte-engr-alt"
    ```
2. Add the below line to the volumes of the airflow documentation

    ```
    ~/.google/credentials/:/.google/credentials:ro
    ```
3. Create requirements.txt inside the airflow folder and add dependencies as Repo
4. Create scripts folder inside the airflow folder and inside it create a entrypoint.sh file (paste dependencies from Repo)