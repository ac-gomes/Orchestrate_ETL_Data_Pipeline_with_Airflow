# Orchestrating a data pipeline using Apache Airflow - Demo

In this demo project, I aproach how to use apache airflow to orchestrate a simple data pipeline.

Workflow:
![pipeline workflow](/git_img/workflow.PNG)

Basically this data pipeline is consuming data from a REST API, doing a little data transformation and then saving the file in json format into s3 bucket.

Here I also demonstrate how to create a custom module and use it in your dags. For that just create a folder inside the 'dags' folder as follows below.

```bash
# The 'utils' and 'tempdata' are used as extra folders, where utils is used for custom modules and 'tmpdata' is for putting temporary data.

# as per yaml file theses folders needs be created in your docker container and your local env.

mkdir -p /sources/logs /sources/dags /sources/dags/utils /sources/plugins /sources/tmpdata

#after creating the folders you need to assign the airflow user permissions

chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins,tmpdata}
chown -R "${AIRFLOW_UID}:0" /sources/dags/utils

```

> ***NOTE:***<br>
> The airflow user permissions ```"${AIRFLOW_UID}:0"``` must be applied in  docker container and your local env.
>
> User permissions are applied to container folders as per yaml file configuration during build docker container image.
>
>For local folders you need to extract user ID ```"${AIRFLOW_UID}:0"``` and put it into a '.env' file then apply the user permission.

To extract AIRFLOW_UID:
```bash

echo -e "\nAIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> .env

 ```

> ***NOTE:***<br>
> I kept the original docker-compose file as a text file to compare both and see all modification that a did into current yaml file.

Another interesting point is the installation of libraries in the docker airflow container. This can be done through 'Dockerfile'. Basically I created a requirements.txt file and put there some libraries and apache airflow extensions.

In the 'Dockerfile' it is necessary put the apache airflow version, copy requirements.txt to docker container then run a 'pip install' see Dockerfile into this repo.

After creating the Dockerfile, it is necessary to extend the airflow docker container as per follows:
```bash

# This will use the current airflow container to rebuild the new image withe new libraries

docker build . --tag extending_airflow:lates

```
> ***NOTE:***<br>
> After running the above line. It is need change airflow image description into yaml file.
>
> **from** ````image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.6.0}````  **to** ````image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}````
>
>Yes, You are right, We are versioning our docker container with the tag ``extending_airflow:latest```

And finally it is need rebuild the 'airflow-webserver' and 'airflow-scheduler':
```bash

docker compose up -d --no-deps --build airflow-webserver airflow-scheduler

```

That's all, enjoy!

You can see more datails about the data source in this link
[The OpenSky Network](https://opensky-network.org).