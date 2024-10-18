# Initial Server Setup 

This usage guide should be followed if setting up this repository on a server from scratch.

- [Server Dependencies](#server-dependencies)
- [Clone the Repository](#clone-the-repository)
- [Creating and Starting the Docker Container for the MongoDB](#creating-and-starting-the-docker-container-for-the-mongodb)
- [Initialize MongoDB User](#initialize-mongodb-user)
- [Creating and Starting Docker Container for the API](#creating-and-starting-docker-container-for-the-api)
- [Managing the Docker Containers with a Service File](#managing-the-docker-containers-with-a-service-file)

## Server Dependencies

The following must be available on your server: 
- wheel 
- pymongo 
- docker 

## Clone the Repository

Clone the reporistory onto the target machine using `git clone`. 

## Creating and Starting the Docker Container for the MongoDB

Navigate to the `/api` subdirectory and run the `create_mongodb_container.py` script: 

```bash 
cd api 
python create_mongodb_container.py $SER 
docker ps --all 
```

The first command will navigate you into the api directory. The second command will run the script. The `$SER` argument should be replaced with the server you are running on (dev, tst, beta, prd). The last command lists all docker containers. You should see the docker mongodb docker container that the script created, in the format of `running_biomarker-api_mongo_$SER` where `$SER` is the specified server.

Start the MongoDB container using the `docker start {container}` command or by creating a service file (instructions for this in the [Managing the Docker Containers with a Service File](#managing-the-docker-containers-with-a-service-file)) section. 

## Initialize MongoDB User

Stay in the `/api` subdirectory and run the `init_mongodb.py` script: 

```bash
python init_mongodb.py $SER
```

Where the `$SER` argument is the specified server. This should only be run once on initial setup for each server.

## Creating and Starting Docker Container for the API

To create the API container, run the `create_api_container.py` script from the `/api` directory. 

```bash 
python create_api_container.py $SER
docker ps --all
```

The first command will run the script. The `$SER` argument should be replaced with the server you are running on. The last command lists all docker containers. You should see the api container that the script created, in the format of `running_biomarker-api_api_$SER` where `$SER` is the specified server. Start the docker container with the `docker start` command or create a service file (recommended).

## Managing the Docker Containers with a Service File

The service files should be located at `/usr/lib/systemd/system/` and named something along the lines of `docker-biomarker-api-mongo-{SER}.service` (using the MongoDB container as an example) where `{SER}` indicates the server. Place the following content in it: 

```
[Unit]
Description=Biomarker Backend API MongoDB Container
Requires=docker.service
After=docker.service

[Service]
Restart=always
ExecStart=/usr/bin/docker start -a running_biomarker-api_mongo_$SER
ExecStop=/usr/bin/docker stop -t 2 running_biomarker-api_mongo_$SER

[Install]
WantedBy=default.target
```

This will ensure the container is automatically restarted in case of server reboot. You can start/stop the container with the following commands:

```
$ sudo systemctl daemon-reload 
$ sudo systemctl enable docker-biomarker-api-mongo-{SER}.service
$ sudo systemctl start docker-biomarker-api-mongo-{SER}.service
$ sudo systemctl stop docker-biomarker-api-mongo-{SER}.service
```

## Loading an Existing Data Version

To load an existing data version, go to https://data.biomarkerkb.org/ftp/ and select the desired data version directory. TODO
