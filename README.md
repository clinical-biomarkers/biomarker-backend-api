# Biomarker Backend API 

- [Server Requirements](#server-requirements)
- [Getting Started](#getting-started)
    - [Clone the Repository](#clone-the-repository)
    - [Creating and Starting Docker Container for MongoDB](#creating-and-starting-docker-container-for-mongodb)
    - [Initialize MongoDB User](#initialize-mongodb-user)
    - [Assign Biomarker IDs](#assign-biomarker-ids)
    - [Populate Database](#populate-database)
    - [Copy Files](#copy-files)
    - [Creating and Starting Docker Container for the APIs](#creating-and-starting-docker-container-for-the-apis)
- [Config File Definitions](#config-file-definitions)
- [Internal Backend Documentation](#internal-backend-documentation)
    - [ID Assignment System](#id-assignment-system)

API documentation can be found [here](./api/biomarker/README.md).

# Server Requirements 

The following must be available on your server: 
- wheel 
- pymongo 
- docker 

# Getting Started 

## Clone the Repository

Clone the repository onto your host machine using `git clone`. 

## Creating and Starting Docker Container for MongoDB 

Navigate to the `/api` subdirectory and run the `create_mongodb_container.py` script: 

```bash 
cd api 
python create_mongodb_container.py -s $SER 
docker ps --all 
```

The first command will navigate you into the api directory. The second command will run the script. The `$SER` argument should be replaced with the server you are running on (dev, tst, beta, prd). The last command lists all docker containers. You should see the docker mongodb docker container that the script created, in the format of `running_biomarker-api_mongo_$SER` where `$SER` is the specified server.

Expected output should look something like this:

```bash
Found container: running_biomarker-api_mongo_{SER}
Found network: biomarker-api_backend_network_{SER}
e6c50502da1b

5e1146780c4fa96a6af6e4555cd119368e9907c4d50ad4790f9f5e54e13bf043
7baa10fed7e89181c902b24f7af9991e07b00c0f3f31f7df58cccba80aef1a2c
```

The first two print statements indicate that an old instance of the container and docker network were found. These will be removed by the script. The `e6c50502da1b` is the ID of the removed container. This indicates that the `docker rm -f ...` command executed successfully and removed the existing container. The second to last line is the ID of the newly created docker network. The last line is the ID of the newly created docker container. 

Start the MongoDB container using the `docker start {container}` command or by creating a service file. The service file should be located at `/usr/lib/systemd/system/` and named something along the lines of `docker-biomarker-api-mongo-{SER}.service`. Place the following content in it: 

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

## Initialize MongoDB User 

Stay in the `/api` subdirectory and run the `init_mongodb.py` script: 

```bash
python init_mongodb.py -s $SER
```

Where the `$SER` argument is the specified server. This should only be run once on initial setup for each server. 

## Assign Biomarker IDs 

To assign biomarker IDs to your new data, run the `id_assign.py` script from the `/api` directory. This script can only be run from the `tst` server. More information about the under the hood implementation of the ID generation is available in the [ID Assignment System](#id-assignment-system) section. Whether or not a collision is found, the record will be assigned its corresponding `biomarker_id`. An additional key, `collision` is added to the record, with a value of `0` indicating no collision and `1` indicating a collision. This will be used during the data load process and removed. If a collision is found, an entry will be created for it in the file's corresponding collision report. 

```bash 
python id_assign.py -s $SER
```

## Populate Database 

To load data, run the `load_data.py` script from the `/api` directory. You have to complete the ID assignment steps and handle collisions before this step can be completed. The data should be in the filepath `/data/shared/biomarkerdb/generated/datamodel/new_data/current` where `current/` is a symlink to the current version directory. 

```bash 
python load_data.py -s $SER
```

Where the `$SER` argument is the specified server. 

The code will do some preliminary checks on the data that is to be loaded. It will make sure that each record has a valid formatted biomarker ID. 

## Copy Files

After the data has been properly ID assigned, collisions have been handled, and the `tst` and `prd` databases have been loaded, run the `copy_files.py` script to copy the files into the `existing_data` directory. This is the master directory which holds all the data files that have been loaded into the backend API. This must be run from the `tst` server.

```bash 
python copy_files.py -s tst
```

## Creating and Starting Docker Container for the APIs 

To create the API container, run the `create_api_container.py` script from the `/api` directory. 

```bash 
python create_api_container.py -s $SER
docker ps --all
```

The first command will run the script. The `$SER` argument should be replaced with the server you are running on (tst, prd). The last command lists all docker containers. You should see the api container that the script created, in the format of `running_biomarker-api_api_$SER` where `$SER` is the specified server. Start the docker container with the `docker start` command or create a service file as shown above (recommended).

API documentation can be found [here](./api/biomarker/README.md).

# Config File Definitions

```json
    {
        "project": "project name",
        "api_port": {
            "prd": "production server api port",
            "beta": "beta server api port",
            "tst": "test server api port",
            "dev": "development server api port"
        },
        "data_path": "prefix filepath for the bind-mounted directory",
        "dbinfo": {
            "dbname": "database name",
            "port": { 
                "prd": "production server database port",
                "beta": "beta server database port",
                "tst": "test server database port",
                "dev": "development server database port"
            },
            "bridge_network": "docker bridge network name",
            "admin": {
                "db": "admin database name (admin)",
                "user": "admin username",
                "password": "admin password"
            },
            "biomarkerdb_api": {
                "db": "database name",
                "collection": "data collection",
                "id_collection": "ID map",
                "user": "biomarker database username",
                "password": "biomarker database password"
            }
        }
    }
```

# Internal Backend Documentation

## ID Assignment System

The high level workflow for the ID assignment system is as follows:

```mermaid
flowchart TD
    A[Data Release with JSON Data] --> B{id_assign.py}
    B --> C[Extracts the core field elements]
    C --> D[Preprocesses/cleans core field values]
    D --> E[Concatenates core fields in alphabetical order]
    E --> F[Resulting string is hashed]
    F --> G[Check the id_map collection for potential collision]
    G --> H[If collision:\nMark as collision and add to collision report]
    H --> K[Investigate collisions and handle accordingly]
    G --> I[If no collision:\nMap and increment new ordinal ID mapped to hash value]
    I --> J[Assign new ordinal ID to record]
```

The core fields are defined as in the Biomarker-Partnership RFC (which can be found in [this](https://github.com/biomarker-ontology/biomarker-partnership) repository). 

When loading data into the project, the core field values are extracted, cleaned, sorted, and concatenated. The resulting string is hashed and that hash value is checked for a potential collision in the MongoDB `id_map_collection`. If no collision is found, a new entry is added to the `id_map_collection` which stores the hash value and maps an incremented human readable ordinal ID. The core values string that generated the hash value is also stored with each entry for potential debugging purposes. 

Example: 
```json 
{
    "hash_value": "<VALUE>",
    "ordinal_id": "<VALUE>",
    "core_values_str": "<VALUE>"
}
```

The ordinal ID format is two letters followed by four digits. The ID space goes from `AA0000` to `ZZ9999`.

This hash system combined with a MongoDB unique field index allows for scalable and fast ID assignments. 