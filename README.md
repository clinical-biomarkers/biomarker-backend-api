# Biomarkerkb Backend Dataset Viewer

Work in progress. 

- [Server Requirements](#server-requirements)
- [Getting Started](#getting-started)
    - [Clone the Repository](#clone-the-repository)
    - [Creating and Starting Docker Container for MongoDB](#creating-and-starting-docker-container-for-mongodb)
    - [Initialize MongoDB User](#initialize-mongodb-user)
    - [Populate Database](#populate-database)
    - [Creating and Starting Docker Container for the APIs](#creating-and-starting-docker-container-for-the-apis)
- [Config File Definitions](#config-file-definitions)

API documentation can be found [here](./api/biomarker/README.md).

# Server Requirements 

The following must be available on your server: 
- wheel 
- pymongo 
- httpie 
- jsonref
- jsonschema
- Node.js and npm 
- docker 

# Getting Started 

## Clone the Repository

Clone the repository onto your host machine:

```bash
git clone https://github.com/biomarker-ontology/biomarkerkb-backend-datasetviewer.git
```

## Creating and Starting Docker Container for MongoDB 

Navigate to the `/api` subdirectory and run the `create_mongodb_container.py` script: 

```bash 
cd api 
python create_mongodb_container.py -s $SER 
docker ps --all 
```

The first command will navigate you into the api directory. The second command will run the script. The `$SER` argument should be replaced with the server you are running on (dev, tst, beta, prd). The last command lists all docker containers. You should see the docker mongodb docker container that the script created, in the format of `running_biomarkerkb_mongo_$SER` where `$SER` is the specified server.

Expected output should look something like this:

```bash
Found container: running_biomarkerkb_mongo_dev
Found network: biomarkerkb_backend_network_dev
e6c50502da1b

5e1146780c4fa96a6af6e4555cd119368e9907c4d50ad4790f9f5e54e13bf043
7baa10fed7e89181c902b24f7af9991e07b00c0f3f31f7df58cccba80aef1a2c
```

The first two print statements indicate that an old instance of the container and docker network were found. These will be removed by the script. The `e6c50502da1b` is the ID of the removed container. This indicates that the `docker rm -f ...` command executed successfully and removed the existing container. The second to last line is the ID of the newly created docker network. The last line is the ID of the newly created docker container. 

## Initialize MongoDB User 

Stay in the `/api` subdirectory and run the `init_mongodb.py` script: 

```bash
python init_mongodb.py -s $SER
```

Where the `$SER` argument is the specified server. This should only be run once. 

## Populate Database 

To load data, run the `load_data.py` script from the `/api` directory. 

```bash 
python load_data.py -s $SER -f $FP 
```

Where the `$SER` argument is the specified server and `$FP` is the filepath to the seed csv data. 

If testing on a local machine, you can test using code or a GUI option such as MongoDB Compass. The connection string should look something along the lines of:

```bash 
mongodb://biomarkeradmin:biomarkerpass@localhost:27017/?authMechanism=SCRAM-SHA-1&authSource=biomarkerkbdb
```

## Creating and Starting Docker Container for the APIs 

To create the API container, run the `create_api_container.py` script from the `/api` directory. 

```bash 
python create_api_container.py -s $SER
docker ps --all
```

The first command will run the script. The `$SER` argument should be replaced with the server you are running on (dev, tst, beta, prd). The last command lists all docker containers. You should see the api container that the script created, in the format of `running_biomarkerkb_api_$SER` where `$SER` is the specified server. 

After the container is up and running, you can manually test the API using Python's `request` library, curl, or in the web browser. An example API call:

```bash
http://localhost:8081/dataset/randomsample?sample=5
```

API documentation can be found [here](https://github.com/biomarker-ontology/biomarkerkb-backend-datasetviewer/tree/main/api/biomarkerkb#endpoints).

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
        "mail":{
            "server": "not used for now", 
            "port": "not used for now",
            "sender": "not used for now"
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
            "biomarkerkb": {
                "db": "biomarkerkbdb database",
                "user": "biomarkerkb database username",
                "password": "biomarkerkb database password"
            }
        }
    }
```
