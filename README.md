# Biomarkerkb Backend Dataset Viewer

Work in progress. 

- [Server Requirements](#server-requirements)
- [Getting Started](#getting-started)
    - [Clone the Repository](#clone-the-repository)
    - [Creating and Starting Docker Container for MongoDB](#creating-and-starting-docker-container-for-mongodb)
    - [Initialize MongoDB User](#initialize-mongodb-user)
    - [Populate Database](#populate-database)
    - [Creating and Starting Docker Container for the APIs](#creating-and-starting-docker-container-for-the-apis)
- [Config Files](#config-files)
    - [API Config Definitions](#api-config-definitions)
    - [App Config Definitions](#app-config-definitions)
- [Background Information](#background-information)
    - [MongoDB Overview](#mongodb-overiew)
    - [Docker Implementation](#docker-implementation)
        - [Bind-Mounted Directory](#bind-mounted-directory)
        - [Data Storage Structure](#data-storage-structure)


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
git clone [placeholder]
```

## Creating and Starting Docker Container for MongoDB 

Navigate to the `/api` subdirectory and run the `create_mongodb_container.py` script: 

```bash 
cd api 
python create_mongodb_container.py -s $SER 
docker ps --all 
```

The first command will navigate you into the api directory. The second command will run the script. The `$SER` argument should be replaced with the server you are running on (dev, tst, beta, prd). The last command lists all docker containers. You should see the docker mongodb docker container that the script created, in the format of `running_biomarkerkb_mongo_$SER` where `$SER` is the specified server. 

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

## Creating and Starting Docker Container for the APIs 

To create the API container, run the `create_api_container.py` script from the `/api` directory. 

```bash 
python create_api_container.py -s $SER
docker ps --all
```

The first command will run the script. The `$SER` argument should be replaced with the server you are running on (dev, tst, beta, prd). The last command lists all docker containers. You should see the api container that the script created, in the format of `running_biomarkerkb_api_$SER` where `$SER` is the specified server. 

# Config Files

The config files can be found at `/api/config.json` and `/app/config.json`. 

## API Config Definitions

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
            "server": "", 
            "port": "",
            "sender": ""
        },
        "data_path": "prefix filepath for the bind-mounted directory",
        "search_options": {
            "metadata": "",
            "data": ""
        },
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

## App Config Definitions

# Background Information 

## MongoDB Overiew

MongoDB is a document-oriented database manager. In MongoDB, a database is a container for collections. Collections can be thought of conceptually as tables (such as in a relational database). Collections hold documents. Documents are analogous to rows in a relational database table. Documents are stored in a JSON-like format called BSON.  

In MongoDB, databases and collections are created lazily. The database is not actually created until some data is written to them, simply referencing a database does not create it.  

MongoDB's user and role information is stored in the `admin` database, so users and corresponding roles are applied immediately even if the database itself doesn't physically exist yet. 

## Docker Implementation

### Bind-Mounted Directories  

Our created Docker Mongo container mounts a volume from the host machine to the container. Specifically, in the `create_mongodb_container.py` file, the following portion of the Docker command creates the volume mount.  
```bash 
-v {data_path}/db/{server}:/data/db
``` 
This means that a specific directory on the host machine (`{data_path}/db/{server}`) is made accessible within the container at the specified file path, `/data/db` (this is the default directory inside the Mongo container where MongoDB sotres its own internal database files, including data files, index files, and journal files).  

The created Docker api container also includes the `-v` flag. However, this container has two volume mounts. In the `create_api_container.py` file, the following portion fo the Docker command creates the volume mounts. 
```bash
-v {data_path}:{data_path} -v /software/pipes:/hostpipe 
```

### Data Storage Structure 

As discussed above, the `{data_path}/db/{server}` directory is dedicated to MongoDB's internal storage. The seed data (CSV file) should be stored in a different location on the host machine. 