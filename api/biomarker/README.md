# API 

All endpoints are hosted at the root URL https://hivelab.biochemistry.gwu.edu/biomarker/api/.

- [Endpoints](#endpoints)
    - [Dataset Endpoints](#dataset-endpoints)
    - [ID Endpoints](#id-endpoints)
    - [Unreviewed Endpoints](#unreviewed-endpoints)
- [Models](#models)
- [Directory Strucutre](#directory-structure)

## Endpoints 

### Dataset Endpoints 

General API Endpoints that query on the reviewed data. 

`GET /dataset/getall?page={page}&per_page={per_page}`
- Parameters:
    - `page`: The page number to return (default = 1).
    - `per_page`: The number of records to return per page (default = 50).
- Returns:
    - `200 Success`: The biomarker records. 
  
`GET /dataset/randomsample?sample={sample}`
- Parameters:
    - `sample`: The number of samples to return (default = 1).
- Returns:
    - `200 Success`: The random subset of biomarker records.
    - `400 Bad Request`: Error indicating an invalid sample size was provided (sample must be positive integer). 

### ID Endpoints 

ID API endpoints that retrieve specific biomarker records from the reviewed data.

`GET /id/{biomarker_id}`  
- Parameters:
    - `biomarker_id`: The biomarker ID to query for. 
- Returns: 
    - `200 Success`: A single biomarker record corresponding to the `biomarker_id` param. 
    - `400 No biomarker ID provided`: Error indicating param was not included. 
    - `404 Not Found`: Error indicating biomarker ID was not found. 

`POST /id/set`
- JSON Body:
    - Array of biomarker ID's to query for. 
- Returns: 
    - `200 Success (full or partial)`: The biomarkers records (invalid ID's will be silently skipped).
    - `400 No biomarker ID provided or unexpected error`: Error indicating unexpected error or no ID set provided.
    - `404 Not Found`: Error indicating biomarker ID's were not found. 

### Unreviewed Endpoints

API endpoints that query from the unreviewed data. 

`GET /unreviewed/getall?page={page}&per_page={per_page}`
- Parameters:
    - `page`: The page number to return (default = 1).
    - `per_page`: The number of records to return per page (default = 50).
- Returns:
    - `200 Success`: The biomarker records. 

`GET /unreviewed/randomsample?sample={sample}`
- Parameters:
    - `sample`: The number of samples to return (default = 1).
- Returns:
    - `200 Success`: The random subset of biomarker records.

`GET /unreviewed/id/{biomarker_id}`  
- Parameters:
    - `biomarker_id`: The biomarker ID to query for. 
- Returns: 
    - `200 Success`: A single biomarker record corresponding to the `biomarker_id` param. 
    - `400 No biomarker ID provided`: Error indicating param was not included. 

## Models 

The data models can be seen [here](data_models.py).

## Directory Structure 

| Directory/File                |                                                                   |
|-------------------------------|-------------------------------------------------------------------|
| `__init__.py`                 | Entry point for the api module.                                   |
| `dataset.py`                  | The general dataset API endpoints.          | 
| `id.py`                       | The biomarker ID specific API endpoints.     |
| `data_models.py`              | Defines the data models for the API documentation. |
