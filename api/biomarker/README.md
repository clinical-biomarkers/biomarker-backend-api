# API 

All endpoints are hosted at the root URL https://hivelab.biochemistry.gwu.edu/biomarker/api/.

- [Endpoints](#endpoints)
    - [Dataset Endpoints](#dataset-endpoints)
    - [ID Endpoints](#id-endpoints)
- [Models](#models)
- [Directory Strucutre](#directory-structure)

## Endpoints 

### Dataset Endpoints 

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

`GET /id/getbiomarker?biomarker_id={biomarker_id}`  
- Parameters:
    - `biomarker_id`: The biomarker ID to query for. 
- Returns: 
    - `200 Success`: A single biomarker record corresponding to the `biomarker_id` param. 
    - `400 No biomarker ID provided`: Error indicating param was not included. 
    - `404 Not Found`: Error indicating biomarker ID was not found. 

## Models 

The data models can be seen [here](data_models.py).

## Directory Structure 

| Directory/File                |                                                                   |
|-------------------------------|-------------------------------------------------------------------|
| `__init__.py`                 | Entry point for the api module.                                   |
| `dataset.py`                  | The general dataset API endpoints.          | 
| `id.py`                       | The biomarker ID specific API endpoints.     |
| `data_models.py`              | Defines the data models for the API documentation. |
