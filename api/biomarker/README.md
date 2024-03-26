# API 

All endpoints are hosted at the root URL https://hivelab.biochemistry.gwu.edu/biomarker/api/.

- [Endpoints](#endpoints)
    - [Dataset Endpoints](#dataset-endpoints)
    - [ID Endpoints](#id-endpoints)
    - [Canonical ID Endpoints](#canonical-id-endpoints)
    - [Unreviewed Endpoints](#unreviewed-endpoints)
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
        - Returns `{'biomarkers': <return_data>}, 200`
    - `400 Bad Request`: Error indicating an invalid sample size was provided (sample must be positive integer). 
    - `500 Database query failed`: Error indicating there was an error querying the database on the client side.
        - Returns `{'message': 'Database query failed.'}, 500`
  
`GET /dataset/randomsample?sample={sample}`
- Parameters:
    - `sample`: The number of samples to return (default = 1).
- Returns:
    - `200 Success`: The random subset of biomarker records.
        - Returns `{'biomarkers': <return_data>}, 200`
    - `400 Bad Request`: Error indicating an invalid sample size was provided (sample must be positive integer). 
    - `500 Database query failed`: Error indicating there was an error querying the database on the client side.
        - Returns `{'message': 'Database query failed.'}, 500`

### ID Endpoints 

ID API endpoints that retrieve specific biomarker records from the reviewed data.

`GET /id/{biomarker_id}`  
- Returns: 
    - `200 Success`: A single biomarker record corresponding to the `biomarker_id` param. 
        - Returns `{'biomarker': <return_data>}, 200`
    - `400 No biomarker ID provided`: Error indicating param was not included. 
        - Returns `{'message': 'No biomarker ID provided'}, 400`
    - `404 Not Found`: Error indicating biomarker ID was not found. 
        - Returns `{'message': 'Biomarker ID not found'}, 404`
    - `500 Database query failed`: Error indicating there was an error querying the database on the client side.
        - Returns `{'message': 'Database query failed.'}, 500`

`POST /id/set`
- JSON Body:
    - Array of biomarker ID's to query for under the key `biomarker_ids`. 
- Returns: 
    - `200 Success (full or partial)`: The biomarkers records (invalid ID's will be silently skipped).
        - Returns `{'biomarkers': <return_data>}, 200`
    - `400 No biomarker ID provided or unexpected error`: Error indicating either no IDs were passed or they were in the incorrect format or data type. 
        - Returns `{'message': 'No biomarker IDs provided or invalid format'}, 400`
    - `404 Not Found`: Error indicating biomarker ID's were not found. 
        - Returns `{'message': 'Biomarker IDs not found'}, 404`
    - `500 Database query failed`: Error indicating there was an error querying the database on the client side.
        - Returns `{'message': 'Database query failed.'}, 500`

### Canonical ID Endpoints

Canonical ID endpoints that retrieve specific biomarker records from the reviewed data.

`GET /canonical_id/{canonical_id}`
- Returns:
    - `200 Success`: All the biomarker records under a particular canonical ID.
        - Returns `{'biomarkers': <return_data>}, 200`
    - `400 No canonical ID provided`: Error indicating no canonical ID was provided.
        - Returns `{'message': 'No canonical ID provided.}, 400`
    - `404 Not found`: Error indicating nothing was found for provided canonical ID.
        - Returns `{'message': 'Canonical ID not found.'}, 404`
    - `500 Database query failed`: Error indicating there was an error querying the database on the client side.
        - Returns `{'message': 'Database query failed.'}, 500`

`POST /canonical_id/set`
- JSON Body: 
    - Array of canonical ID strings to query for under the key `canonical_ids`.
- Returns:
    - `200 Success (full or partial)`: The biomarkers associated with the provided lists of Canonical IDs.
        - Returns `{'biomarkers': <return_data>}, 200`
    - `400 No canonical IDs provided or invalid format`: Error indicating either no IDs were passed or they were in the incorrect format or data type.
        - Returns `{'message': 'No canonical IDs provided or invalid format.}, 400`
    - `404 Not found`: Error indicating nothing was found for provided canonical IDs.
        - Returns `{'message': 'Canonical IDs not found}, 404`
    - `500 Database query failed`: Error indicating there was an error querying the database on the client side.
        - Returns `{'message': 'Database query failed.'}, 500`

### Unreviewed Endpoints

API endpoints that query from the unreviewed data the documentation/behavior for these endpoints match their corresponding reviewed endpoints above. 

`GET /unreviewed/getall?page={page}&per_page={per_page}`  

`GET /unreviewed/randomsample?sample={sample}`  

`GET /unreviewed/id/{biomarker_id}`  

`POST /unreviewed/ids`  

`GET /unreviewed/canonical/{canonical_id}`  

`POST /unreviewed/canonical/set`

## Directory Structure 

| Directory/File                |                                                                   |
|-------------------------------|-------------------------------------------------------------------|
| `__init__.py`                 | Entry point for the api module.                                   |
| `canonical_id.py`             | The canonical ID API endpoints. |
| `dataset.py`                  | The general dataset API endpoints.          | 
| `id.py`                       | The biomarker ID specific API endpoints.     |
| `unreviewed.py`               | The API endpoints to query the unreviewed data. |
