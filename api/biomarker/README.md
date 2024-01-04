# API 

- [Documentation](#documentation)
    - [Endpoints](#endpoints)
    - [Models](#models)
- [Directory Strucutre](#directory-structure)

## Documentation 

### Endpoints 

`GET /dataset/getall`  
Returns the entire dataset. 
- Example call: `http://{HOST}:8081/dataset/getall`
- Parameters: 
    - x-fields (optional): optional fields mask
- Return schema: [`data_model`](#models)

---

`GET /dataset/randomsample`  
Returns a random subset of the dataset.
- Example call: `http://{HOST}:8081/dataset/randomsample?sample={NUMBER}`
- Parameters:
    - sample (optional, default = 1): number of samples to return
    - x-fields (optional): optional fields mask
- Return schema: [`data_model`](#models)

### Models 

`data_model`: 
| Field                 | Type      | Description                       |
|-----------------------|-----------|-----------------------------------|

## Directory Structure 

| Directory/File                |                                                                   |
|-------------------------------|-------------------------------------------------------------------|
| `__init__.py`                 | Entry point for the api module.                                   |
| `dataset.py`                  | The local dataset module, which defines the dataset API.          | 
| `config/`                     | Config files for flask instance.                                  |