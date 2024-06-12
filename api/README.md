# API Setup

## Directory Structure

| Directory/File                | Description                                                                                            |
| ----------------------------- | ------------------------------------------------------------------------------------------------------ |
| `biomarker/`                  | The biomarker api module.                                                                              |
| `clear_collections.py`        | Clears some supplementary collections (can clear and or all of the cache, log, and error collections). |
| `config.json`                 | Config file for the api configuration.                                                                 |
| `create_api_container.py`     | Creates the api docker container.                                                                      |
| `create_index.py`             | Creates the MondoDB text index on the `all_text` field.                                                |
| `create_mongodb_container.py` | Creates the MongoDB docker container.                                                                  |
| `Dockerfile`                  | Dockerfile for the api image (used in `create_api_container.py`)                                       |
| `init_mongodb.py`             | Creates the database user.                                                                             |
| `misc_functions.py`           | Useful miscellaneous functions used throughout the various scripts.                                    |
| `requirements.txt`            | Requirements file for the api image.                                                                   |
| `setup.py`                    | Setup script for packaging the biomarker project.                                                      |
