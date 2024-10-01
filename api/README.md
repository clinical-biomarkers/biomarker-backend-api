# API Setup

## Directory Structure

| Directory/File                | Description                                                                                            |
| ----------------------------- | ------------------------------------------------------------------------------------------------------ |
| `biomarker/`                  | The biomarker api module.                                                                              |
| `config.json`                 | Config file for the API configuration.                                                                 |
| `create_api_container.py`     | Creates the api docker container.                                                                      |
| `create_mongodb_container.py` | Creates the MongoDB docker container.                                                                  |
| `Dockerfile`                  | Dockerfile for the api image (used in `create_api_container.py`)                                       |
| `init_mongodb.py`             | Creates the database user.                                                                             |
| `requirements.txt`            | Requirements file for the api image.                                                                   |
| `setup.py`                    | Setup script for packaging the biomarker project.                                                      |
