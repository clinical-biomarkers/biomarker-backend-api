# API Structure

| Directory/File                | Description                                                                |
| ----------------------------- | -------------------------------------------------------------------------- |
| `biomarker/`                  | The actual api module.                                                     |
| `config.json`                 | Centralized configuration file for the API and the data loading processes. |
| `create_api_container.py`     | Creates the API docker container.                                          |
| `create_mongodb_container.py` | Creates the MongoDB docker container.                                      |
| `Dockerfile`                  | Dockerfile for the api image (used in `create_api_container.py`)           |
| `init_mongodb.py`             | Creates the database user on initial setup.                                |
| `requirements.txt`            | Requirements file for the API container.                                   |
| `setup.py`                    | Setup script for packaging the biomarker project.                          |
