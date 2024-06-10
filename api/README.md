# API Setup

## Directory Structure

| Directory/File                |                                                                     |
| ----------------------------- | ------------------------------------------------------------------- |
| `biomarker/`                  | The biomarker api module.                                           |
| `config.json`                 | Config file for the api setup.                                      |
| `create_api_container.py`     | Creates the api container.                                          |
| `create_mongodb_container.py` | Creates the initial MongoDB container.                              |
| `Dockerfile`                  | Dockerfile for the api image (used in `create_api_container.py`)    |
| `id_assign.py`                | Handles the biomarker ID assignments.                               |
| `id.py`                       | Defines the backend logic for the ID assignment system.             |
| `init_mongodb.py`             | Creates the database user.                                          |
| `load_data.py`                | Loads the MongoDB collection (`biomarker_collection`).              |
| `misc_functions.py`           | Useful miscellaneous functions used throughout the various scripts. |
| `requirements.txt`            | Requirements file for the api image.                                |
| `setup.py`                    | Setup script for packaging the biomarker project.                   |
