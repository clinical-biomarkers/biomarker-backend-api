# API 

## Directory Structure 

| Directory/File                |                                                                   |
|-------------------------------|-------------------------------------------------------------------|
| `__init__.py`                 | Entry point for the api module.                                   |
| `dataset.py`                  | The local dataset module, which defines the dataset API.          | 

## Documentation 

## Notes 

### `__init__.py`

[Line 10] The Flask instance is created with name `__name__`. In Python, `__name__` evaluates to the name of the current module.  

[Line 12] Cross-Origin Resource Sharing (CORS) middleware is applied to the Flask app. CORS is a security feature implemented in web browsers to prevent requests to different domains unless the target domain indicates it's allowed. By just using `CORS(app)`, we are allowing any domain to make requests to the Flask application. Eventually we might lock it down further by restricting which domains are allowed using the `origins` parameter (e.g. `CORS(app, origins = ['http://example.com'])`).

[Line 15] We use the `flask_restx` extension for Flask to create the RESTful APIs. The `flask_restx` library extends base Flask library to add utilities specifically designed for creating APIs. 

### `dataset.py`

[]