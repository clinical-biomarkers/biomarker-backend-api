# Biomarker API Entrypoint

The [`__init__.py`](../__init__.py) is the entry point for the biomarker API module. Functionality: 

- In order to inform type checking linters of attribute types, a custom class called `CustomFlask` is created which is a subclass of the `Flask` type.
- Creates the `CustomFlask` app instance.
- Sets up the API logger (named `app.log`).
- Reads in the hit score configuration file and stores it on the app instance.
- Creates the MongoDB database handle and stores it on the app instance.
- Sets up the API namespaces.
