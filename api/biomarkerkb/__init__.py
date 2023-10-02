from flask import Flask 
from flask_cors import CORS 
from flask_restx import Api
from .dataset import api as dataset_api 
from .db import init_app as init_db_app

def create_app():

    # create flask instance 
    app = Flask(__name__)

    app.config['ENV'] = 'development'

    if app.config['ENV'] == 'production':
        app.config.from_pyfile('./config/config.py')
    else:
        app.config.from_pyfile('./config/config_dev.py')

    CORS(app)
    init_db_app(app)

    # setup the api using the flask_restx library 
    api = Api(app, version = '1.0', title = 'BiomarkerKB APIs', description = 'Biomarker Knowledgebase API')
    api.add_namespace(dataset_api)

    return app