from flask import Flask 
from flask_cors import CORS 
from flask_restx import Api
from .dataset import api as dataset_api 
from flask_pymongo import PyMongo
import os

def create_app():

    # create flask instance 
    app = Flask(__name__)

    app.config['MONGO_URI'] = os.getenv('MONGODB_CONNSTRING')
    app.config['DB_COLLECTION'] = 'biomarker_collection'

    CORS(app)
    mongo = PyMongo(app)
    app.mongo = mongo 

    # setup the api using the flask_restx library 
    api = Api(app, version = '1.0', title = 'Biomarker APIs', description = 'Biomarker Knowledgebase API')
    api.add_namespace(dataset_api)

    return app