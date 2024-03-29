from flask import Flask 
from flask_cors import CORS 
from flask_restx import Api
from .dataset import api as dataset_api 
from .id import api as id_api
from .unreviewed import api as unreviewwed_api
from pymongo import MongoClient
import os

MONGO_URI = os.getenv('MONGODB_CONNSTRING')
DB_NAME = 'biomarkerdb_api'
DB_COLLECTION = 'biomarker_collection'
COLLISION_COLLECTION = 'collision_collection'

def create_app():

    # create flask instance 
    app = Flask(__name__)

    CORS(app)

    # initialize mongo client 
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[DB_NAME]
    app.mongo_db = mongo_db
    app.config['DB_COLLECTION'] = DB_COLLECTION
    app.config['COLLISION_COLLECTION'] = COLLISION_COLLECTION

    # setup the api using the flask_restx library 
    api = Api(app, version = '1.0', title = 'Biomarker APIs', description = 'Biomarker Knowledgebase API')
    api.add_namespace(dataset_api)
    api.add_namespace(id_api)
    api.add_namespace(unreviewwed_api)

    return app