import os 
from flask import Flask 
from flask_jwt_extended import JWTManager
from flask_cors import CORS 
from flask_restx import Api, Resource, fields
from .dataset import api as dataset_api 
from flask_pymongo import PyMongo

def create_app():

    # create flask instance 
    app = Flask(__name__, instance_relative_config = True)

    # load configurations  
    app.config['MONGO_URI'] = f'mongodb://running_biomarkerkb:27017/biomarkerkbdb'

    CORS(app)
    mongo = PyMongo(app)

    # setup the api using the flask_restx library 
    api = Api(app, version = '1.0', title = 'BiomarkerKB APIs', description = 'Documentation for the BiomarkerKB dataset APIs')
    api.add_namespace(dataset_api)

