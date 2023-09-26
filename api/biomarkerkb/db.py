import bson 
from flask import current_app as app 
from flask import g 
from flask_pymongo import PyMongo

mongo = PyMongo() 

def get_db():
    if 'db' not in g:
        g.db = mongo.db
    return g.db 

def init_app(app):
    mongo.init_app(app)