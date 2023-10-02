DEBUG = False 
TESTING = False 

SERVER = 'prd'
DB_AUTH = 'SCRAM-SHA-1'
DB_NAME = 'biomarkerkbdb'
DB_COLLECTION = 'biomarker_collection'
DB_USERNAME = 'biomarkeradmin'
DB_PASSWORD = 'biomarkerpass'
MONGO_URI = f'mongodb://{DB_USERNAME}:{DB_PASSWORD}@localhost:27017/?authMechanism={DB_AUTH}&authSource={DB_NAME}'