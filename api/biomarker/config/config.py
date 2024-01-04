DEBUG = False 
TESTING = False 

SERVER = 'prd'
DB_AUTH = 'SCRAM-SHA-1'
DB_NAME = 'biomarkerkbdb'
DB_COLLECTION = 'biomarker_collection'
DB_USERNAME = 'biomarkeradmin'
DB_PASSWORD = 'biomarkerpass'
MONGO_URI = f'mongodb://{DB_USERNAME}:{DB_PASSWORD}@172.18.0.2:27017/{DB_NAME}?authMechanism={DB_AUTH}&authSource={DB_NAME}'