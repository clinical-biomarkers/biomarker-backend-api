DEBUG = False 
TESTING = False 

SERVER = 'prd'
DB_NAME = 'biomarkerkbdb'
DB_USERNAME = 'biomarkeradmin'
DB_PASSWORD = 'biomarkerpass'
MONGO_URI = f'mongodb://{DB_USERNAME}:{DB_PASSWORD}@localhost:27017/{DB_NAME}?authSource={DB_NAME}'