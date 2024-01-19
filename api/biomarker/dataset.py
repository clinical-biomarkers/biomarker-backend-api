from flask_restx import Namespace, Resource, inputs
from flask import current_app as app 
from flask import request
from .data_models import data_model

api = Namespace('dataset', description = 'Dataset operations API')

def get_collection_name():
    return app.config['DB_COLLECTION']

class DatasetGetAll(Resource):
    ''' Get the entire dataset. 
    '''
    @api.doc(description = 'Returns all the data records Supports pagination and per page filtering.')
    @api.param('page', 'The page number to return.', type = int, default = 1)
    @api.param('per_page', 'The number of records to return per page.', type = int, default = 50)
    @api.response(200, 'Success', data_model)
    def get(self):
        page = request.args.get('page', default = 1, type = int)
        per_page = request.args.get('per_page', default = 50, type = int)
        data = app.mongo_db[get_collection_name()].find().skip((page - 1) * per_page).limit(per_page)
        return list(data)

class DatasetRandomSample(Resource):
    ''' Get a random subset of data. 
    '''
    @api.doc(description = 'Returns a random subset of the data. The sample size must be a positive integer.')
    @api.param('sample', 'The number of samples to return.', type = inputs.positive, default = 1)
    @api.response(200, 'Success', data_model)
    @api.response(400, 'Bad Request')
    def get(self):
        try:
            sample_size = request.args.get('sample', default = 1, type = int)
        except ValueError:
            return {'message': 'Invalid sample size provided. Sample must be a positive integer.'}, 400
        data = app.mongo_db[get_collection_name()].aggregate([{'$sample': {'size': sample_size}}])
        return list(data)

api.add_resource(DatasetGetAll, '/getall')
api.add_resource(DatasetRandomSample, '/randomsample')