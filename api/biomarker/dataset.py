from flask_restx import Namespace, Resource, fields
from flask import current_app as app 
from flask import request

api = Namespace ('dataset', description = 'Dataset operations API')

def get_collection_name():
    return app.config['DB_COLLECTION']

data_model = api.model()

class DatasetGetAll(Resource):
    ''' Get the entire dataset. 
    '''
    @api.doc(description = 'Returns all the data records.')
    @api.response(200, 'Success', data_model)
    # @api.marshal_list_with(data_model)
    def get(self):
        data = app.mongo.db[get_collection_name()].find() 
        return list(data)

class DatasetRandomSample(Resource):
    ''' Get a random subset of data. 
    '''
    @api.doc(description = 'Returns a random subset of the data.')
    @api.param('sample', 'The number of samples to return.')
    @api.response(200, 'Success', data_model)
    @api.marshal_list_with(data_model)
    def get(self):
        try:
            sample_size = int(request.args.get('sample', default = 1))
        except ValueError:
            return {'message': 'Invalid sample size provided'}, 400
        if sample_size <= 0:
            return {'message': 'Sample size must be a positive integer.'}, 400
        data = app.mongo.db[get_collection_name()].aggregate([{'$sample': {'size': sample_size}}])
        return list(data)

api.add_resource(DatasetGetAll, '/getall')
api.add_resource(DatasetRandomSample, '/randomsample')