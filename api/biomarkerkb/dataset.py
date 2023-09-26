from flask_restx import Namespace, Resource, fields
from db import get_db
from flask import current_app as app 

api = Namespace ('dataset', description = 'Dataset operations API')

def get_collection_name():
    return app.config['DB_COLLECTION']

data_model = api.model('Data', {
    'field1': fields.String(description = 'Field 1 description'),
    'field2': fields.String(description = 'Field 2 description')
})

class DatasetGetAll(Resource):
    ''' Get the entire dataset. 
    '''
    @api.doc('getall_dataset')
    @api.response(200, 'Success', data_model)
    @api.marshal_list_with(data_model)
    def get(self):
        data = get_db()[get_collection_name()].find() 
        return list(data)

class DatasetRandomSample(Resource):
    ''' Get a random subset of data. 
    '''
    @api.doc('random_sample')
    @api.response(200, 'Success', data_model)
    @api.marshal_list_with(data_model)
    def get(self, sample):
        data = get_db()[get_collection_name()].aggregate([{'$sample': {'size': sample}}])
        return list(data)

api.add_resource(DatasetGetAll, '/getall')
api.add_resource(DatasetRandomSample, '/randomsample/<int: sample>')