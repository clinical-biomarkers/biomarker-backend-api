from flask_restx import Namespace, Resource, inputs 
from flask import current_app as app
from flask import request
from .data_models import data_model

api = Namespace('unreviewed', description = 'Get unreviewed records.')

def get_collection_name():
    return app.config['COLLISION_COLLECTION']

class UnreviewedGetAll(Resource):
    ''' Get all unreviewed records. 
    '''
    @api.doc(description = 'Returns all unreviewed records.')
    @api.response(200, 'Success', data_model)
    def get(self):
        data = app.mongo_db[get_collection_name()].find({}, {'_id': 0})
        return list(data)

class UnreviewedRandomSample(Resource):
    ''' Get a random subset of unreviewed records. 
    '''
    @api.doc(description = 'Returns a random subset of unreviewed records. The sample size must be a positive integer.' )
    @api.param('sample', 'The number of samples to return.', type = inputs.positive, default = 1)
    @api.response(200, 'Success', data_model)
    @api.response(400, 'Bad Request')
    def get(self):
        try:
            sample_size = request.args.get('sample', default = 1, type = int)
        except ValueError:
            return {'message': 'Invalid sample size provided. Sample must be a positive integer.'}, 400
        data = app.mongo_db[get_collection_name()].aggregate([
            {'$sample': {'size': sample_size}},
            {'$project': {'_id': 0}}
            ])
        return list(data)

class Biomarker(Resource):
    ''' Get a single biomarker record by biomarker ID. 
    '''
    @api.doc(description='Returns a single unreviewed biomarker record by biomarker ID (if available).')
    @api.response(200, 'Success', data_model)
    @api.response(400, 'Invalid biomarker ID provided')
    @api.response(404, 'Not Found')
    @api.param('id', 'The biomarker ID', _in = 'path', type = str, required = True)
    def get(self, id):
        biomarker_id = id 
        try:
            data = app.mongo_db[get_collection_name()].find_one({'biomarker_id': biomarker_id}, {'_id': 0})
        except Exception as e:
            return {'message': 'Invalid biomarker ID provided'}, 400
        if not data:
            return {'message': 'Biomarker ID not found'}, 404
        return data

api.add_resource(UnreviewedGetAll, '/getall')
api.add_resource(UnreviewedRandomSample, '/randomsample')
api.add_resource(Biomarker, '/id/<string:id>')