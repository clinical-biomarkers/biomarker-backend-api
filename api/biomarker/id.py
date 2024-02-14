from flask_restx import Namespace, Resource
from flask import current_app as app
from flask import request
from .data_models import data_model

api = Namespace('id', description = 'Get records by biomarker ID.')

def get_collection_name():
    return app.config['DB_COLLECTION']

class Biomarker(Resource):
    ''' Get a single biomarker record by biomarker ID. 
    '''
    @api.doc(description = 'Returns a single biomarker record by biomarker ID.')
    @api.response(200, 'Partial or full success', data_model)
    @api.response(400, 'No biomarker ID provided')
    @api.response(404, 'Not Found')
    @api.param('biomarker_id', 'The biomarker ID.', _in = 'path', type = str, required = True)
    def get(self, id):
        biomarker_id = id
        if not biomarker_id:
            return {'message': 'No biomarker ID provided'}, 400
        try:
            data = app.mongo_db[get_collection_name()].find_one({'biomarker_id': biomarker_id}, {'_id': 0})
        except Exception as e:
            return {'message': 'Invalid biomarker ID provided'}, 400
        if not data:
            return {'message': 'Biomarker ID not found'}, 404
        return data

class Biomarkers(Resource):
    ''' Get multiple biomarker records by biomarker ID. 
    '''
    @api.doc(description = 'Returns multiple biomarker records by biomarker ID.')
    @api.response(200, 'Success', data_model)
    @api.response(400, 'No biomarker IDs provided')
    @api.response(404, 'Not Found')
    def post(self):
        data = request.json
        biomarker_ids = data.get('biomarker_ids')

        if not biomarker_ids:
            return {'message': 'No biomarker IDs provided'}, 400

        try:
            results = app.mongo_db[get_collection_name()].find({'biomarker_id': {'$in': biomarker_ids}}, {'_id': 0})
            if results.count() == 0:
                return {'message': 'Biomarker IDs not found'}, 404
            return list(results), 200
        except Exception as e:
            return {'message': 'Error processing request'}, 400

api.add_resource(Biomarker, '/<string:id>')
api.add_resource(Biomarkers, '/set')