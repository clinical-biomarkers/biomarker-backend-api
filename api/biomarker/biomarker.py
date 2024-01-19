from flask_restx import Namespace, Resource
from flask import current_app as app
from flask import request
from .data_models import data_model

api = Namespace('biomarker', description = 'Biomarker operations API')

def get_collection_name():
    return app.config['DB_COLLECTION']

class Biomarker(Resource):
    ''' Get a single biomarker record by biomarker ID. 
    '''
    @api.doc(description = 'Returns a single biomarker record by biomarker ID.')
    @api.response(200, 'Success', data_model)
    @api.response(400, 'No biomarker ID provided')
    @api.response(404, 'Not Found')
    @api.param('biomarker_id', 'The biomarker ID.', type = str, required = True)
    def get(self):
        biomarker_id = request.args.get('biomarker_id', default = None)
        if not biomarker_id:
            return {'message': 'No biomarker ID provided'}, 400
        try:
            data = app.mongo_db[get_collection_name()].find_one({'biomarker_id': biomarker_id}, {'_id': 0})
        except Exception as e:
            return {'message': 'Invalid biomarker ID provided'}, 400
        if not data:
            return {'message': 'Biomarker ID not found'}, 404
        return data

api.add_resource(Biomarker, '/getbiomarker')