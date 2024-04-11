from flask_restx import Namespace, Resource
from flask import current_app as app
from flask import request

api = Namespace('id', description = 'Get records by biomarker ID.')

def get_collection_name():
    return app.config['DB_COLLECTION']

class Biomarker(Resource):
    ''' Get a single biomarker record by biomarker ID. 
    '''
    @api.doc(description = 'Returns a single biomarker record by biomarker ID.')
    @api.response(200, 'Partial or full success')
    @api.response(400, 'No biomarker ID provided')
    @api.response(404, 'Not Found')
    @api.response(500, 'Database query failed')
    @api.param('biomarker_id', 'The biomarker ID.', _in = 'path', type = str, required = True)
    def get(self, id):
        biomarker_id = id
        if not biomarker_id:
            return {'message': 'No biomarker ID provided.'}, 400
        try:
            data = app.mongo_db[get_collection_name()].find_one({'biomarker_id': biomarker_id}, {'_id': 0})
        except Exception as e:
            app.logger.error(f'Database query failed on biomarker ID (`{biomarker_id}`): {e}')
            return {'message': 'Database query failed.'}, 500
        if not data:
            return {'message': 'Biomarker ID not found'}, 404
        return {'biomarker': data}, 200

class Biomarkers(Resource):
    ''' Get multiple biomarker records by biomarker ID. 
    '''
    @api.doc(description = 'Returns multiple biomarker records by biomarker ID.')
    @api.response(200, 'Success')
    @api.response(400, 'No biomarker IDs provided or invalid format')
    @api.response(404, 'Not Found')
    @api.response(500, 'Database query failed')
    def post(self):
        data = request.json
        if not data:
            return {'message': 'No biomarker IDs provided or invalid format.'}, 400
        biomarker_ids = data.get('biomarker_ids')

        if not biomarker_ids or not isinstance(biomarker_ids, list) or not all(isinstance(id, str) for id in biomarker_ids):
            return {'message': 'No biomarker IDs provided or invalid format.'}, 400

        try:
            results = app.mongo_db[get_collection_name()].find({'biomarker_id': {'$in': biomarker_ids}}, {'_id': 0})
            if results.count_documents({}) == 0:
                return {'message': 'Biomarker IDs not found.'}, 404
            return {'biomarkers': list(results)}, 200
        except Exception as e:
            app.logger.error(f'Database query failed for biomarker ID set (`{biomarker_ids}`): {e}')
            return {'message': 'Database query failed.'}, 500

api.add_resource(Biomarker, '/<string:id>')
api.add_resource(Biomarkers, '/set')
