from flask_restx import Namespace, Resource
from flask import current_app as app
from flask import request

api = Namespace('canonical_id', description = 'Get biomarkers by canonical ID.')

def get_collection_name():
    return app.config['DB_collection']

class Canonical(Resource):
    ''' Get all biomarkers under a particular canonical ID.
    '''
    @api.doc(description = 'Returns all records under a specified canonical ID.')
    @api.response(200, 'Partial or full success')
    @api.response(400, 'No biomarker ID provided')
    @api.response(404, 'Not found')
    @api.response(500, 'Database query failed')
    @api.param('canonical_id', 'The canonical ID.', _in = 'path', type = str, required = True)
    def get(self, id):
        canonical_id = id
        if not canonical_id:
            return {'message': 'No canonical ID provided.'}, 400
        try:
            cursor = app.mongo_db[get_collection_name()].find({'biomarker_canonical_id': canonical_id}, {'_id': 0})
            data = list(cursor)
        except Exception as e:
            app.logger.error(f'Database query failed on canonical ID (`{canonical_id}`): {e}')
            return {'message': 'Database query failed.'}, 500
        if not data:
            return {'message': 'Canonical ID not found.'}, 404
        return {'results': data}, 200

class Canonicals(Resource):
    ''' Get all biomarkers passing a set of canonical IDs.
    '''
    @api.doc(description = 'Returns all records for a list of canonical ID.')
    @api.response(200, 'Partial or full success')
    @api.response(400, 'No biomarker ID provided or invalid format')
    @api.response(404, 'Not found')
    @api.response(500, 'Database query failed')
    def post(self):
        data = request.json
        if not data:
            return {'message': 'No canonical IDs provided or invalid format.'}, 400
        canonical_ids = data.get('canonical_ids')
        
        if not canonical_ids or not isinstance(canonical_ids, list) or not all(isinstance(id, str) for id in canonical_ids):
            return {'message': 'No canonical IDs provided or invalid format.'}, 400
        
        try:
            results = app.mongo_db[get_collection_name()].find(
                {'biomarker_canonical_id': {'$in': canonical_ids}},
                {'_id': 0}
            )
            results_list = list(results)
            if len(results_list) == 0:
                return {'message': 'Canonical IDs not found.'}, 404
            return {'biomarkers': results_list}, 200
        except Exception as e:
            app.logger.error(f'Database query failed for canonical ID set (`{canonical_ids}`): {e}')
            return {'message': 'Database query failed'}, 500

api.add_resource(Canonical, '/<string:id>')
api.add_resource(Canonicals, '/set')
