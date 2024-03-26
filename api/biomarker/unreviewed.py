from flask_restx import Namespace, Resource, inputs
from flask import current_app as app
from flask import request

api = Namespace('unreviewed', description = 'Get unreviewed records.')

def get_collection_name():
    return app.config['UNREVIEWED_COLLECTION']

class UnreviewedGetAll(Resource):
    ''' Get all unreviewed records. 
    '''
    @api.doc(description = 'Returns all unreviewed records.')
    @api.response(200, 'Success')
    @api.response(500, 'Database query failed')
    def get(self):
        try:
            data = app.mongo_db[get_collection_name()].find({}, {'_id': 0})
            return {'biomarkers': list(data)}, 200
        except Exception as e:
            app.logger.error(f'Database query failed on unreviewed get all: {e}')
            return {'message': 'Database query failed'}, 500

class UnreviewedRandomSample(Resource):
    ''' Get a random subset of unreviewed records. 
    '''
    @api.doc(description = 'Returns a random subset of unreviewed records. The sample size must be a positive integer.' )
    @api.param('sample', 'The number of samples to return.', type = inputs.positive, default = 1)
    @api.response(200, 'Success')
    @api.response(400, 'Bad Request')
    @api.response(500, 'Database query failed')
    def get(self):
        sample_size = request.args.get('sample', default = 1, type = int)
        try:
            data = app.mongo_db[get_collection_name()].aggregate([
                {'$sample': {'size': sample_size}},
                {'$project': {'_id': 0}}
            ])
        except Exception as e:
            app.logger.error(f'Database query failed on unreviewed random sample (sample = `{sample_size}`): {e}')
            return {'message': 'Database query failed.'}, 500
        return {'biomarkers': list(data)}, 200

class Biomarker(Resource):
    ''' Get a single biomarker record by biomarker ID from the unreviewed collection. 
    '''
    @api.doc(description='Returns a single unreviewed biomarker record by biomarker ID (if available).')
    @api.response(200, 'Success')
    @api.response(400, 'Invalid biomarker ID provided')
    @api.response(404, 'Not found')
    @api.response(500, 'Database query failed')
    @api.param('biomarker_id', 'The biomarker ID', _in = 'path', type = str, required = True)
    def get(self, id):
        biomarker_id = id 
        if not biomarker_id:
            return {'message': 'No biomarker ID provided.'}, 400
        try:
            data = app.mongo_db[get_collection_name()].find_one({'biomarker_id': biomarker_id}, {'_id': 0})
        except Exception as e:
            app.logger.error(f'Database query failed on unreviewed biomarker ID (`{biomarker_id}`): {e}')
            return {'message': 'Database query failed.'}, 500
        if not data:
            return {'message': 'Biomarker ID not found'}, 404
        return {'biomarker': data}, 200

class Biomarkers(Resource):
    ''' Returns multiple biomarker records by biomarker ID from the unreviewed data.
    '''
    @api.doc(description = 'Returns multiple biomarker records by biomarker ID from the unreviewed data.')
    @api.response(200, 'Success')
    @api.response(400, 'No biomarker IDs provided or invalid format')
    @api.response(404, 'Not found')
    @api.response(500, 'Database query failed')
    def post(self):
        data = request.json
        if not data:
            return {'message': 'No biomarker IDs provided or invalid format'}, 400
        biomarker_ids = data.get('biomarker_ids')

        if not biomarker_ids or not isinstance(biomarker_ids, list) or not all(isinstance(id, str) for id in biomarker_ids):
            return {'message': 'No biomarker IDs provided or invalid format.'}, 400

        try:
            results = app.mongo_db[get_collection_name()].find({'biomarker_id': {'$in': biomarker_ids}}, {'_id': 0})
            if results.count_documents({}) == 0:
                return {'messsage': 'Biomarkers IDs not found.'}, 404
            return {'biomarkers': list(results)}, 200
        except Exception as e:
            app.logger.error(f'Database query failed for unreviewed biomarker ID set (`{biomarker_ids}`): {e}')
            return {'message': 'Database query failed.'}, 500

class CanonicalID(Resource):
    ''' Gets all biomarkers under a particular Canonical ID from the unreviewed data.
    '''
    @api.doc(description = 'Returns all records under a specified canonical ID from the unreviewed data.')
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
            app.logger.error(f'Database query failed on canonical ID from unreviewed (`{canonical_id}`): {e}')
            return {'messsage': 'Database query failed.'}, 500
        if not data:
            return {'message': 'Canonical ID not found in the unreviewd data.'}, 500
        return {'results': data}, 200

class CanonicalIDS(Resource):
    ''' Get all biomarkers from the unreviewed data passing a set of canonical IDs.
    '''
    @api.doc(description = 'Returns all records for a list of canonical ID from the unreviewed data.')
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

api.add_resource(UnreviewedGetAll, '/getall')
api.add_resource(UnreviewedRandomSample, '/randomsample')
api.add_resource(Biomarker, '/id/<string:id>')
api.add_resource(Biomarkers, '/ids')
api.add_resource(CanonicalID, '/canonical/<string:id>')
api.add_resource(CanonicalIDS, '/canonical/set')
