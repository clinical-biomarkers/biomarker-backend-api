from flask_restx import Namespace, Resource, inputs
from flask import current_app as app 
from flask import request

api = Namespace('dataset', description = 'Dataset operations API')

def get_collection_name():
    return app.config['DB_COLLECTION']

class DatasetGetAll(Resource):
    ''' Get the entire dataset. 
    '''
    @api.doc(description = 'Returns all the data records Supports pagination and per page filtering.')
    @api.param('page', 'The page number to return.', type = int, default = 1)
    @api.param('per_page', 'The number of records to return per page.', type = int, default = 50)
    @api.response(200, 'Success')
    @api.response(500, 'Database query failed')
    def get(self):
        page = request.args.get('page', default = 1, type = int)
        per_page = request.args.get('per_page', default = 50, type = int)
        try:
            data = app.mongo_db[get_collection_name()].find({}, {'_id': 0}).skip((page - 1) * per_page).limit(per_page)
        except Exception as e:
            app.logger.error(f'Database query failed for get all: {e}')
            return {'message': 'Database query failed'}, 500
        return {'biomarkers': list(data)}, 200

class DatasetRandomSample(Resource):
    ''' Get a random subset of data. 
    '''
    @api.doc(description = 'Returns a random subset of the data. The sample size must be a positive integer.')
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
            app.logger.error(f'Database query failed on random sample (sample = `{sample_size}`): {e}')
            return {'message': 'Database query failed.'}, 500
        return {'biomarkers': list(data)}, 200

api.add_resource(DatasetGetAll, '/getall')
api.add_resource(DatasetRandomSample, '/randomsample')
