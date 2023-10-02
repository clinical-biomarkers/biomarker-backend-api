from flask_restx import Namespace, Resource, fields
from .db import get_db
from flask import current_app as app 
from flask import request

api = Namespace ('dataset', description = 'Dataset operations API')

def get_collection_name():
    return app.config['DB_COLLECTION']

data_model = api.model('Data', {
    'biomarker_id': fields.String(description = 'Unique biomarker identifier.'),
    'main_x_ref': fields.String(description = 'Accession or identifier that most closely matches the biomarker term.'),
    'assessed_biomarker_entity': fields.String(description = 'Change in entity and common name.'),
    'biomarker_status': fields.String(description = 'Change measured in disease versus healthy individual.'),
    'best_biomarker_type': fields.String(description = 'Category of best biomarker.'),
    'specimen_type': fields.String(description = 'Type of speciment used to access the biomarker (with Uberon ID)'),
    'loinc_code': fields.String(description = 'Lab test ID associated with biomarker.'),
    'condition_name': fields.String(description = 'Condition name with DOID.'),
    'assessed_entity_type': fields.String(description = 'Entity type of the biomarker.'),
    'evidence_source': fields.String(description = 'Source of biomarker with corresponding link to data page within the source.'),
    'notes': fields.String(description = 'Meta data, if applicable.')
})

class DatasetGetAll(Resource):
    ''' Get the entire dataset. 
    '''
    @api.doc(description = 'Returns all the data records.')
    @api.response(200, 'Success', data_model)
    @api.marshal_list_with(data_model)
    def get(self):
        data = get_db()[get_collection_name()].find() 
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
        data = get_db()[get_collection_name()].aggregate([{'$sample': {'size': sample_size}}])
        return list(data)

api.add_resource(DatasetGetAll, '/getall')
api.add_resource(DatasetRandomSample, '/randomsample')