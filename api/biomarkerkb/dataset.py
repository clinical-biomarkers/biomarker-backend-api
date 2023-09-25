from flask_restx import Namespace, Resource, fields 
from flask import current_app as app 

api = Namespace ('dataset', description = 'Dataset operations API')

data_model = api.model('Data', {
    'field1': fields.String(description = 'Field 1 description'),
    'field2': fields.String(description = 'Field 2 description')
})

class DatasetGetAll(Resource):
    @api.doc('getall_dataset')
    @api.response(200, 'Success', data_model)
    @api.marshal_list_with(data_model)
    def get(self):
        data = app.mongo.db.biomarker_collection.find() 
        return list(data)

api.add_resource(DatasetGetAll, '/getall')