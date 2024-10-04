from flask_restx import Resource, Namespace
import requests

api = Namespace("swagger", description="Swagger endpoints.", hide=True)

swagger_url = "https://hivelab.tst.biochemistry.gwu.edu/biomarker/api/swagger.json"

class SwaggerHack(Resource):

    @api.doc(False)
    def get(self):
        
        swagger_response = requests.get(swagger_url)

        if swagger_response.status_code != 200:

            return {"error": "error-retrieving-swagger-json"}, 500

        swagger_json = swagger_response.json()
        swagger_json["basePath"] = "/biomarker/api"

        return swagger_json, 200

api.add_resource(SwaggerHack, "/")
