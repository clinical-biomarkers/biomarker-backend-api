"""Hacks the swagger.json base_url. This is a temporary placeholder while
the biomarker-partnership is hosted on the hivelab server. Once a dedicated
server is opened up this will be removed.
"""

from flask_restx import Resource, Namespace  # type: ignore
import requests

api = Namespace("swagger", description="Swagger JSON namespace.")


class SwaggerFix(Resource):

    def get(self):

        swagger_url = "https://hivelab.biochemistry.gwu.edu/biomarker/api/swagger.json"

        swagger_response = requests.get(swagger_url)
        if swagger_response.status_code != 200:
            return {"error": "error-retrieving-swagger-json"}, 500

        swagger_json = swagger_response.json()
        swagger_json["basePath"] = "/biomarker/api"

        return swagger_json, 200


api.add_resource(SwaggerFix, "/")
