from flask import Request, Response
from typing import Tuple, Dict, Union
import csv
import json
import gzip
from io import BytesIO

from . import db as db_utils
from . import utils


def detail_download(api_request: Request) -> Tuple[Union[Dict, Response], int]:
    """Entry point for the detail download endpoint."""
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "download"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    biomarker_id = request_arguments["id"]
    mongo_query = {"biomarker_id": biomarker_id}
    projection_object = {"_id": 0, "all_text": 0}

    detail_record, detail_http_code = db_utils.find_one(
        query_object=mongo_query, projection_object=projection_object
    )
    if detail_http_code != 200:
        return detail_record, detail_http_code

    format = str(request_arguments["format"]).lower().strip()
    if format != "json":
        error_object = db_utils.log_error(
            error_log=f"User passed invalid format: {format}",
            error_msg="invalid-format-type",
            origin="detail_download",
        )
        return error_object, 415

    file_content = json.dumps(detail_record, indent=2)
    mimetype = "application/json"
    filename = f"{biomarker_id}.json"

    compressed = request_arguments["compressed"]
    if compressed:
        compressed_file = BytesIO()
        with gzip.GzipFile(fileobj=compressed_file, mode="wb") as f:
            f.write(file_content.encode("utf-8"))

        file_content = compressed_file.getvalue()  # type: ignore
        filename += ".gz"
        mimetype = "application/gzip"

    return (
        Response(
            file_content,
            mimetype=mimetype,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": f"{mimetype}; charset=utf-8",
            },
        ),
        200,
    )
