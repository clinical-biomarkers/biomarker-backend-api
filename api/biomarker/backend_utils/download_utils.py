from flask import Request, Response
from typing import Tuple, Dict, Union
import csv
import json
import gzip
from io import BytesIO, StringIO

from . import db as db_utils
from . import utils


def detail_download(api_request: Request) -> Union[Tuple[Dict, int], Response]:
    """Entry point for the detail download endpoint."""
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "download"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    mongo_query, projection_object, biomarker_id = _detail_query_builder(
        request_arguments
    )

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

    return Response(
        file_content,
        mimetype=mimetype,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": f"{mimetype}; charset=utf-8",
        },
    )


def section_download(api_request: Request) -> Union[Tuple[Dict, int], Response]:
    """Entry point for the section download endpoint."""
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "download"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    section = request_arguments.get("section")
    section = section.lower().strip() if isinstance(section, str) else section
    if section != "biomarker_component":
        error_object = db_utils.log_error(
            error_log=f"Unsupported section download section: {section}",
            error_msg="invalid-section",
            origin="section_download",
        )
        return error_object, 400

    mongo_query, projection_object, biomarker_id = _detail_query_builder(
        request_arguments
    )

    detail_record, detail_http_code = db_utils.find_one(
        query_object=mongo_query, projection_object=projection_object
    )
    if detail_http_code != 200:
        return detail_record, detail_http_code

    format = str(request_arguments["format"]).lower().strip()
    if format != "csv":
        error_object = db_utils.log_error(
            error_log=f"User passed invalid format: {format}",
            error_msg="invalid-format-type",
            origin="section_download",
        )
        return error_object, 415

    biomarker_components = detail_record["biomarker_component"]

    output = StringIO()
    csv_writer = csv.writer(output)

    csv_writer.writerow(
        [
            "biomarker",
            "assessed_biomarker_entity",
            "assessed_biomarker_entity_synonyms",
            "assessed_biomarker_entity_id",
            "assessed_entity_type",
            "specimen",
            "specimen_id",
            "loinc_code",
            "evidence_source",
            "evidence",
        ]
    )
    for component in biomarker_components:
        csv_writer.writerow(
            [
                component["biomarker"],
                component["assessed_biomarker_entity"]["recommended_name"],
                ", ".join(
                    [
                        s["synonym"]
                        for s in component["assessed_biomarker_entity"]["synonyms"]
                    ]
                ),
                component["assessed_biomarker_entity_id"],
                component["assessed_entity_type"],
                ", ".join([s["name"] for s in component["specimen"]]),
                ", ".join([s["id"] for s in component["specimen"]]),
                ", ".join([s["loinc_code"] for s in component["specimen"]]),
                ", ".join(
                    [f"{e['database']}:{e['id']}" for e in component["evidence_source"]]
                ),
                "; ".join(
                    [
                        evidence["evidence"]
                        for source in component["evidence_source"]
                        for evidence in source["evidence_list"]
                    ]
                ),
            ]
        )

    file_content = output.getvalue()
    mimetype = "text/csv"
    filename = f"{biomarker_id}_components.csv"

    compressed = request_arguments["compressed"]
    if compressed:
        compressed_file = BytesIO()
        with gzip.GzipFile(fileobj=compressed_file, mode="wb") as f:
            f.write(file_content.encode("utf-8"))

        file_content = compressed_file.getvalue()  # type: ignore
        filename += ".gz"
        mimetype = "application/gzip"

    return Response(
        file_content,
        mimetype=mimetype,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": f"{mimetype}; charset=utf-8",
        },
    )


def _detail_query_builder(
    request_object: Dict,
) -> Tuple[Dict[str, str], Dict[str, int], str]:
    """Biomarker detail query builder.

    Parameters
    ----------
    request_object : dict
        The validated request object from the user API call.

    Returns
    -------
    tuple : (dict[str, str], dict[str, int], str)
        The MongoDB query for the detail endpoint, the projection object,
        and biomarker ID.
    """
    biomarker_id = request_object["id"]
    projection_object = {"_id": 0, "all_text": 0}
    mongo_query = {"biomarker_id": biomarker_id}
    return mongo_query, projection_object, biomarker_id
