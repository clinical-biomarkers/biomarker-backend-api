from flask import Request, Response
from typing import Tuple, Dict, Union
import csv
import json
import gzip
from io import BytesIO, StringIO

from . import db as db_utils
from . import utils
from .detail_utils import _detail_query_builder
from .list_utils import _search_query_builder, _list_query_builder, _unroll_results


def detail_download(api_request: Request) -> Union[Tuple[Dict, int], Response]:
    """Entry point for the detail download endpoint."""
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "download"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    format = str(request_arguments["format"]).lower().strip()
    if format != "json":
        error_object = db_utils.log_error(
            error_log=f"User passed invalid format: {format}",
            error_msg="invalid-format-type",
            origin="detail_download",
        )
        return error_object, 415

    biomarker_id = request_arguments["id"]
    request_arguments["biomarker_id"] = biomarker_id
    mongo_query, projection_object = _detail_query_builder(request_arguments)

    detail_record, detail_http_code = db_utils.find_one(
        query_object=mongo_query, projection_object=projection_object
    )
    if detail_http_code != 200:
        return detail_record, detail_http_code

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

    biomarker_id = request_arguments["id"]
    request_arguments["biomarker_id"] = biomarker_id
    mongo_query, projection_object = _detail_query_builder(request_arguments)

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


def list_download(api_request: Request) -> Union[Tuple[Dict, int], Response]:
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "download"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    format = str(request_arguments["format"]).lower().strip()
    if format not in {"json", "csv"}:
        error_object = db_utils.log_error(
            error_log=f"User passed invalid format: {format}",
            error_msg="invalid-format-type",
            origin="list_download",
        )
        return error_object, 415

    request_arguments["offset"] = 0
    request_arguments["sort"] = "hit_score"
    request_arguments["limit"] = 10_000
    request_arguments["order"] = "desc"

    mongo_query, projection_object = _list_query_builder(request_arguments)

    cache_object, query_http_code = db_utils.get_cached_objects(
        request_object=request_arguments,
        query_object=mongo_query,
        projection_object=projection_object,
    )
    if query_http_code != 200:
        return cache_object, query_http_code

    search_query = cache_object["mongo_query"]
    search_pipeline = _search_query_builder(search_query, request_arguments)

    pipeline_result, pipeline_http_code = db_utils.execute_pipeline(search_pipeline)
    if pipeline_http_code != 200:
        return pipeline_result, pipeline_http_code

    formatted_results = _unroll_results(pipeline_result.get("results", []))

    filename = "list_download.json"
    if format == "json":
        file_content = json.dumps(formatted_results)
        mimetype = "application/json"
    else:
        output = StringIO()
        csv_writer = csv.writer(output)

        csv_writer.writerow(
            [
                "biomarker_canonical_id",
                "biomarker_id",
                "biomarker",
                "assessed_biomarker_entity_id",
                "assessed_entity_type",
                "assessed_biomarker_entity",
                "specimen",
                "best_biomarker_role",
                "condition",
                "component_count",
                "record_type",
                "hit_score",
            ]
        )

        for result in formatted_results:
            csv_writer.writerow(
                [
                    result.get("biomarker_canonical_id", ""),
                    result.get("biomarker_id", ""),
                    result.get("biomarker", ""),
                    result.get("assessed_biomarker_entity_id", ""),
                    result.get("assessed_entity_type", ""),
                    result.get("assessed_biomarker_entity", ""),
                    result.get("specimen", ""),
                    result.get("best_biomarker_role", ""),
                    result.get("condition", ""),
                    result.get("component_count", ""),
                    result.get("record_type", ""),
                    result.get("hit_score", ""),
                ]
            )

        file_content = output.getvalue()
        mimetype = "text/csv"

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
