""" Handles the backend logic for the biomarker list endpoint.
"""

from flask import Request, current_app
from typing import Tuple, Dict, List, Optional, Union, Set
import time

from . import utils as utils
from . import db as db_utils
from . import DB_COLLECTION, SEARCH_CACHE_COLLECTION, SEARCH_BATCH_SIZE


def list(api_request: Request) -> Tuple[Dict, int]:
    """Entry point for the backend logic of the list endpoint.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_arguments, request_http_code = utils.get_request_object(api_request, "list")
    if request_http_code != 200:
        return request_arguments, request_http_code

    mongo_query, projection_object = _list_query_builder(request_arguments)

    # TODO : delete logging
    custom_app = db_utils.cast_app(current_app)
    start_time = time.time()
    custom_app.api_logger.info("LIST GET BATCHED OBJECTS")
    custom_app.api_logger.info(f"\tSTART TIME: {start_time}")

    cache_object, query_http_code = db_utils.get_cached_objects(
        request_object=request_arguments,
        query_object=mongo_query,
        projection_object=projection_object,
        cache_collection=SEARCH_CACHE_COLLECTION,
    )

    # TODO : delete logging
    end_time = time.time()
    custom_app.api_logger.info(f"\tEND TIME: {end_time}")
    custom_app.api_logger.info(f"\tELAPSED TIME: {end_time - start_time}")

    if query_http_code != 200:
        return cache_object, query_http_code

    applied_filters = request_arguments.get("filters", [])
    filter_object, filter_codes = _format_filter_data(applied_filters)

    offset = request_arguments["offset"]
    sort_field = request_arguments["sort"]
    limit = request_arguments["limit"]
    order = request_arguments["order"]
    reverse_flag = True if order.lower().strip() == "desc" else False

    id_list = cache_object["id_list"]
    cache_info = cache_object["cache_info"]
    total_ids = len(id_list)
    batches = total_ids // SEARCH_BATCH_SIZE

    all_batches: List[List] = []
    
    # TODO : delete logging
    overall_start_time = time.time()
    custom_app.api_logger.info("BATCH PROCESSING") 
    custom_app.api_logger.info(f"\tSTART TIME: {overall_start_time}")
    batch_total = 0.0
    get_cache_batch_total = 0.0
    filter_and_format_total = 0.0
    sort_batch_total = 0.0

    for i in range(0, batches + 1):

        # TODO : delete logging
        batch_start_time = time.time()
        custom_app.api_logger.info(f"\tBATCH: {i}")
        custom_app.api_logger.info(f"\t\tSTART TIME: {batch_start_time}")

        start_index = i * SEARCH_BATCH_SIZE
        end_index = min(start_index + SEARCH_BATCH_SIZE, total_ids)
        batch_ids = id_list[start_index:end_index]

        if not batch_ids:
            continue

        # TODO : delete logging
        custom_app.api_logger.info(f"\t\tTOTAL IDS: {len(batch_ids)}")
        get_batch_start_time = time.time()
        custom_app.api_logger.info("\t\tGET CACHE BATCH")
        custom_app.api_logger.info(f"\t\t\tSTART TIME: {get_batch_start_time}")

        batch_results, batch_http_code = db_utils.get_cache_batch(
            batch_ids, i, {"_id": 0}, DB_COLLECTION
        )
        if batch_http_code != 200:
            return batch_results, batch_http_code

        # TODO : delete logging
        get_batch_end_time = time.time()
        get_batch_elapsed = get_batch_end_time - get_batch_start_time
        get_cache_batch_total += get_batch_elapsed
        custom_app.api_logger.info(f"\t\t\tEND TIME: {get_batch_end_time}")
        custom_app.api_logger.info(f"\t\t\tELAPSED TIME: {get_batch_elapsed}")

        # TODO : delete logging
        filter_format_start_time = time.time()
        custom_app.api_logger.info("\t\tFILTER AND FORMAT BATCH")
        custom_app.api_logger.info(f"\t\t\tSTART TIME: {filter_format_start_time}")

        formatted_return_results, filter_object = _filter_and_format(
            batch_results["results"],
            filter_object,
            filter_codes=filter_codes if filter_codes != {} else None,
        )

        # TODO : delete logging
        filter_format_end_time = time.time()
        filter_format_elapsed = filter_format_end_time - filter_format_start_time
        filter_and_format_total += filter_format_elapsed
        custom_app.api_logger.info(f"\t\t\tEND TIME: {filter_format_end_time}")
        custom_app.api_logger.info(f"\t\t\tELAPSED TIME: {filter_format_elapsed}")
        sort_batch_start_time = time.time()
        custom_app.api_logger.info("\t\tSORT BATCH")
        custom_app.api_logger.info(f"\t\t\tSTART TIME: {sort_batch_start_time}")

        # sort batch
        sorted_batch = sorted(
            formatted_return_results,
            key=lambda x: x[sort_field] if sort_field in x else "hit_score",
            reverse=reverse_flag,
        )
        all_batches.append(sorted_batch)

        # TODO : delete logging
        sort_batch_end_time = time.time()
        sort_batch_elapsed = sort_batch_end_time - sort_batch_start_time
        sort_batch_total += sort_batch_elapsed
        custom_app.api_logger.info(f"\t\t\tEND TIME: {sort_batch_end_time}")
        custom_app.api_logger.info(f"\t\t\tELAPSED TIME: {sort_batch_elapsed}")
        batch_end_time = time.time()
        batch_elapsed = batch_end_time - batch_start_time
        batch_total += batch_elapsed
        custom_app.api_logger.info(f"\t\tEND TIME: {batch_end_time}")
        custom_app.api_logger.info(f"\t\tELAPSED TIME: {batch_elapsed}")

    # TODO : delete logging
    overall_end_time = time.time()
    custom_app.api_logger.info(f"\tEND TIME: {overall_end_time}")
    custom_app.api_logger.info(f"\tELAPSED TIME: {overall_end_time - overall_start_time}")
    custom_app.api_logger.info(f"\tAVG TIME: {(overall_end_time - overall_start_time) / (batches + 1)}")
    custom_app.api_logger.info(f"\tGET BATCH AVG: {get_cache_batch_total / (batches + 1)}")
    custom_app.api_logger.info(f"\tFILTER AND FORMAT AVG: {filter_and_format_total / (batches + 1)}")
    custom_app.api_logger.info(f"\tSORT BATCH AVG: {sort_batch_total / (batches + 1)}")
    sort_batch_list_start_time = time.time()
    custom_app.api_logger.info("SORT BATCH LIST")
    custom_app.api_logger.info(f"\tSTART TIME: {sort_batch_list_start_time}")
    
    # sort batches within all batches list
    sorted_batch_list = sorted(
        all_batches,
        key=lambda x: x[0][sort_field] if sort_field in x else "hit_score",
        reverse=reverse_flag,
    )

    # TODO : delete logging
    sort_batch_list_end_time = time.time()
    sort_batch_list_elapsed = sort_batch_list_end_time - sort_batch_list_start_time
    custom_app.api_logger.info(f"\tEND TIME: {sort_batch_list_end_time}")
    custom_app.api_logger.info(f"\tELAPSED TIME: {sort_batch_list_elapsed}")

    # merge sorted batch list into one list
    merged_batch_list: List[Dict] = []
    for sorted_batch in sorted_batch_list:
        merged_batch_list.extend(sorted_batch)

    results = {
        "cache_info": cache_info,
        "filters": filter_object,
        "results": merged_batch_list[offset : offset + limit],
        "pagination": {
            "offset": offset,
            "limit": limit,
            "total_length": len(merged_batch_list),
            "sort": sort_field,
            "order": order,
        },
    }

    return results, 200


def _filter_and_format(
    batch: List,
    filters: Dict,
    filter_codes: Optional[Dict[str, Dict[str, Union[int, Set]]]],
) -> Tuple[List, Dict]:
    """Transforms data records from the full JSON data model to the flat list format
    while applying filtering.

    Parameters
    ----------
    documents : list
        The batched list of documents to transform.
    filters : dict
        The base filter object to build.
    filter_codes : dict or None
        The applied filters or None if none provided.

    Returns
    -------
    tuple : (list, dict)
        The formatted records and the updated filter object.
    """
    results: List[Dict] = []
    counts: Dict[str, Dict[str, int]] = {"entity_types": {}, "roles": {}}

    for document in batch:

        assessed_entity_types = set(
            [
                component["assessed_entity_type"].lower()
                for component in document["biomarker_component"]
            ]
        )
        roles = set([role["role"].lower() for role in document["best_biomarker_role"]])

        # handle filtering
        include_record = True
        if filter_codes is not None:
            for filter_id, filter_data in filter_codes.items():
                filter_values = filter_data["values"]
                filter_code = filter_data["code"]

                if filter_id == "assessed_entity_type":
                    if filter_code == 0:  # OR
                        if not assessed_entity_types & filter_values:  # type: ignore
                            include_record = False
                            break
                    elif filter_code == 1:  # AND
                        if not filter_values <= assessed_entity_types:  # type: ignore
                            include_record = False
                            break
                elif filter_id == "best_biomarker_role":
                    if filter_code == 0:  # OR
                        if not roles & filter_values:  # type: ignore
                            include_record = False
                            break
                    elif filter_code == 1:  # AND
                        if not filter_values <= roles:  # type: ignore
                            include_record = False
                            break
        if not include_record:
            continue

        # update available filters
        for entity_type in assessed_entity_types:
            if entity_type in counts["entity_types"]:
                counts["entity_types"][entity_type] += 1
            else:
                counts["entity_types"][entity_type] = 1
        for role in roles:
            if role in counts["roles"]:
                counts["roles"][role] += 1
            else:
                counts["roles"][role] = 1

        # format entry
        components = document["biomarker_component"]
        hit_score, score_info = utils.get_hit_score(document)
        entry = {
            "biomarker_canonical_id": document["biomarker_canonical_id"],
            "biomarker_id": document["biomarker_id"],
            "biomarker": "; ".join(set([comp["biomarker"] for comp in components])),
            "assessed_biomarker_entity_id": "; ".join(
                set([comp["assessed_biomarker_entity_id"] for comp in components])
            ),
            "assessed_entity_type": "; ".join(
                set([comp["assessed_entity_type"] for comp in components])
            ),
            "assessed_biomarker_entity": "; ".join(
                set(
                    [
                        comp["assessed_biomarker_entity"]["recommended_name"]
                        for comp in components
                    ]
                )
            ),
            "specimen": "; ".join(
                set(
                    [
                        f"{specimen['name']} ({specimen['id']})"
                        for comp in components
                        for specimen in comp["specimen"]
                    ]
                )
            ),
            "best_biomarker_role": "; ".join(
                [role["role"] for role in document["best_biomarker_role"]]
            ),
            "condition": "; ".join(
                [
                    f"{document['condition']['recommended_name']['name']} ({document['condition']['recommended_name']['id']})"
                ]
            ),
            "component_count": len(components),
            "record_type": "biomarker",
            "hit_score": hit_score,
            "score_info": score_info,
        }

        results.append(entry)

    # build updated filters
    filters["available"][0]["options"] = [
        {"id": et, "label": et.title(), "count": count, "order": idx + 1}
        for idx, (et, count) in enumerate(sorted(counts["entity_types"].items()))
    ]
    filters["available"][1]["options"] = [
        {"id": role, "label": role.title(), "count": count, "order": idx + 1}
        for idx, (role, count) in enumerate(sorted(counts["roles"].items()))
    ]

    return results, filters


def _list_query_builder(request_object: Dict) -> Tuple[Dict, Dict]:
    """Biomarker list endpoint query builder.

    Parameters
    ----------
    request_object : dict
        The validated request object from the user API call.

    Returns
    -------
    tuple : (dict, dict)
        The MongoDB query and the projection object.
    """
    return {"list_id": request_object["id"]}, {"_id": 0}


def _format_filter_data(
    applied_filters: Dict,
) -> Tuple[
    Dict[str, Union[List[Dict], int, str]], Dict[str, Dict[str, Union[int, Set]]]
]:
    """Creates the filter objects.

    Parameters
    ----------
    applied_filters : dict
        The user applied filters.

    Returns
    -------
    tuple : (dict, dict)
        The base filter object and the filter code object.
    """
    filter_object: Dict = {
        "applied": [],
        "available": [
            {
                "id": "by_assessed_entity_type",
                "label": "By Entity Type",
                "order": 1,
                "tooltip": "",
                "options": [],
            },
            {
                "id": "by_biomarker_role",
                "label": "By Biomarker Role",
                "order": 2,
                "tooltip": "",
                "options": [],
            },
        ],
    }
    # 0 == OR
    # 1 == AND
    filter_codes: Dict[str, Dict[str, Union[int, Set]]] = {}
    for filter in applied_filters:
        filter_object["applied"] = {
            "id": filter["id"],
            "operator": filter["operator"],
            "selected": filter["selected"],
        }
        if filter["id"] == "by_assessed_entity_type":
            filter_codes["assessed_entity_type"] = {}
            filter_codes["assessed_entity_type"]["code"] = (
                0 if filter["operator"].strip().lower() == "or" else 1
            )
            filter_codes["assessed_entity_type"]["values"] = set(filter["selected"])
        elif filter["id"] == "by_biomarker_role":
            filter_codes["best_biomarker_role"] = {}
            filter_codes["best_biomarker_role"]["code"] = (
                0 if filter["operator"].strip().lower() == "or" else 1
            )
            filter_codes["best_biomarker_role"]["values"] = set(filter["selected"])
    return filter_object, filter_codes
