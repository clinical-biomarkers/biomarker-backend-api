""" Handles the backend logic for the biomarker list endpoint.
"""

from flask import Request, current_app
from typing import Tuple, Dict, List, Optional, Union, Set

from . import utils as utils
from . import db as db_utils
from .cache_utils import batch_cache, generate_cache_key
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

    list_id = request_arguments["id"]
    applied_filters = request_arguments.get("filters", [])
    filter_object, filter_codes = _format_filter_data(applied_filters)
    offset = request_arguments["offset"]
    sort_field = request_arguments["sort"]
    limit = request_arguments["limit"]
    order = request_arguments["order"]
    reverse_flag = True if order.lower().strip() == "desc" else False

    mongo_query, projection_object = _list_query_builder(request_arguments)

    cache_object, query_http_code = db_utils.get_cached_objects(
        request_object=request_arguments,
        query_object=mongo_query,
        projection_object=projection_object,
        cache_collection=SEARCH_CACHE_COLLECTION,
    )

    if query_http_code != 200:
        return cache_object, query_http_code

    id_list = cache_object["id_list"]
    cache_info = cache_object["cache_info"]
    total_ids = len(id_list)
    batches = total_ids // SEARCH_BATCH_SIZE

    custom_app = db_utils.cast_app(current_app)
    perf_logger = custom_app.performance_logger

    all_batches: List[List] = []

    # logging
    perf_logger.start_timer(f"Total time for {batches + 1} batches")

    for i in range(0, batches + 1):

        start_index = i * SEARCH_BATCH_SIZE
        end_index = min(start_index + SEARCH_BATCH_SIZE, total_ids)
        batch_ids = id_list[start_index:end_index]

        # logging
        perf_logger.start_timer(f"Total batch {i} time (size: {len(batch_ids)})")

        if not batch_ids:
            continue

        # logging
        perf_logger.start_timer(f"Batch {i}", "Cache Retrieval")

        # this is confusing, the MongoDB cache collection is a pseudo cache
        # that still requires disk retrieval and is slow (holdover from legacy
        # code which this API was ported from), this is the in memory cache which
        # is different than any "cache" functions defined in the db_utils namespace
        batch_cache_key = generate_cache_key(list_id, i)
        batch_results = batch_cache.get(batch_cache_key)

        # if cache miss, hit MongoDB
        if not batch_results:

            # logging
            perf_logger.cancel_timer(f"Batch {i}", "Cache Retrieval")
            perf_logger.start_timer(f"Batch {i}", "Cache Miss, MongoDB Retrieval")

            batch_results, batch_http_code = db_utils.get_cache_batch(
                batch_ids, i, {"_id": 0}, DB_COLLECTION
            )
            if batch_http_code != 200:
                return batch_results, batch_http_code
            # cache the batch in memory
            batch_cache[batch_cache_key] = batch_results

            # logging
            perf_logger.end_timer(f"Batch {i}", "Cache Miss, MongoDB Retrieval")

        # logging
        perf_logger.end_timer(f"Batch {i}", "Cache Retrieval")

        # logging
        perf_logger.start_timer(f"Batch {i}", "Filter and Format")

        formatted_return_results, filter_object = _filter_and_format(
            batch_results["results"],
            filter_object,
            filter_codes=filter_codes if filter_codes != {} else None,
        )

        # logging
        perf_logger.end_timer(f"Batch {i}", "Filter and Format")

        # logging
        perf_logger.start_timer(f"Batch {i}", "Sort Batch")

        # sort batch
        sorted_batch = sorted(
            formatted_return_results,
            key=lambda x: x[sort_field] if sort_field in x else "hit_score",
            reverse=reverse_flag,
        )

        # logging
        perf_logger.end_timer(f"Batch {i}", "Sort Batch")

        all_batches.append(sorted_batch)

        # logging
        perf_logger.end_timer(f"Total batch {i} time (size: {len(batch_ids)})")

    # sort batches within all batches list
    sorted_batch_list = sorted(
        all_batches,
        key=lambda x: x[0][sort_field] if sort_field in x else "hit_score",
        reverse=reverse_flag,
    )

    # logging
    perf_logger.start_timer("Merge batch list")

    # merge sorted batch list into one list
    merged_batch_list: List[Dict] = []
    for sorted_batch in sorted_batch_list:
        merged_batch_list.extend(sorted_batch)

    # logging
    perf_logger.end_timer("Merge batch list")

    # logging
    perf_logger.end_timer(f"Total time for {batches + 1} batches")
    perf_logger.log_times(
        total_ids=total_ids,
        query=cache_info["query"],
        request_arguments=request_arguments,
        timestamp=db_utils.create_timestamp(),
    )

    results = {
        "cache_info": cache_info,
        "filters": filter_object,
        "results": merged_batch_list[offset - 1 : offset + limit],
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
