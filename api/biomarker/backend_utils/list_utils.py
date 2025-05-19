"""Handles the backend logic for the biomarker list endpoint."""

from flask import Request, current_app
from typing import Optional, Tuple, Dict, List

from . import utils as utils
from . import db as db_utils
from . import cache_utils
from . import SEARCH_CACHE_COLLECTION
from .performance_logger import PerformanceLogger


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

    custom_app = db_utils.cast_app(current_app)
    perf_logger = PerformanceLogger(custom_app.api_logger)

    perf_logger.start_timer(process_name="get_mongodb_query")
    cache_object, query_http_code = db_utils.get_cached_objects(
        request_object=request_arguments,
        query_object=mongo_query,
        projection_object=projection_object,
        cache_collection=SEARCH_CACHE_COLLECTION,
    )
    perf_logger.end_timer(process_name="get_mongodb_query")

    if query_http_code != 200:
        return cache_object, query_http_code

    search_query = cache_object["mongo_query"]
    cache_info = cache_object["cache_info"]
    # patching for now
    mongo_query = cache_info["query"]
    api_request = cache_info["api_request"]
    cache_info["query"] = api_request
    del cache_info["api_request"]
    cache_info["mongo_query"] = mongo_query

    ai_search_metata = cache_info.get("ai_parsing", None)
    if ai_search_metata is not None:
        cache_info["ai_parsing"] = ai_search_metata

    search_pipeline = _search_query_builder(search_query, request_arguments)

    # Try to get cached pipeline results
    perf_logger.start_timer(process_name="atttempt_ttl_cache_retrieval")
    list_id = request_arguments["id"]
    cached_results = cache_utils.get_cached_pipeline_results(
        list_id=list_id, request_args=request_arguments, cache_info=cache_info
    )

    if cached_results is not None:
        perf_logger.end_timer(process_name="atttempt_ttl_cache_retrieval")
        perf_logger.log_times(
            request_arguments=request_arguments,
            search_query=search_query,
            lru_cache_hit=True,
        )
        return {
            "cache_info": cache_info,
            "filters": _format_filter_data(
                applied_filters=request_arguments.get("filters", []),
                pipeline_result=cached_results,
            ),
            "results": _unroll_results(cached_results.get("results", [])),
            "pagination": {
                "offset": request_arguments["offset"],
                "limit": request_arguments["limit"],
                "total_length": cached_results.get("total_count", 0),
                "sort": request_arguments["sort"],
                "order": request_arguments["order"],
            },
        }, 200

    perf_logger.end_timer(process_name="atttempt_ttl_cache_retrieval")

    # If not cached, execute pipeline and cache results
    perf_logger.start_timer(process_name="execute_pipeline")
    pipeline_result, pipeline_http_code = db_utils.execute_pipeline(search_pipeline)
    perf_logger.end_timer(process_name="execute_pipeline")

    if pipeline_http_code != 200:
        return pipeline_result, pipeline_http_code

    # Cache pipeline results
    cache_utils.cache_pipeline_results(
        list_id=list_id,
        request_args=request_arguments,
        results=pipeline_result,
        cache_info=cache_info,
    )

    filter_object = _format_filter_data(
        request_arguments.get("filters", []), pipeline_result
    )
    perf_logger.start_timer(process_name="unroll_results")
    formatted_results = _unroll_results(pipeline_result.get("results", []))
    perf_logger.end_timer(process_name="unroll_results")

    perf_logger.log_times(
        request_arguments=request_arguments, search_query=search_query
    )

    results = {
        "cache_info": cache_info,
        "filters": filter_object,
        "results": formatted_results,
        "pagination": {
            "offset": request_arguments["offset"],
            "limit": request_arguments["limit"],
            "total_length": pipeline_result.get("total_count", 0),
            "sort": request_arguments["sort"],
            "order": request_arguments["order"],
        },
    }

    return results, 200


def _unroll_results(results: List[Dict]) -> List:
    """Unrolls the data model data into the flat list data.

    Parameters
    ----------
    results: list
        The list of data model results.

    Returns
    -------
    list
        The formatted data.
    """
    return_list: List[Dict] = []
    for document in results:
        components = document["biomarker_component"]

        condition = document.get("condition")
        condition_unrolled: Optional[str] = None
        if condition is not None:
            cond_rec_name = condition.get("recommended_name", {})
            condition_unrolled = "; ".join(
                [f"{cond_rec_name.get('name', '')} ({cond_rec_name.get('id', '')})"]
            )

        exposure_agent = document.get("exposure_agent")
        exposure_agent_unrolled: Optional[str] = None
        if exposure_agent is not None:
            exposure_agent_rec_name = exposure_agent.get("recommended_name", {})
            exposure_agent_unrolled = "; ".join(
                [
                    f"{exposure_agent_rec_name.get('name', '') ({exposure_agent_rec_name.get('id', '')})}"
                ]
            )

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
            "component_count": len(components),
            "record_type": "biomarker",
            "hit_score": document["score"],
            "score_info": document.get(
                "score_info",
                {
                    "contributions": [
                        {"c": "biomarker_exact_match", "w": 0.0, "f": 0.0}
                    ],
                    "formula": "sum(w + 0.01*f)",
                    "variables": {
                        "c": "condition name",
                        "w": "condition weight",
                        "f": "condition match frequency",
                    },
                },
            ),
        }

        if condition_unrolled is not None:
            entry["condition"] = condition_unrolled

        if exposure_agent_unrolled is not None:
            entry["exposure_agent"] = exposure_agent_unrolled

        return_list.append(entry)

    return return_list


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


def _search_query_builder(query_object: Dict, request_object: Dict) -> List:
    """Creates the aggregation pipeline for the search.

    Parameters
    ----------
    query_object : dict
        The MongoDB query for the match step.
    request_object : dict
        The request object containing postprocessing information from the user.

    Returns
    -------
    list
        The MongoDB aggregation pipeline.
    """
    sort_field_map = {
        "biomarker_id": "biomarker_id",
        "biomarker": "biomarker_component.biomarker",
        "assessed_biomarker_entity": "biomarker_component.assessed_biomarker_entity.recommended_name",
        "assessed_biomarker_entity_id": "biomarker_component.assessed_biomarker_entity_id",
        "hit_score": "score",
        "condition": "condition.recommended_name.name",
    }
    filter_map = {
        "by_biomarker_role": "best_biomarker_role.role",
        "by_assessed_entity_type": "biomarker_component.assessed_entity_type",
    }

    applied_filters = request_object.get("filters", [])
    offset = request_object["offset"]
    cleaned_offset = offset - 1 if not offset - 1 < 0 else 0
    sort_field = request_object["sort"]
    mapped_sort_field = sort_field_map.get(sort_field, "hit_score")
    secondary_sort_field = "score" if mapped_sort_field != "score" else "biomarker_id"
    limit = request_object["limit"]
    order = request_object["order"]
    reverse_flag = -1 if order.lower().strip() == "desc" else 1

    search_condition = query_object
    additional_condition_filters: List[Dict] = []

    for filter in applied_filters:

        filter_id = filter["id"]
        operator = filter["operator"]
        selected_values = filter["selected"]

        if filter_id in filter_map:
            condition = {filter_map[filter_id]: {"$in": selected_values}}
        # ignore unsupported filter IDs
        else:
            continue

        if operator.lower().strip() == "or":
            additional_condition_filters.append({"$or": [condition]})
        else:
            additional_condition_filters.append(condition)

    if additional_condition_filters:
        combined_match_conditions = {
            "$and": [search_condition] + additional_condition_filters
        }
    else:
        combined_match_conditions = search_condition

    match_stage = {"$match": combined_match_conditions}
    sort_stage = {"$sort": {mapped_sort_field: reverse_flag, secondary_sort_field: 1}}
    skip_stage = {"$skip": cleaned_offset}
    limit_stage = {"$limit": limit}
    project_results_stage = {
        "$project": {
            "_id": 0,
            "biomarker_id": 1,
            "biomarker_canonical_id": 1,
            "biomarker_component.biomarker": 1,
            "biomarker_component.assessed_biomarker_entity.recommended_name": 1,
            "biomarker_component.assessed_biomarker_entity_id": 1,
            "biomarker_component.assessed_entity_type": 1,
            "biomarker_component.specimen": 1,
            "best_biomarker_role.role": 1,
            "condition.recommended_name.name": 1,
            "condition.recommended_name.id": 1,
            "score": 1,
            "score_info": 1,
        }
    }

    # main facet steps
    total_count_step = [{"$project": {"biomarker_id": 1}}, {"$count": "count"}]
    role_count_step = [
        {"$project": {"best_biomarker_role": 1}},
        {"$unwind": "$best_biomarker_role"},
        {
            "$group": {
                "_id": "$best_biomarker_role.role",
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": None,
                "counts": {"$push": {"role": "$_id", "count": "$count"}},
            }
        },
        {"$project": {"_id": 0}},
    ]
    entity_type_count_step = [
        {"$project": {"biomarker_component": 1}},
        {"$unwind": "$biomarker_component"},
        {
            "$group": {
                "_id": "$biomarker_component.assessed_entity_type",
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": None,
                "counts": {"$push": {"type": "$_id", "count": "$count"}},
            }
        },
        {"$project": {"_id": 0}},
    ]
    results_step = [sort_stage, skip_stage, limit_stage, project_results_stage]

    counts_stage = {
        "$project": {
            "total_count": {"$arrayElemAt": ["$total_count.count", 0]},
            "role_counts": {"$arrayElemAt": ["$role_count.counts", 0]},
            "entity_type_counts": {"$arrayElemAt": ["$entity_type_count.counts", 0]},
            "results": 1,
        }
    }

    pipeline = [
        match_stage,
        # sort_stage,
        {
            "$facet": {
                "total_count": total_count_step,
                "role_count": role_count_step,
                "entity_type_count": entity_type_count_step,
                "results": results_step,
            }
        },
        counts_stage,
    ]
    return pipeline


def _format_filter_data(applied_filters: List, pipeline_result: Dict) -> Dict:
    """Creates the filter objects.

    Parameters
    ----------
    applied_filters : list
        Any applied filters sent by the user.
    pipeline_result : dict
        The return result from the aggreagation pipeline.

    Returns
    -------
    dict
        The base filter object.
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

    for idx, entity_type in enumerate(pipeline_result.get("entity_type_counts", [])):
        entity_type_entry = {
            "id": entity_type["type"].lower(),
            "label": (
                entity_type["type"].upper()
                if len(entity_type["type"]) == 3
                else entity_type["type"].title()
            ),
            "count": entity_type["count"],
            "order": idx + 1,
        }
        filter_object["available"][0]["options"].append(entity_type_entry)

    for idx, role_count in enumerate(pipeline_result.get("role_counts", [])):
        role_entry = {
            "id": role_count["role"].lower(),
            "label": role_count["role"].title(),
            "count": role_count["count"],
            "order": idx + 1,
        }
        filter_object["available"][1]["options"].append(role_entry)

    if len(applied_filters) > 0:
        for filter in applied_filters:
            filter_object["applied"].append(filter)

    return filter_object
